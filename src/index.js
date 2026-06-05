import { connect } from "cloudflare:sockets";

const BUFFER_SIZE = 128;

function encode(type, streamId, payload) {
  const out = new Uint8Array(5 + payload.byteLength);
  const view = new DataView(out.buffer);
  view.setUint8(0, type);
  view.setUint32(1, streamId, true);
  out.set(new Uint8Array(payload), 5);
  return out.buffer;
}

function continuePacket(streamId) {
  const p = new Uint8Array(4);
  new DataView(p.buffer).setUint32(0, BUFFER_SIZE, true);
  return encode(0x03, streamId, p.buffer);
}

function closePacket(streamId, reason = 0x02) {
  return encode(0x04, streamId, new Uint8Array([reason]).buffer);
}

async function handleWisp(ws) {
  const streams = new Map();

  ws.send(continuePacket(0));

  ws.addEventListener("message", async (event) => {
    try {
      const raw = event.data;
      const buf = raw instanceof ArrayBuffer ? raw : await raw.arrayBuffer();
      const view = new DataView(buf);
      const type = view.getUint8(0);
      const streamId = view.getUint32(1, true);
      const payload = buf.slice(5);

      if (type === 0x01) { // CONNECT
        const pv = new DataView(payload);
        const port = pv.getUint16(1, true);
        const hostname = new TextDecoder().decode(new Uint8Array(payload).slice(3));
        console.log(`[wisp] CONNECT ${hostname}:${port}`);

        try {
          const socket = connect({ hostname: hostname.trim(), port });
          const writer = socket.writable.getWriter();
          streams.set(streamId, { writer, socket, hostname, port });

          socket.opened.then(() => {
            console.log(`[wisp] opened ${hostname}:${port}`);
          }).catch(e => {
            console.error(`[wisp] failed to open ${hostname}:${port}:`, e);
          });

          socket.closed.then((info) => {
            console.log(`[wisp] closed ${hostname}:${port}:`, JSON.stringify(info));
          }).catch(e => {
            console.error(`[wisp] close error ${hostname}:${port}:`, e);
          });

          ws.send(continuePacket(streamId));

          (async () => {
            const reader = socket.readable.getReader();
            try {
              while (true) {
                const { done, value } = await reader.read();
                if (done) break;
                const chunk = new Uint8Array(value);
                ws.send(encode(0x02, streamId, chunk.buffer));
              }
            } catch (e) {
              console.error(`[wisp] read error ${hostname}:${port}:`, e);
            }
            try { ws.send(closePacket(streamId, 0x02)); } catch {}
            streams.delete(streamId);
          })();

        } catch (e) {
          console.error(`[wisp] connect error ${hostname}:${port}:`, e);
          ws.send(closePacket(streamId, 0x42));
        }

      } else if (type === 0x02) { // DATA
        const stream = streams.get(streamId);
        if (stream) {
          try {
            await stream.writer.write(new Uint8Array(payload));
          } catch(e) {
            console.error(`[wisp] write error stream ${streamId}:`, e);
          }
        }

      } else if (type === 0x04) { // CLOSE
        const stream = streams.get(streamId);
        if (stream) {
          console.log(`[wisp] closing stream ${streamId} ${stream.hostname}:${stream.port}`);
          try { stream.writer.close(); } catch {}
          try { stream.socket.close(); } catch {}
          streams.delete(streamId);
        }
      } else {
        console.warn(`[wisp] unknown packet type 0x${type.toString(16)} stream ${streamId}`);
      }
    } catch (e) {
      console.error("[wisp] message handler error:", e);
    }
  });

  ws.addEventListener("close", (event) => {
    console.log("[wisp] WebSocket closed:", event.code, event.reason);
    for (const { writer, socket, hostname, port } of streams.values()) {
      try { writer.close(); } catch {}
      try { socket.close(); } catch {}
    }
    streams.clear();
  });

  ws.addEventListener("error", (event) => {
    console.error("[wisp] WebSocket error:", event);
  });
}

export default {
  async fetch(request, env, ctx) {
    if (request.headers.get("Upgrade") !== "websocket") {
      return new Response("Wisp server running", { status: 200 });
    }

    const [client, server] = Object.values(new WebSocketPair());
    server.accept();
    ctx.waitUntil(handleWisp(server));

    return new Response(null, { status: 101, webSocket: client });
  },
};