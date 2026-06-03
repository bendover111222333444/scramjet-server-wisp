import { connect } from "cloudflare:sockets";

const BUFFER_SIZE = 128;

function makePacket(type, streamId, payload) {
  const buf = new ArrayBuffer(5 + payload.byteLength);
  const view = new DataView(buf);
  view.setUint8(0, type);
  view.setUint32(1, streamId, true);
  new Uint8Array(buf).set(new Uint8Array(payload), 5);
  return buf;
}

function continuePacket(streamId) {
  const payload = new ArrayBuffer(4);
  new DataView(payload).setUint32(0, BUFFER_SIZE, true);
  return makePacket(0x03, streamId, payload);
}

function closePacket(streamId, reason = 0x01) {
  const payload = new Uint8Array([reason]);
  return makePacket(0x04, streamId, payload.buffer);
}

async function handleWisp(ws) {
  const streams = new Map();

  // send initial CONTINUE (stream 0 = handshake)
  ws.send(continuePacket(0));

  ws.addEventListener("message", async (event) => {
    try {
      const buf = event.data;
      const view = new DataView(buf);
      const type = view.getUint8(0);
      const streamId = view.getUint32(1, true);
      const payload = buf.slice(5);

      if (type === 0x01) { // CONNECT
        const pv = new DataView(payload);
        const port = pv.getUint16(1, true);
        const hostname = new TextDecoder().decode(payload.slice(3));

        try {
          const socket = connect({ hostname: hostname.trim(), port });
          const writer = socket.writable.getWriter();
          streams.set(streamId, { writer, socket });

          // send CONTINUE for new stream
          ws.send(continuePacket(streamId));

          // pipe TCP → WS
          (async () => {
            const reader = socket.readable.getReader();
            try {
              while (true) {
                const { done, value } = await reader.read();
                if (done) break;
                ws.send(makePacket(0x02, streamId, value.buffer));
              }
            } catch {}
            ws.send(closePacket(streamId, 0x02));
            streams.delete(streamId);
          })();

        } catch {
          ws.send(closePacket(streamId, 0x42));
        }

      } else if (type === 0x02) { // DATA
        const stream = streams.get(streamId);
        if (stream) {
          try {
            await stream.writer.write(new Uint8Array(payload));
          } catch {}
        }

      } else if (type === 0x04) { // CLOSE
        const stream = streams.get(streamId);
        if (stream) {
          try { stream.writer.close(); } catch {}
          try { stream.socket.close(); } catch {}
          streams.delete(streamId);
        }
      }
    } catch {}
  });

  ws.addEventListener("close", () => {
    for (const { writer, socket } of streams.values()) {
      try { writer.close(); } catch {}
      try { socket.close(); } catch {}
    }
    streams.clear();
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