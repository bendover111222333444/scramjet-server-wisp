import { connect } from "cloudflare:sockets";

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

async function handleWisp(ws) {
  const streams = new Map();

  // REQUIRED: send initial CONTINUE packet immediately on connect
  // stream ID 0 = Wisp v1 handshake, buffer size = 128
  const initial = new ArrayBuffer(9);
  const view = new DataView(initial);
  view.setUint8(0, 0x03);        // CONTINUE type
  view.setUint32(1, 0, true);    // stream ID 0
  view.setUint32(5, 128, true);  // buffer size
  ws.send(initial);

  ws.addEventListener("message", async (event) => {
    try {
      const buf = event.data;
      const view = new DataView(buf);
      const type = view.getUint8(0);
      const streamId = view.getUint32(1, true);
      const payload = buf.slice(5);

      if (type === 0x01) { // CONNECT
        const streamType = new DataView(payload).getUint8(0);
        const port = new DataView(payload).getUint16(1, true);
        const hostname = new TextDecoder().decode(payload.slice(3));

        try {
          const socket = connect({ hostname, port });
          streams.set(streamId, socket);

          // send CONTINUE for this stream
          const cont = new ArrayBuffer(9);
          const cv = new DataView(cont);
          cv.setUint8(0, 0x03);
          cv.setUint32(1, streamId, true);
          cv.setUint32(5, 128, true);
          ws.send(cont);

          // pipe TCP → WebSocket
          socket.readable.pipeTo(new WritableStream({
            write(chunk) {
              const header = new ArrayBuffer(5);
              const hv = new DataView(header);
              hv.setUint8(0, 0x02); // DATA
              hv.setUint32(1, streamId, true);
              const out = new Uint8Array(5 + chunk.byteLength);
              out.set(new Uint8Array(header), 0);
              out.set(new Uint8Array(chunk), 5);
              ws.send(out);
            },
            close() {
              const close = new ArrayBuffer(6);
              const cv = new DataView(close);
              cv.setUint8(0, 0x04); // CLOSE
              cv.setUint32(1, streamId, true);
              cv.setUint8(5, 0x02);
              ws.send(close);
              streams.delete(streamId);
            },
            abort() {
              streams.delete(streamId);
            }
          })).catch(() => streams.delete(streamId));

        } catch (e) {
          // send CLOSE with unreachable reason
          const close = new ArrayBuffer(6);
          const cv = new DataView(close);
          cv.setUint8(0, 0x04);
          cv.setUint32(1, streamId, true);
          cv.setUint8(5, 0x42);
          ws.send(close);
        }

      } else if (type === 0x02) { // DATA
        const stream = streams.get(streamId);
        if (stream) {
          const writer = stream.writable.getWriter();
          await writer.write(new Uint8Array(payload));
          writer.releaseLock();
        }

      } else if (type === 0x04) { // CLOSE
        const stream = streams.get(streamId);
        if (stream) {
          stream.close?.();
          streams.delete(streamId);
        }
      }
    } catch (e) {}
  });

  ws.addEventListener("close", () => {
    for (const stream of streams.values()) {
      try { stream.close?.(); } catch {}
    }
    streams.clear();
  });
}