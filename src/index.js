import { connect } from "cloudflare:sockets";

export default {
  async fetch(request, env) {
    if (request.headers.get("Upgrade") !== "websocket") {
      return new Response("Wisp server running", { status: 200 });
    }

    const [client, server] = Object.values(new WebSocketPair());
    server.accept();

    handleWisp(server).catch(() => {});

    return new Response(null, {
      status: 101,
      webSocket: client,
    });
  },
};

async function handleWisp(ws) {
  const streams = new Map();

  ws.addEventListener("message", async (event) => {
    const data = new Uint8Array(event.data);
    const type = data[0];
    const streamId = new DataView(data.buffer).getUint32(1, true);

    if (type === 0x01) { // CONNECT
      const port = new DataView(data.buffer).getUint16(5, true);
      const hostname = new TextDecoder().decode(data.slice(7));

      try {
        const socket = connect({ hostname, port });
        streams.set(streamId, socket);

        const writer = socket.writable.getWriter();

        socket.readable.pipeTo(new WritableStream({
          write(chunk) {
            const header = new Uint8Array(5);
            header[0] = 0x02; // DATA
            new DataView(header.buffer).setUint32(1, streamId, true);
            const msg = new Uint8Array(header.length + chunk.length);
            msg.set(header);
            msg.set(chunk, header.length);
            ws.send(msg);
          },
          close() {
            const header = new Uint8Array(5);
            header[0] = 0x03; // CLOSE
            new DataView(header.buffer).setUint32(1, streamId, true);
            ws.send(header);
            streams.delete(streamId);
          }
        })).catch(() => {});

        ws.send((() => {
          const buf = new Uint8Array(5);
          buf[0] = 0x02;
          new DataView(buf.buffer).setUint32(1, streamId, true);
          return buf;
        })());

      } catch (e) {
        streams.delete(streamId);
      }

    } else if (type === 0x02) { // DATA
      const socket = streams.get(streamId);
      if (socket) {
        const writer = socket.writable.getWriter();
        await writer.write(data.slice(5));
        writer.releaseLock();
      }

    } else if (type === 0x03) { // CLOSE
      const socket = streams.get(streamId);
      if (socket) {
        socket.close();
        streams.delete(streamId);
      }
    }
  });

  ws.addEventListener("close", () => {
    for (const socket of streams.values()) socket.close();
    streams.clear();
  });
}
