import { connect } from "cloudflare:sockets";
import { server as wisp } from "@mercuryworkshop/wisp-js/server";

class CloudflareTCPSocket {
  constructor(hostname, port) {
    this.hostname = hostname;
    this.port = port;
    this.recv_buffer_size = 128;
    this.connected = false;
    this._socket = null;
    this._reader = null;
    this._writer = null;
    this._queue = [];
    this._waiters = [];
    this._closed = false;
    this.paused = false;
  }

  async connect() {
    this._socket = connect({ hostname: this.hostname, port: this.port });
    this._writer = this._socket.writable.getWriter();
    this._reader = this._socket.readable.getReader();
    this.connected = true;
    this._readLoop();
  }

  async _readLoop() {
    try {
      while (true) {
        const { done, value } = await this._reader.read();
        if (done) break;
        if (this._waiters.length > 0) {
          this._waiters.shift()(value);
        } else {
          this._queue.push(value);
        }
      }
    } catch {}
    this._closed = true;
    this._waiters.forEach(w => w(null));
    this._waiters = [];
  }

  async recv() {
    if (this._queue.length > 0) return this._queue.shift();
    if (this._closed) return null;
    return new Promise(resolve => this._waiters.push(resolve));
  }

  async send(data) {
    await this._writer.write(data);
  }

  async close() {
    try { this._writer?.close(); } catch {}
    try { this._reader?.cancel(); } catch {}
    this._socket?.close();
  }

  pause() {}
  resume() {}
}

export default {
  async fetch(request, env, ctx) {
    if (request.headers.get("Upgrade") !== "websocket") {
      return new Response("Wisp server", { status: 200 });
    }

    const [client, server] = Object.values(new WebSocketPair());
    server.accept();

    const conn = new wisp.ServerConnection(server, "/wisp/", {
      TCPSocket: CloudflareTCPSocket,
    });

    ctx.waitUntil(conn.setup().then(() => conn.run()));

    return new Response(null, { status: 101, webSocket: client });
  },
};