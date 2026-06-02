import { server as wisp } from "@mercuryworkshop/wisp-js/server";

export default {
  async fetch(request, env, ctx) {
    if (request.headers.get("Upgrade") === "websocket") {
      return wisp.handleRequest(request);
    }
    return new Response("Wisp Server", { status: 200 });
  }
};
