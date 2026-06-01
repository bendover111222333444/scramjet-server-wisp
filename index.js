import { server as wisp } from "@mercuryworkshop/wisp-js/server";
import { createServer } from "node:http";

const server = createServer();
server.on("upgrade", (req, socket, head) => {
    wisp.routeRequest(req, socket, head);
});

server.listen(process.env.PORT || 8080);