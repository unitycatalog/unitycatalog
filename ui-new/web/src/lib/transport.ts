import { createClient } from "@connectrpc/connect";
import { createConnectTransport } from "@connectrpc/connect-web";
import { addStaticKeyToTransport } from "@connectrpc/connect-query";
import { UnityProxyService } from "@/gen/uc/v1/proxy_pb";

// Same-origin baseUrl: in dev, Vite proxies /uc.v1.* to the Rust bridge; in prod
// the bridge serves the SPA and the RPCs from one origin. Because it is
// same-origin, the browser's fetch sends the auth cookie by default
// (credentials: "same-origin"), which the bridge forwards to the UC server.
//
// A static transport key gives connect-query stable query keys across reloads.
export const transport = addStaticKeyToTransport(
  createConnectTransport({ baseUrl: import.meta.env.VITE_API_BASE ?? "/" }),
  "uc",
);

export const proxyClient = createClient(UnityProxyService, transport);
