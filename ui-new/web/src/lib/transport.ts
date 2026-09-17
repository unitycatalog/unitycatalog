import { createClient, type Interceptor } from "@connectrpc/connect";
import { createConnectTransport } from "@connectrpc/connect-web";
import { addStaticKeyToTransport } from "@connectrpc/connect-query";
import { UnityProxyService } from "@/gen/uc/v1/proxy_pb";
import { getToken } from "@/lib/session";

// Same-origin baseUrl: in dev, Vite proxies /uc.v1.* to the Rust bridge; in prod
// the bridge serves the SPA and the RPCs from one origin. Because it is
// same-origin, the browser sends the auth cookie, which the bridge forwards to
// the UC server. The explicit credentials mode also supports VITE_API_BASE on
// an allowed cross-origin bridge.
//
// A static transport key gives connect-query stable query keys across reloads.
const authInterceptor: Interceptor = (next) => async (request) => {
  const token = getToken();
  if (token) request.header.set("Authorization", `Bearer ${token}`);
  return next(request);
};

export const transport = addStaticKeyToTransport(
  createConnectTransport({
    baseUrl: import.meta.env.VITE_API_BASE ?? "/",
    interceptors: [authInterceptor],
    fetch: (input, init) =>
      globalThis.fetch(input, { ...init, credentials: "include" }),
  }),
  "uc",
);

export const proxyClient = createClient(UnityProxyService, transport);
