import { useQuery, type UseQueryOptions } from "@tanstack/react-query";
import { callUnaryMethod, createConnectQueryKey, useTransport } from "@connectrpc/connect-query";
import { UnityProxyService } from "@/gen/uc/v1/proxy_pb";
import { UCError } from "@/lib/uc";
import { useToken } from "@/lib/session";

// The single generic UC REST proxy RPC. Every scoped fetch flows through it so
// connect-query owns a stable, structured query key (method + input), which is
// what makes the TanStack Query cache consistent across components and reloads.
const callMethod = UnityProxyService.method.call;

function toKV(query?: Record<string, string | undefined>) {
  const out: { key: string; value: string }[] = [];
  if (!query) return out;
  for (const [key, value] of Object.entries(query)) {
    if (value !== undefined && value !== "") out.push({ key, value });
  }
  return out;
}

export type UcQueryOpts<T> = {
  query?: Record<string, string | undefined>;
  // Extra TanStack Query options (enabled, staleTime, select, ...).
  queryOptions?: Partial<UseQueryOptions<T, Error, T>>;
};

// useUcQuery runs a GET (or other verb) against the UC REST API through the Rust
// bridge, cached by connect-query/TanStack Query. Non-2xx UC responses throw a
// UCError so the query lands in an error state; a 2xx body is JSON-parsed.
export function useUcQuery<T = unknown>(method: string, path: string, opts: UcQueryOpts<T> = {}) {
  const transport = useTransport();
  // Re-keys queries when the pasted bearer token changes (sign in / out).
  const token = useToken();
  const input = {
    serverUrl: "",
    token,
    method,
    path,
    query: toKV(opts.query),
    jsonBody: "",
    contentType: "",
  };

  return useQuery<T, Error, T>({
    queryKey: createConnectQueryKey({
      schema: callMethod,
      transport,
      input,
      cardinality: "finite",
    }),
    queryFn: async () => {
      const res = await callUnaryMethod(transport, callMethod, input);
      if (!res.ok) throw new UCError(res.httpStatus, res.body);
      return (res.body ? JSON.parse(res.body) : undefined) as T;
    },
    ...opts.queryOptions,
  });
}
