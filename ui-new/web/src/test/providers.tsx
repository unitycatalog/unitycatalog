import type { ReactElement, ReactNode } from "react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { TransportProvider } from "@connectrpc/connect-query";
import { createRouterTransport, type Transport } from "@connectrpc/connect";
import { render, renderHook } from "@testing-library/react";
import { UnityProxyService } from "@/gen/uc/v1/proxy_pb";
import { ThemeProvider } from "@/lib/theme";

// A single UC call as the bridge would forward it, plus the canned reply a test
// wants back. The handler dispatches on method + path (+ optional query).
export type UcReply = { httpStatus?: number; body?: string; ok?: boolean };
export type UcHandler = (call: {
  method: string;
  path: string;
  query: { key: string; value: string }[];
  jsonBody: string;
  token: string;
  contentType: string;
}) => UcReply;

const okAll: UcHandler = () => ({ httpStatus: 200, body: "{}", ok: true });

// Builds an in-memory Connect transport implementing UnityProxyService.Call, so
// useUcQuery / ucCall exercise their real code paths against canned UC replies.
export function makeTransport(handler: UcHandler = okAll): Transport {
  return createRouterTransport(({ service }) => {
    service(UnityProxyService, {
      call(req) {
        const reply = handler({
          method: req.method,
          path: req.path,
          query: req.query.map((kv) => ({ key: kv.key, value: kv.value })),
          jsonBody: req.jsonBody,
          token: req.token,
          contentType: req.contentType,
        });
        return {
          httpStatus: reply.httpStatus ?? 200,
          body: reply.body ?? "",
          ok: reply.ok ?? (reply.httpStatus ?? 200) < 400,
        };
      },
    });
  });
}

export function makeQueryClient(): QueryClient {
  return new QueryClient({
    defaultOptions: { queries: { retry: false }, mutations: { retry: false } },
  });
}

export function Providers({
  children,
  transport,
  client,
}: {
  children: ReactNode;
  transport?: Transport;
  client?: QueryClient;
}) {
  const t = transport ?? makeTransport();
  const qc = client ?? makeQueryClient();
  return (
    <ThemeProvider>
      <TransportProvider transport={t}>
        <QueryClientProvider client={qc}>{children}</QueryClientProvider>
      </TransportProvider>
    </ThemeProvider>
  );
}

export function renderWithProviders(
  ui: ReactElement,
  opts: { handler?: UcHandler; transport?: Transport; client?: QueryClient } = {},
) {
  const transport = opts.transport ?? makeTransport(opts.handler);
  const client = opts.client ?? makeQueryClient();
  return render(
    <Providers transport={transport} client={client}>
      {ui}
    </Providers>,
  );
}

export function renderHookWithProviders<T>(
  cb: () => T,
  opts: { handler?: UcHandler; transport?: Transport; client?: QueryClient } = {},
) {
  const transport = opts.transport ?? makeTransport(opts.handler);
  const client = opts.client ?? makeQueryClient();
  return renderHook(cb, {
    wrapper: ({ children }) => (
      <Providers transport={transport} client={client}>
        {children}
      </Providers>
    ),
  });
}
