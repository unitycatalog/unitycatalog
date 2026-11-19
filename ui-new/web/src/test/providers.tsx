import type { ReactElement, ReactNode } from "react";
import { type QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { TransportProvider } from "@connectrpc/connect-query";
import {
  createRouterTransport,
  type ServiceImpl,
  type Transport,
} from "@connectrpc/connect";
import { render, renderHook } from "@testing-library/react";
import { UnityProxyService } from "@/gen/uc/v1/proxy_pb";
import { CatalogService } from "@/gen/uc/v1/catalog_pb";
import { SchemaService } from "@/gen/uc/v1/schema_pb";
import { TableService } from "@/gen/uc/v1/table_pb";
import { VolumeService } from "@/gen/uc/v1/volume_pb";
import { FunctionService } from "@/gen/uc/v1/function_pb";
import { ModelService } from "@/gen/uc/v1/model_pb";
import { ViewService } from "@/gen/uc/v1/view_pb";
import { createQueryClient } from "@/lib/queryClient";
import { ThemeProvider } from "@/lib/theme";

export type UcReply = { httpStatus?: number; body?: string; ok?: boolean };
export type UcHandler = (call: {
  method: string;
  path: string;
  query: { key: string; value: string }[];
  jsonBody: string;
  contentType: string;
}) => UcReply;

export type RpcHandlers = {
  catalogs?: Partial<ServiceImpl<typeof CatalogService>>;
  schemas?: Partial<ServiceImpl<typeof SchemaService>>;
  tables?: Partial<ServiceImpl<typeof TableService>>;
  volumes?: Partial<ServiceImpl<typeof VolumeService>>;
  functions?: Partial<ServiceImpl<typeof FunctionService>>;
  models?: Partial<ServiceImpl<typeof ModelService>>;
  views?: Partial<ServiceImpl<typeof ViewService>>;
};

const okAll: UcHandler = () => ({ httpStatus: 200, body: "{}", ok: true });
const succeed = () => ({});

const mutationDefaults = {
  catalogs: {
    createCatalog: succeed,
    updateCatalog: succeed,
    deleteCatalog: succeed,
  },
  schemas: {
    createSchema: succeed,
    updateSchema: succeed,
    deleteSchema: succeed,
  },
  tables: { createTable: succeed, deleteTable: succeed },
  volumes: {
    createVolume: succeed,
    updateVolume: succeed,
    deleteVolume: succeed,
  },
  functions: { createFunction: succeed, deleteFunction: succeed },
  models: {
    createRegisteredModel: succeed,
    updateRegisteredModel: succeed,
    deleteRegisteredModel: succeed,
    createModelVersion: succeed,
    updateModelVersion: succeed,
    finalizeModelVersion: succeed,
    deleteModelVersion: succeed,
  },
  views: { createView: succeed, deleteView: succeed },
} satisfies RpcHandlers;

export function makeTransport(
  handler: UcHandler = okAll,
  rpc: RpcHandlers = {},
): Transport {
  return createRouterTransport(({ service }) => {
    service(UnityProxyService, {
      call(request) {
        const reply = handler({
          method: request.method,
          path: request.path,
          query: request.query.map(({ key, value }) => ({ key, value })),
          jsonBody: request.jsonBody,
          contentType: request.contentType,
        });
        return {
          httpStatus: reply.httpStatus ?? 200,
          body: reply.body ?? "",
          ok: reply.ok ?? (reply.httpStatus ?? 200) < 400,
        };
      },
    });
    service(CatalogService, { ...mutationDefaults.catalogs, ...rpc.catalogs });
    service(SchemaService, { ...mutationDefaults.schemas, ...rpc.schemas });
    service(TableService, { ...mutationDefaults.tables, ...rpc.tables });
    service(VolumeService, { ...mutationDefaults.volumes, ...rpc.volumes });
    service(FunctionService, {
      ...mutationDefaults.functions,
      ...rpc.functions,
    });
    service(ModelService, { ...mutationDefaults.models, ...rpc.models });
    service(ViewService, { ...mutationDefaults.views, ...rpc.views });
  });
}

export function makeQueryClient(): QueryClient {
  return createQueryClient();
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
  return (
    <ThemeProvider>
      <TransportProvider transport={transport ?? makeTransport()}>
        <QueryClientProvider client={client ?? makeQueryClient()}>
          {children}
        </QueryClientProvider>
      </TransportProvider>
    </ThemeProvider>
  );
}

type RenderOptions = {
  handler?: UcHandler;
  rpc?: RpcHandlers;
  transport?: Transport;
  client?: QueryClient;
};

function options({ handler, rpc, transport, client }: RenderOptions) {
  return {
    transport: transport ?? makeTransport(handler, rpc),
    client: client ?? makeQueryClient(),
  };
}

export function renderWithProviders(
  ui: ReactElement,
  opts: RenderOptions = {},
) {
  const providers = options(opts);
  return render(<Providers {...providers}>{ui}</Providers>);
}

export function renderHookWithProviders<T>(
  callback: () => T,
  opts: RenderOptions = {},
) {
  const providers = options(opts);
  return renderHook(callback, {
    wrapper: ({ children }) => <Providers {...providers}>{children}</Providers>,
  });
}
