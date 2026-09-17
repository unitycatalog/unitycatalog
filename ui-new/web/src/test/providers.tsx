import type { ReactElement, ReactNode } from "react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { TransportProvider } from "@connectrpc/connect-query";
import { createRouterTransport, type Transport } from "@connectrpc/connect";
import { render, renderHook } from "@testing-library/react";
import { UnityProxyService } from "@/gen/uc/v1/proxy_pb";
import { CatalogService } from "@/gen/uc/v1/catalog_pb";
import { SchemaService } from "@/gen/uc/v1/schema_pb";
import {
  DataSourceFormat,
  TableService,
  TableType,
} from "@/gen/uc/v1/table_pb";
import { VolumeService, VolumeType } from "@/gen/uc/v1/volume_pb";
import { FunctionService } from "@/gen/uc/v1/function_pb";
import { ModelService, ModelVersionStatus } from "@/gen/uc/v1/model_pb";
import { ViewService } from "@/gen/uc/v1/view_pb";
import { ColumnTypeName } from "@/gen/uc/v1/common_pb";
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
    registerDomainServices(service, handler);
  });
}

type KeyValue = { key: string; value: string };
type Json = Record<string, unknown>;
type ServiceRegistrar = Parameters<
  Parameters<typeof createRouterTransport>[0]
>[0]["service"];

function registerDomainServices(service: ServiceRegistrar, handler: UcHandler) {
  const raw = (method: string, path: string, query: KeyValue[] = []): Json => {
    const reply = handler({
      method,
      path,
      query,
      jsonBody: "",
      token: "",
      contentType: "",
    });
    if (!reply.ok)
      throw new Error(
        `UC test handler returned ${reply.httpStatus} for ${path}`,
      );
    return reply.body ? JSON.parse(reply.body) : {};
  };
  const list = (
    path: string,
    query: KeyValue[],
    key: string,
    map: (value: Json) => object,
  ) => {
    const value = raw("GET", path, query);
    return {
      items: ((value[key] as Json[] | undefined) ?? []).map(map),
      page: { nextPageToken: string(value.next_page_token) },
    };
  };
  const succeed = () => ({});

  service(CatalogService, {
    listCatalogs(request) {
      const result = list(
        "/api/2.1/unity-catalog/catalogs",
        pageQuery(request.page),
        "catalogs",
        catalog,
      );
      return { catalogs: result.items, page: result.page };
    },
    getCatalog: (request) => ({
      catalog: catalog(
        raw(
          "GET",
          `/api/2.1/unity-catalog/catalogs/${request.catalog?.name ?? ""}`,
        ),
      ),
    }),
    createCatalog: succeed,
    updateCatalog: succeed,
    deleteCatalog: succeed,
  });
  service(SchemaService, {
    listSchemas(request) {
      const result = list(
        "/api/2.1/unity-catalog/schemas",
        [
          ...pageQuery(request.page),
          { key: "catalog_name", value: request.catalog?.name ?? "" },
        ],
        "schemas",
        schema,
      );
      return { schemas: result.items, page: result.page };
    },
    getSchema: (request) => ({
      schema: schema(
        raw(
          "GET",
          `/api/2.1/unity-catalog/schemas/${request.schema?.catalogName ?? ""}.${request.schema?.name ?? ""}`,
        ),
      ),
    }),
    createSchema: succeed,
    updateSchema: succeed,
    deleteSchema: succeed,
  });
  service(TableService, {
    listTables(request) {
      const result = list(
        "/api/2.1/unity-catalog/tables",
        [
          ...pageQuery(request.page),
          { key: "catalog_name", value: request.schema?.catalogName ?? "" },
          { key: "schema_name", value: request.schema?.name ?? "" },
        ],
        "tables",
        table,
      );
      return {
        tables: result.items.filter(
          (item) =>
            (item as { tableType: number }).tableType !== TableType.METRIC_VIEW,
        ),
        page: result.page,
      };
    },
    getTable: (request) => ({
      table: table(
        raw("GET", `/api/2.1/unity-catalog/tables/${fullName(request.table)}`),
      ),
    }),
    createTable: succeed,
    deleteTable: succeed,
  });
  service(VolumeService, {
    listVolumes(request) {
      const result = list(
        "/api/2.1/unity-catalog/volumes",
        [
          ...pageQuery(request.page),
          { key: "catalog_name", value: request.schema?.catalogName ?? "" },
          { key: "schema_name", value: request.schema?.name ?? "" },
        ],
        "volumes",
        volume,
      );
      return { volumes: result.items, page: result.page };
    },
    getVolume: (request) => ({
      volume: volume(
        raw(
          "GET",
          `/api/2.1/unity-catalog/volumes/${fullName(request.volume)}`,
        ),
      ),
    }),
    createVolume: succeed,
    updateVolume: succeed,
    deleteVolume: succeed,
  });
  service(FunctionService, {
    listFunctions(request) {
      const result = list(
        "/api/2.1/unity-catalog/functions",
        [
          ...pageQuery(request.page),
          { key: "catalog_name", value: request.schema?.catalogName ?? "" },
          { key: "schema_name", value: request.schema?.name ?? "" },
        ],
        "functions",
        func,
      );
      return { functions: result.items, page: result.page };
    },
    getFunction: (request) => ({
      function: func(
        raw(
          "GET",
          `/api/2.1/unity-catalog/functions/${fullName(request.function)}`,
        ),
      ),
    }),
    createFunction: succeed,
    deleteFunction: succeed,
  });
  service(ModelService, {
    listRegisteredModels(request) {
      const result = list(
        "/api/2.1/unity-catalog/models",
        [
          ...pageQuery(request.page),
          { key: "catalog_name", value: request.schema?.catalogName ?? "" },
          { key: "schema_name", value: request.schema?.name ?? "" },
        ],
        "registered_models",
        model,
      );
      return { models: result.items, page: result.page };
    },
    getRegisteredModel: (request) => ({
      model: model(
        raw("GET", `/api/2.1/unity-catalog/models/${fullName(request.model)}`),
      ),
    }),
    listModelVersions(request) {
      const result = list(
        `/api/2.1/unity-catalog/models/${fullName(request.model)}/versions`,
        pageQuery(request.page),
        "model_versions",
        modelVersion,
      );
      return { versions: result.items, page: result.page };
    },
    getModelVersion: (request) => ({
      modelVersion: modelVersion(
        raw(
          "GET",
          `/api/2.1/unity-catalog/models/${fullName(request.modelVersion?.model)}/versions/${request.modelVersion?.version ?? 0}`,
        ),
      ),
    }),
    createRegisteredModel: succeed,
    updateRegisteredModel: succeed,
    deleteRegisteredModel: succeed,
    createModelVersion: succeed,
    updateModelVersion: succeed,
    finalizeModelVersion: succeed,
    deleteModelVersion: succeed,
  });
  service(ViewService, {
    listViews(request) {
      const result = list(
        "/api/2.1/unity-catalog/tables",
        [
          ...pageQuery(request.page),
          { key: "catalog_name", value: request.schema?.catalogName ?? "" },
          { key: "schema_name", value: request.schema?.name ?? "" },
        ],
        "tables",
        metricView,
      );
      return {
        views: result.items.filter(
          (item) =>
            (item as { tableType: number }).tableType === TableType.METRIC_VIEW,
        ),
        page: result.page,
      };
    },
    getView: (request) => ({
      view: metricView(
        raw("GET", `/api/2.1/unity-catalog/tables/${fullName(request.view)}`),
      ),
    }),
    createView: succeed,
    deleteView: succeed,
  });
}

function string(value: unknown): string {
  return typeof value === "string" ? value : "";
}

function enumValue(values: object, value: unknown): number {
  return typeof value === "string"
    ? ((values as Record<string, number>)[value] ?? 0)
    : typeof value === "number"
      ? value
      : 0;
}

function timestamp(value: unknown) {
  if (typeof value !== "number") return undefined;
  return {
    seconds: BigInt(Math.trunc(value / 1000)),
    nanos: (value % 1000) * 1_000_000,
  };
}

function audit(value: Json) {
  return {
    owner: string(value.owner),
    createdAt: timestamp(value.created_at),
    createdBy: string(value.created_by),
    updatedAt: timestamp(value.updated_at),
    updatedBy: string(value.updated_by),
  };
}

function common(value: Json) {
  return {
    id: string(
      value.id ??
        value.catalog_id ??
        value.schema_id ??
        value.table_id ??
        value.volume_id ??
        value.function_id,
    ),
    name: string(value.name),
    fullName: string(value.full_name),
    comment: value.comment == null ? undefined : string(value.comment),
    properties: {
      values: (value.properties as Record<string, string> | undefined) ?? {},
    },
    audit: audit(value),
  };
}

function catalog(value: Json) {
  return { ...common(value), storageRoot: string(value.storage_root) };
}

function schema(value: Json) {
  return {
    ...common(value),
    catalogName: string(value.catalog_name),
    storageRoot: string(value.storage_root),
  };
}

function column(value: Json) {
  return {
    name: string(value.name),
    typeText: string(value.type_text),
    typeName: enumValue(ColumnTypeName, value.type_name),
    nullable: Boolean(value.nullable),
    position: Number(value.position ?? 0),
    comment: value.comment == null ? undefined : string(value.comment),
  };
}

function table(value: Json) {
  return {
    ...common(value),
    catalogName: string(value.catalog_name),
    schemaName: string(value.schema_name),
    tableType: enumValue(TableType, value.table_type),
    dataSourceFormat: enumValue(DataSourceFormat, value.data_source_format),
    columns: ((value.columns as Json[] | undefined) ?? []).map(column),
    storageLocation: string(value.storage_location),
  };
}

function volume(value: Json) {
  return {
    ...common(value),
    catalogName: string(value.catalog_name),
    schemaName: string(value.schema_name),
    volumeType: enumValue(VolumeType, value.volume_type),
    storageLocation: string(value.storage_location),
  };
}

function func(value: Json) {
  const parameters =
    ((value.input_params as Json | undefined)?.parameters as
      Json[] | undefined) ?? [];
  return {
    ...common(value),
    catalogName: string(value.catalog_name),
    schemaName: string(value.schema_name),
    inputParameters: parameters.map(column),
    dataType: enumValue(ColumnTypeName, value.data_type),
    fullDataType: string(value.full_data_type),
    routineDefinition: string(value.routine_definition),
  };
}

function model(value: Json) {
  return {
    ...common(value),
    catalogName: string(value.catalog_name),
    schemaName: string(value.schema_name),
  };
}

function modelVersion(value: Json) {
  return {
    ...common(value),
    modelName: string(value.model_name),
    version: BigInt((value.version as number | string | undefined) ?? 0),
    status: enumValue(ModelVersionStatus, value.status),
    source: string(value.source),
    runId: string(value.run_id),
    storageLocation: string(value.storage_location),
  };
}

function metricView(value: Json) {
  return { ...table(value), viewDefinition: string(value.view_definition) };
}

function fullName(reference?: {
  catalogName: string;
  schemaName: string;
  name: string;
}): string {
  if (!reference) return "";
  return `${reference.catalogName}.${reference.schemaName}.${reference.name}`;
}

function pageQuery(page?: {
  maxResults?: number;
  pageToken: string;
}): KeyValue[] {
  if (!page) return [];
  const query: KeyValue[] = [];
  if (page.maxResults !== undefined)
    query.push({ key: "max_results", value: String(page.maxResults) });
  if (page.pageToken) query.push({ key: "page_token", value: page.pageToken });
  return query;
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
  opts: {
    handler?: UcHandler;
    transport?: Transport;
    client?: QueryClient;
  } = {},
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
  opts: {
    handler?: UcHandler;
    transport?: Transport;
    client?: QueryClient;
  } = {},
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
