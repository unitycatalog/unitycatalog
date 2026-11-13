import { describe, expect, it } from "vitest";
import { act, waitFor } from "@testing-library/react";
import type { DescMessage, DescMethodUnary } from "@bufbuild/protobuf";
import type { QueryClient, QueryKey } from "@tanstack/react-query";
import { createConnectQueryKey } from "@connectrpc/connect-query";
import { createRouterTransport } from "@connectrpc/connect";
import { CatalogService } from "@/gen/uc/v1/catalog_pb";
import { FunctionService } from "@/gen/uc/v1/function_pb";
import { ModelService } from "@/gen/uc/v1/model_pb";
import { UnityProxyService } from "@/gen/uc/v1/proxy_pb";
import { SchemaService } from "@/gen/uc/v1/schema_pb";
import { DataSourceFormat, TableService } from "@/gen/uc/v1/table_pb";
import { ViewService } from "@/gen/uc/v1/view_pb";
import { VolumeService, VolumeType } from "@/gen/uc/v1/volume_pb";
import { ColumnTypeName } from "@/gen/uc/v1/common_pb";
import {
  useCreateCatalog,
  useDeleteCatalog,
  useGetCatalog,
  useUpdateCatalog,
} from "@/hooks/catalog";
import {
  useCreateSchema,
  useDeleteSchema,
  useUpdateSchema,
} from "@/hooks/schemas";
import { useCreateTable, useDeleteTable } from "@/hooks/tables";
import {
  useCreateVolume,
  useDeleteVolume,
  useUpdateVolume,
} from "@/hooks/volumes";
import { useCreateFunction, useDeleteFunction } from "@/hooks/functions";
import {
  useCreateModel,
  useCreateModelVersion,
  useDeleteModel,
  useDeleteModelVersion,
  useFinalizeModelVersion,
  useUpdateModel,
  useUpdateModelVersion,
} from "@/hooks/models";
import { useCreateView, useDeleteView } from "@/hooks/views";
import { usePermissions, type SecurableType } from "@/hooks/permissions";
import {
  makeQueryClient,
  makeTransport,
  renderHookWithProviders,
  type UcHandler,
} from "@/test/providers";

type CatalogHooks = {
  detail: ReturnType<typeof useGetCatalog>;
  update: ReturnType<typeof useUpdateCatalog>;
  remove: ReturnType<typeof useDeleteCatalog>;
};

function useDetailHooks() {
  return {
    deleteCatalog: useDeleteCatalog(),
    updateSchema: useUpdateSchema(),
    deleteSchema: useDeleteSchema(),
    deleteTable: useDeleteTable(),
    updateVolume: useUpdateVolume(),
    deleteVolume: useDeleteVolume(),
    deleteFunction: useDeleteFunction(),
    deleteView: useDeleteView(),
    updateModel: useUpdateModel(),
    deleteModel: useDeleteModel(),
    updateModelVersion: useUpdateModelVersion(),
    finalizeModelVersion: useFinalizeModelVersion(),
    deleteModelVersion: useDeleteModelVersion(),
  };
}
type DetailHooks = ReturnType<typeof useDetailHooks>;

describe("domain mutation hooks", () => {
  it("executes every typed mutation and its invalidation callback", async () => {
    const client = makeQueryClient();
    const catalogKey = createConnectQueryKey({
      schema: CatalogService.method.listCatalogs,
      input: {},
      cardinality: "finite",
    });
    const unrelatedKey = ["current-user"];
    client.setQueryData(catalogKey, { catalogs: [] });
    client.setQueryData(unrelatedKey, { displayName: "Ada" });
    const { result } = renderHookWithProviders(
      () => ({
        createCatalog: useCreateCatalog(),
        updateCatalog: useUpdateCatalog(),
        deleteCatalog: useDeleteCatalog(),
        createSchema: useCreateSchema(),
        updateSchema: useUpdateSchema(),
        deleteSchema: useDeleteSchema(),
        createTable: useCreateTable(),
        deleteTable: useDeleteTable(),
        createVolume: useCreateVolume(),
        updateVolume: useUpdateVolume(),
        deleteVolume: useDeleteVolume(),
        createFunction: useCreateFunction(),
        deleteFunction: useDeleteFunction(),
        createModel: useCreateModel(),
        updateModel: useUpdateModel(),
        deleteModel: useDeleteModel(),
        createModelVersion: useCreateModelVersion(),
        updateModelVersion: useUpdateModelVersion(),
        finalizeModelVersion: useFinalizeModelVersion(),
        deleteModelVersion: useDeleteModelVersion(),
        createView: useCreateView(),
        deleteView: useDeleteView(),
      }),
      { client },
    );
    const catalog = { name: "main" };
    const schema = { catalogName: "main", name: "default" };
    const object = {
      catalogName: "main",
      schemaName: "default",
      name: "entity",
    };
    const modelVersion = { model: object, version: 1n };

    await act(async () => {
      await Promise.all([
        result.current.createCatalog.mutateAsync({ name: "main" }),
        result.current.updateCatalog.mutateAsync({
          catalog,
          comment: "updated",
        }),
        result.current.deleteCatalog.mutateAsync({ catalog }),
        result.current.createSchema.mutateAsync({ schema }),
        result.current.updateSchema.mutateAsync({
          schema,
          comment: "updated",
        }),
        result.current.deleteSchema.mutateAsync({ schema }),
        result.current.createTable.mutateAsync({
          table: object,
          dataSourceFormat: DataSourceFormat.DELTA,
          storageLocation: { value: "s3://bucket/table" },
        }),
        result.current.deleteTable.mutateAsync({ table: object }),
        result.current.createVolume.mutateAsync({
          volume: object,
          volumeType: VolumeType.MANAGED,
        }),
        result.current.updateVolume.mutateAsync({
          volume: object,
          comment: "updated",
        }),
        result.current.deleteVolume.mutateAsync({ volume: object }),
        result.current.createFunction.mutateAsync({
          function: object,
          returnType: ColumnTypeName.STRING,
          routineDefinition: "RETURN 'ok'",
        }),
        result.current.deleteFunction.mutateAsync({ function: object }),
        result.current.createModel.mutateAsync({ model: object }),
        result.current.updateModel.mutateAsync({
          model: object,
          comment: "updated",
        }),
        result.current.deleteModel.mutateAsync({ model: object }),
        result.current.createModelVersion.mutateAsync({
          model: object,
          source: "s3://bucket/model",
        }),
        result.current.updateModelVersion.mutateAsync({
          modelVersion,
          comment: "updated",
        }),
        result.current.finalizeModelVersion.mutateAsync({ modelVersion }),
        result.current.deleteModelVersion.mutateAsync({ modelVersion }),
        result.current.createView.mutateAsync({
          view: object,
          columns: [
            {
              name: "value",
              typeName: ColumnTypeName.STRING,
              nullable: true,
            },
          ],
          viewDefinition:
            "version: 1.1\nsource: main.default.source\ndimensions:\n  - name: value\n    expr: value",
          dependencies: [
            {
              target: {
                case: "tableFullName",
                value: "main.default.source",
              },
            },
          ],
        }),
        result.current.deleteView.mutateAsync({ view: object }),
      ]);
    });

    expect(
      Object.values(result.current).every((mutation) => !mutation.isError),
    ).toBe(true);
    expect(client.getQueryState(catalogKey)?.isInvalidated).toBe(true);
    expect(client.getQueryState(unrelatedKey)?.isInvalidated).toBe(false);
  });

  // Serves one catalog and counts its detail requests, so a test can tell a
  // refetch apart from a stale mark.
  function catalogDetail(name: string) {
    const requests = { count: 0 };
    const handler: UcHandler = ({ method, path }) => {
      if (
        method === "GET" &&
        path === `/api/2.1/unity-catalog/catalogs/${name}`
      ) {
        requests.count += 1;
      }
      return { httpStatus: 200, ok: true, body: JSON.stringify({ name }) };
    };
    return { requests, handler };
  }

  function cachedCatalogDetail(client: QueryClient) {
    return client.getQueryCache().find({
      exact: false,
      queryKey: createConnectQueryKey({
        schema: CatalogService.method.getCatalog,
        cardinality: undefined,
      }),
    });
  }

  it("refetches the detail of an edited catalog", async () => {
    const { requests, handler } = catalogDetail("main");
    const { result } = renderHookWithProviders(
      () => ({ detail: useGetCatalog("main"), update: useUpdateCatalog() }),
      { handler },
    );
    await waitFor(() => expect(result.current.detail.isSuccess).toBe(true));

    await act(async () => {
      await result.current.update.mutateAsync({
        catalog: { name: "main" },
        comment: "edited",
      });
    });

    await waitFor(() => expect(requests.count).toBe(2));
  });

  it("does not cache a read that was in flight during a delete as fresh", async () => {
    type Reply = { catalog: { name: string; comment: string } };
    const replies = [
      Promise.withResolvers<Reply>(),
      Promise.withResolvers<Reply>(),
    ];
    let reads = 0;
    const transport = createRouterTransport(({ service }) => {
      service(CatalogService, {
        getCatalog: () => replies[reads++].promise,
        deleteCatalog: () => ({}),
      });
    });
    const client = makeQueryClient();
    const { result } = renderHookWithProviders(
      () => ({ detail: useGetCatalog("main"), remove: useDeleteCatalog() }),
      { client, transport },
    );
    await waitFor(() => expect(reads).toBe(1));

    await act(async () => {
      await result.current.remove.mutateAsync({ catalog: { name: "main" } });
    });
    await act(async () => {
      replies[0].resolve({
        catalog: { name: "main", comment: "before delete" },
      });
      await new Promise((resolve) => setTimeout(resolve, 0));
    });

    // The pre-delete read was sent again, so its reply is dropped.
    expect(reads).toBe(2);
    expect(result.current.detail.data?.comment).toBeUndefined();

    await act(async () => {
      replies[1].resolve({
        catalog: { name: "main", comment: "after delete" },
      });
    });
    await waitFor(() =>
      expect(result.current.detail.data?.comment).toBe("after delete"),
    );
  });

  it.each([
    {
      change: "renamed",
      mutate: (hooks: CatalogHooks) =>
        hooks.update.mutateAsync({ catalog: { name: "old" }, newName: "new" }),
    },
    {
      change: "deleted",
      mutate: (hooks: CatalogHooks) =>
        hooks.remove.mutateAsync({ catalog: { name: "old" } }),
    },
  ])(
    "marks a $change catalog's detail stale without refetching it",
    async ({ mutate }) => {
      const client = makeQueryClient();
      const { requests, handler } = catalogDetail("old");
      const { result } = renderHookWithProviders(
        (): CatalogHooks => ({
          detail: useGetCatalog("old"),
          update: useUpdateCatalog(),
          remove: useDeleteCatalog(),
        }),
        { client, handler },
      );
      await waitFor(() => expect(result.current.detail.isSuccess).toBe(true));

      await act(async () => {
        await mutate(result.current);
        // Let a refetch, if one were triggered, reach the handler.
        await new Promise((resolve) => setTimeout(resolve, 0));
      });

      expect(cachedCatalogDetail(client)?.state.isInvalidated).toBe(true);
      expect(requests.count).toBe(1);
    },
  );

  const object = { catalogName: "main", schemaName: "default", name: "obj" };
  const modelVersion = { model: object, version: 1n };
  it.each([
    {
      name: "updateSchema",
      detail: SchemaService.method.getSchema,
      run: (h: DetailHooks) =>
        h.updateSchema.mutateAsync({
          schema: { catalogName: "main", name: "default" },
          comment: "x",
        }),
    },
    {
      name: "deleteSchema",
      detail: SchemaService.method.getSchema,
      run: (h: DetailHooks) =>
        h.deleteSchema.mutateAsync({
          schema: { catalogName: "main", name: "default" },
        }),
    },
    {
      name: "deleteTable",
      detail: TableService.method.getTable,
      run: (h: DetailHooks) => h.deleteTable.mutateAsync({ table: object }),
    },
    {
      name: "updateVolume",
      detail: VolumeService.method.getVolume,
      run: (h: DetailHooks) =>
        h.updateVolume.mutateAsync({ volume: object, comment: "x" }),
    },
    {
      name: "deleteVolume",
      detail: VolumeService.method.getVolume,
      run: (h: DetailHooks) => h.deleteVolume.mutateAsync({ volume: object }),
    },
    {
      name: "deleteFunction",
      detail: FunctionService.method.getFunction,
      run: (h: DetailHooks) =>
        h.deleteFunction.mutateAsync({ function: object }),
    },
    {
      name: "deleteView",
      detail: ViewService.method.getView,
      run: (h: DetailHooks) => h.deleteView.mutateAsync({ view: object }),
    },
    {
      name: "updateModel",
      detail: ModelService.method.getRegisteredModel,
      run: (h: DetailHooks) =>
        h.updateModel.mutateAsync({ model: object, comment: "x" }),
    },
    {
      name: "deleteModel",
      detail: ModelService.method.getRegisteredModel,
      run: (h: DetailHooks) => h.deleteModel.mutateAsync({ model: object }),
    },
    {
      name: "updateModelVersion",
      detail: ModelService.method.getModelVersion,
      run: (h: DetailHooks) =>
        h.updateModelVersion.mutateAsync({ modelVersion, comment: "x" }),
    },
    {
      name: "finalizeModelVersion",
      detail: ModelService.method.getModelVersion,
      run: (h: DetailHooks) =>
        h.finalizeModelVersion.mutateAsync({ modelVersion }),
    },
    {
      name: "deleteModelVersion",
      detail: ModelService.method.getModelVersion,
      run: (h: DetailHooks) =>
        h.deleteModelVersion.mutateAsync({ modelVersion }),
    },
  ])("$name marks its cached detail stale", async ({ detail, run }) => {
    const client = makeQueryClient();
    const detailKey = createConnectQueryKey({
      schema: detail,
      input: {},
      cardinality: "finite",
    });
    client.setQueryData(detailKey, {});
    const { result } = renderHookWithProviders(useDetailHooks, { client });

    await act(async () => {
      await run(result.current);
    });

    expect(client.getQueryState(detailKey)?.isInvalidated).toBe(true);
  });

  function useParentHooks() {
    return {
      updateCatalog: useUpdateCatalog(),
      deleteCatalog: useDeleteCatalog(),
      updateSchema: useUpdateSchema(),
      deleteSchema: useDeleteSchema(),
      updateModel: useUpdateModel(),
      deleteModel: useDeleteModel(),
    };
  }
  type ParentHooks = ReturnType<typeof useParentHooks>;
  type ParentCase = {
    parent: string;
    update: (hooks: ParentHooks, newName?: string) => Promise<unknown>;
    remove: (hooks: ParentHooks) => Promise<unknown>;
    descendants: DescMethodUnary[];
    unrelated: DescMethodUnary[];
  };
  const methodKey = <I extends DescMessage, O extends DescMessage>(
    schema: DescMethodUnary<I, O>,
  ): QueryKey => createConnectQueryKey({ schema, cardinality: "finite" });
  const callKey = (path: string): QueryKey =>
    createConnectQueryKey({
      schema: UnityProxyService.method.call,
      input: { method: "GET", path },
      cardinality: "finite",
    });
  const grants = callKey(
    "/api/2.1/unity-catalog/permissions/table/old.default.obj",
  );
  const currentUser = callKey("/api/1.0/unity-control/scim2/Me");
  const parents: ParentCase[] = [
    {
      parent: "catalog",
      update: (h, newName) =>
        h.updateCatalog.mutateAsync({ catalog: { name: "old" }, newName }),
      remove: (h) =>
        h.deleteCatalog.mutateAsync({ catalog: { name: "old" }, force: true }),
      descendants: [
        SchemaService.method.listSchemas,
        SchemaService.method.getSchema,
        TableService.method.getTable,
        ViewService.method.getView,
        VolumeService.method.getVolume,
        FunctionService.method.getFunction,
        ModelService.method.getModelVersion,
      ],
      unrelated: [],
    },
    {
      parent: "schema",
      update: (h, newName) =>
        h.updateSchema.mutateAsync({
          schema: { catalogName: "main", name: "old" },
          newName,
        }),
      remove: (h) =>
        h.deleteSchema.mutateAsync({
          schema: { catalogName: "main", name: "old" },
          force: true,
        }),
      descendants: [
        TableService.method.listTables,
        TableService.method.getTable,
        ViewService.method.getView,
        VolumeService.method.getVolume,
        FunctionService.method.getFunction,
        ModelService.method.getRegisteredModel,
        ModelService.method.getModelVersion,
      ],
      unrelated: [CatalogService.method.getCatalog],
    },
    {
      parent: "model",
      update: (h, newName) =>
        h.updateModel.mutateAsync({ model: object, newName }),
      remove: (h) => h.deleteModel.mutateAsync({ model: object }),
      descendants: [
        ModelService.method.listModelVersions,
        ModelService.method.getModelVersion,
      ],
      unrelated: [TableService.method.getTable],
    },
  ];

  // Caches a result for every case key, runs the mutation, and returns a
  // reader for whether a key was marked stale.
  async function mutateCached(
    parent: ParentCase,
    run: (hooks: ParentHooks) => Promise<unknown>,
  ) {
    const client = makeQueryClient();
    const keys = [...parent.descendants, ...parent.unrelated].map(methodKey);
    for (const key of [...keys, grants, currentUser]) {
      client.setQueryData(key, {});
    }
    const { result } = renderHookWithProviders(useParentHooks, { client });
    await act(async () => {
      await run(result.current);
    });
    return (key: QueryKey) => client.getQueryState(key)?.isInvalidated;
  }

  it.each(
    parents.flatMap((parent) => [
      {
        ...parent,
        change: "renaming",
        run: (h: ParentHooks) => parent.update(h, "new"),
      },
      { ...parent, change: "deleting", run: parent.remove },
    ]),
  )(
    "$change a $parent marks its descendants and grants stale",
    async (parent) => {
      const isStale = await mutateCached(parent, parent.run);

      for (const method of parent.descendants) {
        expect(isStale(methodKey(method)), method.name).toBe(true);
      }
      expect(isStale(grants)).toBe(true);
      for (const method of parent.unrelated) {
        expect(isStale(methodKey(method)), method.name).toBe(false);
      }
      expect(isStale(currentUser)).toBe(false);
    },
  );

  it.each(parents)(
    "editing a $parent keeps its descendants cached",
    async (parent) => {
      const isStale = await mutateCached(parent, (h) => parent.update(h));

      for (const method of parent.descendants) {
        expect(isStale(methodKey(method)), method.name).toBe(false);
      }
      expect(isStale(grants)).toBe(false);
    },
  );

  it.each([
    {
      name: "deleteCatalog",
      type: "catalog",
      fullName: "main",
      run: (h: DetailHooks) =>
        h.deleteCatalog.mutateAsync({ catalog: { name: "main" }, force: true }),
    },
    {
      name: "deleteSchema",
      type: "schema",
      fullName: "main.default",
      run: (h: DetailHooks) =>
        h.deleteSchema.mutateAsync({
          schema: { catalogName: "main", name: "default" },
        }),
    },
    {
      name: "deleteTable",
      type: "table",
      run: (h: DetailHooks) => h.deleteTable.mutateAsync({ table: object }),
    },
    {
      name: "deleteView",
      type: "table",
      run: (h: DetailHooks) => h.deleteView.mutateAsync({ view: object }),
    },
    {
      name: "deleteVolume",
      type: "volume",
      run: (h: DetailHooks) => h.deleteVolume.mutateAsync({ volume: object }),
    },
    {
      name: "renaming a volume",
      type: "volume",
      run: (h: DetailHooks) =>
        h.updateVolume.mutateAsync({ volume: object, newName: "renamed" }),
    },
    {
      name: "deleteFunction",
      type: "function",
      run: (h: DetailHooks) =>
        h.deleteFunction.mutateAsync({ function: object }),
    },
    {
      name: "deleteModel",
      type: "registered_model",
      run: (h: DetailHooks) => h.deleteModel.mutateAsync({ model: object }),
    },
  ] satisfies {
    name: string;
    type: SecurableType;
    fullName?: string;
    run: (hooks: DetailHooks) => Promise<unknown>;
  }[])(
    "$name reloads the old name's permissions on the next visit",
    async ({ type, fullName = "main.default.obj", run }) => {
      let reads = 0;
      const handler: UcHandler = ({ path }) => {
        if (path === `/api/2.1/unity-catalog/permissions/${type}/${fullName}`) {
          reads += 1;
        }
        return { httpStatus: 200, ok: true, body: "{}" };
      };
      // One client and transport for every render, so the cache key matches.
      const opts = {
        client: makeQueryClient(),
        transport: makeTransport(handler),
      };
      const visitPermissions = async () => {
        const { result, unmount } = renderHookWithProviders(
          () => usePermissions(type, fullName),
          opts,
        );
        await waitFor(() => expect(result.current.isSuccess).toBe(true));
        unmount();
      };

      await visitPermissions();
      const { result } = renderHookWithProviders(useDetailHooks, opts);
      await act(async () => {
        await run(result.current);
      });
      await visitPermissions();

      expect(reads).toBe(2);
    },
  );
});
