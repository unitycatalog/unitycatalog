import { describe, expect, it } from "vitest";
import { act } from "@testing-library/react";
import { createConnectQueryKey } from "@connectrpc/connect-query";
import { CatalogService } from "@/gen/uc/v1/catalog_pb";
import { ColumnTypeName } from "@/gen/uc/v1/common_pb";
import { DataSourceFormat } from "@/gen/uc/v1/table_pb";
import { VolumeType } from "@/gen/uc/v1/volume_pb";
import {
  useCreateCatalog,
  useDeleteCatalog,
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
import { makeQueryClient, renderHookWithProviders } from "@/test/providers";

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

  it("does not invalidate an obsolete detail query after rename", async () => {
    const client = makeQueryClient();
    const listKey = createConnectQueryKey({
      schema: CatalogService.method.listCatalogs,
      input: {},
      cardinality: "finite",
    });
    const detailKey = createConnectQueryKey({
      schema: CatalogService.method.getCatalog,
      input: { catalog: { name: "old" } },
      cardinality: "finite",
    });
    client.setQueryData(listKey, { catalogs: [] });
    client.setQueryData(detailKey, { catalog: { name: "old" } });
    const { result } = renderHookWithProviders(() => useUpdateCatalog(), {
      client,
    });

    await act(async () => {
      await result.current.mutateAsync({
        catalog: { name: "old" },
        newName: "new",
      });
    });

    expect(client.getQueryState(listKey)?.isInvalidated).toBe(true);
    expect(client.getQueryState(detailKey)?.isInvalidated).toBe(false);
  });
});
