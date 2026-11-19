import { describe, expect, it } from "vitest";
import { waitFor } from "@testing-library/react";
import { renderHookWithProviders, type RpcHandlers } from "@/test/providers";
import { TableType } from "@/gen/uc/v1/table_pb";
import { useGetCatalog, useListCatalogs } from "@/hooks/catalog";
import { useGetSchema, useListSchemas } from "@/hooks/schemas";
import { useGetTable, useListTables } from "@/hooks/tables";
import { useGetVolume, useListVolumes } from "@/hooks/volumes";
import { useGetFunction, useListFunctions } from "@/hooks/functions";
import {
  useGetModel,
  useGetModelVersion,
  useListModelVersions,
  useListModels,
  parseModelVersion,
} from "@/hooks/models";
import { useGetView, useListViews } from "@/hooks/views";

const identity = { catalogName: "main", schemaName: "default" };
const rpc: RpcHandlers = {
  catalogs: {
    listCatalogs: () => ({ catalogs: [{ name: "main" }] }),
    getCatalog: () => ({ catalog: { name: "main", audit: { owner: "me" } } }),
  },
  schemas: {
    listSchemas: () => ({ schemas: [{ ...identity, name: "default" }] }),
    getSchema: () => ({ schema: { ...identity, name: "default" } }),
  },
  tables: {
    listTables: () => ({
      tables: [{ ...identity, name: "t1", tableType: TableType.MANAGED }],
    }),
    getTable: () => ({
      table: { ...identity, name: "t1", tableType: TableType.MANAGED },
    }),
  },
  views: {
    listViews: () => ({ views: [{ ...identity, name: "mv1" }] }),
    getView: () => ({
      view: { ...identity, name: "mv1", viewDefinition: "version: 1.1" },
    }),
  },
  volumes: {
    listVolumes: () => ({ volumes: [{ ...identity, name: "v1" }] }),
    getVolume: () => ({ volume: { ...identity, name: "v1" } }),
  },
  functions: {
    listFunctions: () => ({ functions: [{ ...identity, name: "f1" }] }),
    getFunction: () => ({ function: { ...identity, name: "f1" } }),
  },
  models: {
    listRegisteredModels: () => ({ models: [{ ...identity, name: "m1" }] }),
    getRegisteredModel: () => ({ model: { ...identity, name: "m1" } }),
    listModelVersions: () => ({
      versions: [{ ...identity, modelName: "m1", version: 1n }],
    }),
    getModelVersion: () => ({
      modelVersion: { ...identity, modelName: "m1", version: 1n },
    }),
  },
};

async function expectData<T>(hook: () => { data?: T; isSuccess: boolean }) {
  const { result } = renderHookWithProviders(hook, { rpc });
  await waitFor(() => expect(result.current.isSuccess).toBe(true));
  return result.current.data;
}

describe("parseModelVersion", () => {
  it.each([
    ["1", 1n],
    ["42", 42n],
    [7, 7n],
    ["0", undefined],
    ["02", undefined],
    ["0x2", undefined],
    [" 2", undefined],
    ["+2", undefined],
    ["-1", undefined],
    ["1.5", undefined],
    ["", undefined],
  ])("parses %j as %s", (version, expected) => {
    expect(parseModelVersion(version)).toBe(expected);
  });
});

describe("domain read hooks", () => {
  it("useListCatalogs / useGetCatalog", async () => {
    expect(await expectData(() => useListCatalogs())).toMatchObject({
      catalogs: [{ name: "main" }],
    });
    expect(await expectData(() => useGetCatalog("main"))).toMatchObject({
      name: "main",
    });
  });

  it("follows list page tokens and combines every page", async () => {
    const pagedRpc: RpcHandlers = {
      catalogs: {
        listCatalogs: ({ page }) =>
          page?.pageToken === "next"
            ? { catalogs: [{ name: "second" }] }
            : {
                catalogs: [{ name: "first" }],
                page: { nextPageToken: "next" },
              },
      },
    };
    const { result } = renderHookWithProviders(() => useListCatalogs(), {
      rpc: pagedRpc,
    });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data?.catalogs.map(({ name }) => name)).toEqual([
      "first",
      "second",
    ]);
  });

  it("rejects pagination token cycles", async () => {
    const cyclicRpc: RpcHandlers = {
      catalogs: {
        listCatalogs: ({ page }) => {
          const token = page?.pageToken ?? "";
          return {
            catalogs: [{ name: token || "first" }],
            page: { nextPageToken: token === "A" ? "B" : "A" },
          };
        },
      },
    };
    const { result } = renderHookWithProviders(() => useListCatalogs(), {
      rpc: cyclicRpc,
    });

    await waitFor(() => expect(result.current.isError).toBe(true));
    expect(result.current.error).toMatchObject({
      message: "Unity Catalog returned a repeated page token",
    });
  });

  it("useListSchemas / useGetSchema", async () => {
    expect(await expectData(() => useListSchemas("main"))).toMatchObject({
      schemas: [{ name: "default" }],
    });
    expect(await expectData(() => useGetSchema("main.default"))).toMatchObject({
      name: "default",
    });
  });

  it("useListTables / useGetTable", async () => {
    expect(
      await expectData(() => useListTables("main", "default")),
    ).toMatchObject({ tables: [{ name: "t1" }] });
    expect(
      await expectData(() => useGetTable("main.default.t1")),
    ).toMatchObject({ tableType: 1 });
  });

  it("useListVolumes / useGetVolume", async () => {
    expect(
      await expectData(() => useListVolumes("main", "default")),
    ).toMatchObject({ volumes: [{ name: "v1" }] });
    expect(
      await expectData(() => useGetVolume("main.default.v1")),
    ).toMatchObject({ name: "v1" });
  });

  it("useListFunctions / useGetFunction", async () => {
    expect(
      await expectData(() => useListFunctions("main", "default")),
    ).toMatchObject({ functions: [{ name: "f1" }] });
    expect(
      await expectData(() => useGetFunction("main.default.f1")),
    ).toMatchObject({ name: "f1" });
  });

  it("metric views: list / get", async () => {
    expect(
      await expectData(() => useListViews("main", "default")),
    ).toMatchObject({ views: [{ name: "mv1" }] });
    expect(
      await expectData(() => useGetView("main.default.mv1")),
    ).toMatchObject({ name: "mv1", viewDefinition: "version: 1.1" });
  });

  it("models: list / get / versions", async () => {
    expect(
      await expectData(() => useListModels("main", "default")),
    ).toMatchObject({
      models: [{ name: "m1" }],
    });
    expect(
      await expectData(() => useGetModel("main.default.m1")),
    ).toMatchObject({ name: "m1" });
    expect(
      await expectData(() => useListModelVersions("main.default.m1")),
    ).toMatchObject({
      versions: [{ version: 1n }],
    });
    expect(
      await expectData(() => useGetModelVersion("main.default.m1", 1)),
    ).toMatchObject({ version: 1n });
  });

  it("disables list hooks when args are missing", () => {
    const { result } = renderHookWithProviders(() => useListSchemas(""), {
      rpc,
    });
    expect(result.current.fetchStatus).toBe("idle");
  });
});
