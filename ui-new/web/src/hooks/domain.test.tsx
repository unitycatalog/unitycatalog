import { describe, expect, it } from "vitest";
import { waitFor } from "@testing-library/react";
import { renderHookWithProviders, type UcHandler } from "@/test/providers";
import { useGetCatalog, useListCatalogs } from "@/hooks/catalog";
import { useGetSchema, useListSchemas } from "@/hooks/schemas";
import { useGetTable, useListTables } from "@/hooks/tables";
import { useGetVolume, useListVolumes } from "@/hooks/volumes";
import { useGetFunction, useListFunctions } from "@/hooks/functions";
import { useGetModel, useGetModelVersion, useListModelVersions, useListModels } from "@/hooks/models";

// One handler that answers every UC read the domain hooks issue, keyed by path.
const handler: UcHandler = ({ path, query }) => {
  const body = (obj: unknown) => ({ httpStatus: 200, body: JSON.stringify(obj), ok: true });
  if (path === "/api/2.1/unity-catalog/catalogs") return body({ catalogs: [{ name: "main" }] });
  if (path === "/api/2.1/unity-catalog/catalogs/main") return body({ name: "main", owner: "me" });
  if (path === "/api/2.1/unity-catalog/schemas")
    return body({ schemas: [{ name: "default", catalog_name: query.find((q) => q.key === "catalog_name")?.value }] });
  if (path === "/api/2.1/unity-catalog/schemas/main.default") return body({ name: "default", catalog_name: "main" });
  if (path === "/api/2.1/unity-catalog/tables") return body({ tables: [{ name: "t1" }] });
  if (path === "/api/2.1/unity-catalog/tables/main.default.t1") return body({ name: "t1", table_type: "MANAGED" });
  if (path === "/api/2.1/unity-catalog/volumes") return body({ volumes: [{ name: "v1" }] });
  if (path === "/api/2.1/unity-catalog/volumes/main.default.v1") return body({ name: "v1" });
  if (path === "/api/2.1/unity-catalog/functions") return body({ functions: [{ name: "f1" }] });
  if (path === "/api/2.1/unity-catalog/functions/main.default.f1") return body({ name: "f1" });
  if (path === "/api/2.1/unity-catalog/models") return body({ registered_models: [{ name: "m1" }] });
  if (path === "/api/2.1/unity-catalog/models/main.default.m1") return body({ name: "m1" });
  if (path === "/api/2.1/unity-catalog/models/main.default.m1/versions")
    return body({ model_versions: [{ version: 1 }] });
  if (path === "/api/2.1/unity-catalog/models/main.default.m1/versions/1")
    return body({ model_name: "m1", version: 1 });
  return { httpStatus: 404, body: "{}", ok: false };
};

async function expectData<T>(hook: () => { data?: T; isSuccess: boolean }) {
  const { result } = renderHookWithProviders(hook, { handler });
  await waitFor(() => expect(result.current.isSuccess).toBe(true));
  return result.current.data;
}

describe("domain read hooks", () => {
  it("useListCatalogs / useGetCatalog", async () => {
    expect(await expectData(() => useListCatalogs())).toMatchObject({ catalogs: [{ name: "main" }] });
    expect(await expectData(() => useGetCatalog("main"))).toMatchObject({ name: "main" });
  });

  it("useListSchemas / useGetSchema", async () => {
    expect(await expectData(() => useListSchemas("main"))).toMatchObject({ schemas: [{ name: "default" }] });
    expect(await expectData(() => useGetSchema("main.default"))).toMatchObject({ name: "default" });
  });

  it("useListTables / useGetTable", async () => {
    expect(await expectData(() => useListTables("main", "default"))).toMatchObject({ tables: [{ name: "t1" }] });
    expect(await expectData(() => useGetTable("main.default.t1"))).toMatchObject({ table_type: "MANAGED" });
  });

  it("useListVolumes / useGetVolume", async () => {
    expect(await expectData(() => useListVolumes("main", "default"))).toMatchObject({ volumes: [{ name: "v1" }] });
    expect(await expectData(() => useGetVolume("main.default.v1"))).toMatchObject({ name: "v1" });
  });

  it("useListFunctions / useGetFunction", async () => {
    expect(await expectData(() => useListFunctions("main", "default"))).toMatchObject({ functions: [{ name: "f1" }] });
    expect(await expectData(() => useGetFunction("main.default.f1"))).toMatchObject({ name: "f1" });
  });

  it("models: list / get / versions", async () => {
    expect(await expectData(() => useListModels("main", "default"))).toMatchObject({
      registered_models: [{ name: "m1" }],
    });
    expect(await expectData(() => useGetModel("main.default.m1"))).toMatchObject({ name: "m1" });
    expect(await expectData(() => useListModelVersions("main.default.m1"))).toMatchObject({
      model_versions: [{ version: 1 }],
    });
    expect(await expectData(() => useGetModelVersion("main.default.m1", 1))).toMatchObject({ version: 1 });
  });

  it("disables list hooks when args are missing", () => {
    const { result } = renderHookWithProviders(() => useListSchemas(""), { handler });
    expect(result.current.fetchStatus).toBe("idle");
  });
});
