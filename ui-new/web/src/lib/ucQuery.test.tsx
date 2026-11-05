import { describe, expect, it } from "vitest";
import { waitFor } from "@testing-library/react";
import { useUcQuery } from "@/lib/ucQuery";
import { renderHookWithProviders } from "@/test/providers";

describe("useUcQuery", () => {
  it("parses a 2xx JSON body", async () => {
    const { result } = renderHookWithProviders(
      () => useUcQuery<{ catalogs: { name: string }[] }>("GET", "/api/2.1/unity-catalog/catalogs"),
      { handler: () => ({ httpStatus: 200, body: '{"catalogs":[{"name":"main"}]}', ok: true }) },
    );
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data?.catalogs[0].name).toBe("main");
  });

  it("surfaces non-2xx UC responses as a UCError", async () => {
    const { result } = renderHookWithProviders(
      () => useUcQuery("GET", "/api/2.1/unity-catalog/catalogs/missing"),
      { handler: () => ({ httpStatus: 404, body: "not found", ok: false }) },
    );
    await waitFor(() => expect(result.current.isError).toBe(true));
    expect(result.current.error).toMatchObject({ name: "UCError", status: 404 });
  });

  it("respects the enabled option", async () => {
    const { result } = renderHookWithProviders(
      () => useUcQuery("GET", "/x", { queryOptions: { enabled: false } }),
    );
    // Disabled queries never leave the pending/idle state.
    expect(result.current.fetchStatus).toBe("idle");
  });
});
