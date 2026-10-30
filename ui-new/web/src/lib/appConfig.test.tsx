import { describe, expect, it, vi, afterEach } from "vitest";
import { waitFor } from "@testing-library/react";
import { useAppConfig } from "@/lib/appConfig";
import { renderHookWithProviders } from "@/test/providers";

afterEach(() => {
  vi.restoreAllMocks();
});

describe("useAppConfig", () => {
  it("merges the /config response over defaults", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue({
        ok: true,
        json: async () => ({ authEnabled: true, googleClientId: "gid" }),
      }),
    );
    const { result } = renderHookWithProviders(() => useAppConfig());
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data).toEqual({
      authEnabled: true,
      googleClientId: "gid",
      oktaEnabled: false,
      keycloakEnabled: false,
    });
  });

  it("returns auth-disabled defaults when /config is not ok", async () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue({ ok: false }));
    const { result } = renderHookWithProviders(() => useAppConfig());
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data?.authEnabled).toBe(false);
  });

  it("returns defaults when fetch throws", async () => {
    vi.stubGlobal("fetch", vi.fn().mockRejectedValue(new Error("network")));
    const { result } = renderHookWithProviders(() => useAppConfig());
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data?.authEnabled).toBe(false);
  });
});
