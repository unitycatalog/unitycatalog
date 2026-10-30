import { describe, expect, it, vi, beforeEach, type Mock } from "vitest";
import { waitFor } from "@testing-library/react";

vi.mock("@/lib/transport", () => ({
  proxyClient: { call: vi.fn() },
}));

import { proxyClient } from "@/lib/transport";
import { renderHookWithProviders } from "@/test/providers";
import { useGetCurrentUser, useLoginWithToken, useLogoutCurrentUser } from "@/hooks/user";

const call = proxyClient.call as unknown as Mock;

beforeEach(() => call.mockReset());

describe("useGetCurrentUser", () => {
  it("returns the SCIM user on 200", async () => {
    call.mockResolvedValue({
      httpStatus: 200,
      ok: true,
      body: JSON.stringify({ displayName: "Ada", emails: [{ value: "ada@x.io" }] }),
    });
    const { result } = renderHookWithProviders(() => useGetCurrentUser(true));
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data?.displayName).toBe("Ada");
    expect(call.mock.calls[0][0].path).toBe("/api/1.0/unity-control/scim2/Me");
  });

  it("resolves to null on 401 (unauthenticated)", async () => {
    call.mockResolvedValue({ httpStatus: 401, ok: false, body: "" });
    const { result } = renderHookWithProviders(() => useGetCurrentUser(true));
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data).toBeNull();
  });

  it("is disabled when not enabled", () => {
    const { result } = renderHookWithProviders(() => useGetCurrentUser(false));
    expect(result.current.fetchStatus).toBe("idle");
    expect(call).not.toHaveBeenCalled();
  });
});

describe("useLoginWithToken", () => {
  it("posts a urlencoded token-exchange with ext=cookie", async () => {
    call.mockResolvedValue({ httpStatus: 200, ok: true, body: "{}" });
    const { result } = renderHookWithProviders(() => useLoginWithToken());
    await result.current.mutateAsync("google-id-token");

    const arg = call.mock.calls[0][0];
    expect(arg.method).toBe("POST");
    expect(arg.path).toBe("/api/1.0/unity-control/auth/tokens");
    expect(arg.contentType).toBe("application/x-www-form-urlencoded");
    expect(arg.query).toEqual([{ key: "ext", value: "cookie" }]);
    expect(arg.jsonBody).toContain("subject_token=google-id-token");
    expect(arg.jsonBody).toContain("grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Atoken-exchange");
  });

  it("rejects on a non-2xx response", async () => {
    call.mockResolvedValue({ httpStatus: 400, ok: false, body: "bad" });
    const { result } = renderHookWithProviders(() => useLoginWithToken());
    await expect(result.current.mutateAsync("x")).rejects.toThrow(/Login failed/);
  });
});

describe("useLogoutCurrentUser", () => {
  it("posts to /auth/logout", async () => {
    call.mockResolvedValue({ httpStatus: 200, ok: true, body: "" });
    const { result } = renderHookWithProviders(() => useLogoutCurrentUser());
    await result.current.mutateAsync();
    expect(call.mock.calls[0][0].path).toBe("/api/1.0/unity-control/auth/logout");
  });
});
