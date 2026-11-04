import { describe, expect, it, vi, beforeEach, afterEach, type Mock } from "vitest";
import type { ReactNode } from "react";
import { act, renderHook, waitFor } from "@testing-library/react";

vi.mock("@/lib/transport", () => ({ proxyClient: { call: vi.fn() } }));

import { proxyClient } from "@/lib/transport";
import { Providers } from "@/test/providers";
import { AuthProvider, useAuth } from "@/context/auth-context";
import { getToken } from "@/lib/session";

const call = proxyClient.call as unknown as Mock;

function stubConfig(cfg: Record<string, unknown>) {
  vi.stubGlobal("fetch", vi.fn().mockResolvedValue({ ok: true, json: async () => cfg }));
}

const wrapper = ({ children }: { children: ReactNode }) => (
  <Providers>
    <AuthProvider>{children}</AuthProvider>
  </Providers>
);

beforeEach(() => call.mockReset());
afterEach(() => vi.restoreAllMocks());

describe("AuthProvider", () => {
  it("auth-disabled: no SCIM call, no current user", async () => {
    stubConfig({ authEnabled: false });
    const { result } = renderHook(() => useAuth(), { wrapper });
    await waitFor(() => expect(result.current.loading).toBe(false));
    expect(result.current.authEnabled).toBe(false);
    expect(result.current.currentUser).toBeNull();
    expect(call).not.toHaveBeenCalled();
  });

  it("auth-enabled: resolves the current user via SCIM", async () => {
    stubConfig({ authEnabled: true });
    call.mockResolvedValue({ httpStatus: 200, ok: true, body: JSON.stringify({ displayName: "Ada" }) });
    const { result } = renderHook(() => useAuth(), { wrapper });
    await waitFor(() => expect(result.current.currentUser?.displayName).toBe("Ada"));
  });

  it("signInWithAccessToken stores the token and authenticates via bearer", async () => {
    stubConfig({ authEnabled: true });
    // A user is only returned when a bearer token is forwarded.
    call.mockImplementation(async (input: { token: string }) =>
      input.token
        ? { httpStatus: 200, ok: true, body: JSON.stringify({ displayName: "Ada" }) }
        : { httpStatus: 401, ok: false, body: "" },
    );

    const { result } = renderHook(() => useAuth(), { wrapper });
    await waitFor(() => expect(result.current.loading).toBe(false));
    expect(result.current.currentUser).toBeNull();

    act(() => result.current.signInWithAccessToken("jwt-xyz"));
    expect(getToken()).toBe("jwt-xyz");
    await waitFor(() => expect(result.current.hasAccessToken).toBe(true));
    await waitFor(() => expect(result.current.currentUser?.displayName).toBe("Ada"));
  });

  it("logout clears the pasted token", async () => {
    stubConfig({ authEnabled: true });
    call.mockResolvedValue({ httpStatus: 200, ok: true, body: "{}" });
    const { result } = renderHook(() => useAuth(), { wrapper });
    await waitFor(() => expect(result.current.loading).toBe(false));

    act(() => result.current.signInWithAccessToken("jwt-xyz"));
    await waitFor(() => expect(getToken()).toBe("jwt-xyz"));

    await act(async () => {
      await result.current.logout();
    });
    expect(getToken()).toBe("");
  });

  it("useAuth throws outside a provider", () => {
    expect(() => renderHook(() => useAuth())).toThrow(/AuthProvider/);
  });
});
