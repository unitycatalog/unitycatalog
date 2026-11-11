import { describe, expect, it, vi, beforeEach, afterEach, type Mock } from "vitest";
import type { ReactNode } from "react";
import { act, renderHook, waitFor } from "@testing-library/react";

vi.mock("@/lib/transport", () => ({ proxyClient: { call: vi.fn() } }));

import { proxyClient } from "@/lib/transport";
import { Providers } from "@/test/providers";
import { AuthProvider, useAuth } from "@/context/auth-context";
import { getToken } from "@/lib/session";
import { isOktaToken } from "@/lib/okta";

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

  describe("signInWithOkta", () => {
    const tokens = { accessToken: "okta-at", idToken: "okta-id" };

    // /scim2/Me needs a bearer token or the session cookie the exchange sets.
    function stubUc(exchange: { httpStatus: number; body: string }) {
      stubConfig({ authEnabled: true });
      call.mockImplementation(async (input: { path: string; token: string }) => {
        if (input.path.endsWith("/auth/tokens")) {
          return { ...exchange, ok: exchange.httpStatus < 300 };
        }
        return { httpStatus: 200, ok: true, body: JSON.stringify({ displayName: "Ada" }) };
      });
    }

    it("internal mode: exchanges the ID token for the UC session cookie and keeps no bearer token", async () => {
      stubUc({ httpStatus: 200, body: "{}" });
      const { result } = renderHook(() => useAuth(), { wrapper });
      await waitFor(() => expect(result.current.loading).toBe(false));

      await act(async () => {
        await result.current.signInWithOkta(tokens);
      });

      const exchange = call.mock.calls.map((c) => c[0]).find((c) => c.path.endsWith("/auth/tokens"));
      expect(exchange.query).toEqual([{ key: "ext", value: "cookie" }]);
      expect(new URLSearchParams(exchange.jsonBody).get("subject_token")).toBe("okta-id");
      expect(getToken()).toBe("");
      expect(isOktaToken()).toBe(false);
    });

    it("external mode: a 501 UNIMPLEMENTED from the exchange keeps the Okta access token as the bearer", async () => {
      stubUc({
        httpStatus: 501,
        body: JSON.stringify({
          error_code: "UNIMPLEMENTED",
          message: "Token exchange is disabled in external mode",
        }),
      });
      const { result } = renderHook(() => useAuth(), { wrapper });
      await waitFor(() => expect(result.current.loading).toBe(false));

      await act(async () => {
        await result.current.signInWithOkta(tokens);
      });

      expect(getToken()).toBe("okta-at");
      expect(isOktaToken()).toBe(true);
      await waitFor(() => expect(result.current.currentUser?.displayName).toBe("Ada"));
    });

    it.each([
      ["a 501 with another error_code", 501, { error_code: "INTERNAL", message: "x" }],
      ["a 403 from the exchange", 403, { error_code: "PERMISSION_DENIED", message: "User not allowed" }],
      ["a 501 with a body that is not JSON", 501, "<html>Not Implemented</html>"],
    ])("fails and keeps no token on %s", async (_name, httpStatus, body) => {
      stubUc({ httpStatus, body: typeof body === "string" ? body : JSON.stringify(body) });
      const { result } = renderHook(() => useAuth(), { wrapper });
      await waitFor(() => expect(result.current.loading).toBe(false));

      await expect(result.current.signInWithOkta(tokens)).rejects.toThrow(`Login failed (HTTP ${httpStatus})`);
      expect(getToken()).toBe("");
      expect(isOktaToken()).toBe(false);
    });

    it("logout forgets that the token came from Okta", async () => {
      stubUc({ httpStatus: 501, body: JSON.stringify({ error_code: "UNIMPLEMENTED" }) });
      const { result } = renderHook(() => useAuth(), { wrapper });
      await waitFor(() => expect(result.current.loading).toBe(false));
      await act(async () => {
        await result.current.signInWithOkta(tokens);
      });
      await act(async () => {
        await result.current.logout();
      });
      expect(getToken()).toBe("");
      expect(isOktaToken()).toBe(false);
    });
  });

  it("useAuth throws outside a provider", () => {
    expect(() => renderHook(() => useAuth())).toThrow(/AuthProvider/);
  });
});
