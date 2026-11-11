import { describe, expect, it, vi, beforeEach, afterEach, type Mock } from "vitest";
import type { ReactNode } from "react";
import { act, renderHook, waitFor } from "@testing-library/react";

vi.mock("@/lib/transport", () => ({ proxyClient: { call: vi.fn() } }));

import { proxyClient } from "@/lib/transport";
import { Providers, makeQueryClient } from "@/test/providers";
import { CURRENT_USER_QUERY_KEY } from "@/hooks/user";
import { AuthProvider, useAuth } from "@/context/auth-context";
import { getToken, setToken } from "@/lib/session";
import { isOktaToken, markOktaToken } from "@/lib/okta";

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

    // /scim2/Me needs a bearer token or the session cookie the exchange sets, and answers slowly.
    function stubUc(exchange: { httpStatus: number; body: string }) {
      stubConfig({ authEnabled: true });
      let cookie = false;
      call.mockImplementation(async (input: { path: string; token: string }) => {
        if (input.path.endsWith("/auth/tokens")) {
          cookie = exchange.httpStatus === 200;
          return { ...exchange, ok: exchange.httpStatus < 300 };
        }
        if (!input.path.endsWith("/scim2/Me")) return { httpStatus: 200, ok: true, body: "{}" };
        await new Promise((r) => setTimeout(r, 20));
        return input.token || cookie
          ? { httpStatus: 200, ok: true, body: JSON.stringify({ displayName: "Ada" }) }
          : { httpStatus: 401, ok: false, body: "" };
      });
    }

    it.each([
      ["internal", { httpStatus: 200, body: "{}" }],
      ["external", { httpStatus: 501, body: JSON.stringify({ error_code: "UNIMPLEMENTED" }) }],
    ])("%s mode: resolves only after /scim2/Me has returned the signed-in user", async (_mode, exchange) => {
      stubUc(exchange);
      const client = makeQueryClient();
      const { result } = renderHook(() => useAuth(), {
        wrapper: ({ children }) => (
          <Providers client={client}>
            <AuthProvider>{children}</AuthProvider>
          </Providers>
        ),
      });
      await waitFor(() => expect(result.current.loading).toBe(false));

      await act(async () => {
        await result.current.signInWithOkta(tokens);
      });

      expect(client.getQueryData(CURRENT_USER_QUERY_KEY)).toEqual({ displayName: "Ada" });
    });

    it("fails with the fetch error when /scim2/Me answers a 5xx after the sign-in", async () => {
      stubConfig({ authEnabled: true });
      let exchanged = false;
      call.mockImplementation(async (input: { path: string }) => {
        if (input.path.endsWith("/auth/tokens")) exchanged = true;
        if (!input.path.endsWith("/scim2/Me")) return { httpStatus: 200, ok: true, body: "{}" };
        return exchanged ? { httpStatus: 503, ok: false, body: "" } : { httpStatus: 401, ok: false, body: "" };
      });
      const { result } = renderHook(() => useAuth(), { wrapper });
      await waitFor(() => expect(result.current.loading).toBe(false));

      await expect(result.current.signInWithOkta(tokens)).rejects.toThrow("Failed to fetch user (HTTP 503)");
    });

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

    it.each([
      ["internal", { httpStatus: 200, body: "{}" }],
      ["external", { httpStatus: 501, body: JSON.stringify({ error_code: "UNIMPLEMENTED" }) }],
    ])(
      "%s mode: ignores the page-load /scim2/Me that is still in flight when the sign-in finishes",
      async (_mode, exchange) => {
        stubConfig({ authEnabled: true });
        let cookie = false;
        let first = true;
        let meCalls = 0;
        call.mockImplementation(async (input: { path: string; token: string }) => {
          if (input.path.endsWith("/auth/tokens")) {
            cookie = exchange.httpStatus === 200;
            return { ...exchange, ok: exchange.httpStatus < 300 };
          }
          if (!input.path.endsWith("/scim2/Me")) return { httpStatus: 200, ok: true, body: "{}" };
          meCalls++;
          // The credentials as they were when the request left, like the real server.
          const signedIn = input.token || cookie;
          if (first) {
            first = false;
            await new Promise((r) => setTimeout(r, 100));
          }
          return signedIn
            ? { httpStatus: 200, ok: true, body: JSON.stringify({ displayName: "Ada" }) }
            : { httpStatus: 401, ok: false, body: "" };
        });
        const client = makeQueryClient();
        const { result } = renderHook(() => useAuth(), {
          wrapper: ({ children }) => (
            <Providers client={client}>
              <AuthProvider>{children}</AuthProvider>
            </Providers>
          ),
        });
        await waitFor(() => expect(meCalls).toBe(1));

        await act(async () => {
          await result.current.signInWithOkta(tokens);
        });

        expect(client.getQueryData(CURRENT_USER_QUERY_KEY)).toEqual({ displayName: "Ada" });
        expect(meCalls).toBe(2);
        expect(getToken()).toBe(exchange.httpStatus === 200 ? "" : "okta-at");
      },
    );

    it("internal mode: fails when /scim2/Me does not know the user after a successful exchange", async () => {
      stubConfig({ authEnabled: true });
      call.mockImplementation(async (input: { path: string }) =>
        input.path.endsWith("/scim2/Me")
          ? { httpStatus: 401, ok: false, body: "" }
          : { httpStatus: 200, ok: true, body: "{}" },
      );
      const { result } = renderHook(() => useAuth(), { wrapper });
      await waitFor(() => expect(result.current.loading).toBe(false));

      await expect(result.current.signInWithOkta(tokens)).rejects.toThrow("Login failed. Contact your administrator.");
    });

    it("external mode: a 401 from /scim2/Me is not an error, so the login page can restart Okta", async () => {
      stubConfig({ authEnabled: true });
      call.mockImplementation(async (input: { path: string }) => {
        if (input.path.endsWith("/auth/tokens"))
          return { httpStatus: 501, ok: false, body: JSON.stringify({ error_code: "UNIMPLEMENTED" }) };
        if (input.path.endsWith("/scim2/Me")) return { httpStatus: 401, ok: false, body: "" };
        return { httpStatus: 200, ok: true, body: "{}" };
      });
      const { result } = renderHook(() => useAuth(), { wrapper });
      await waitFor(() => expect(result.current.loading).toBe(false));

      await act(async () => {
        await result.current.signInWithOkta(tokens);
      });

      expect(getToken()).toBe("okta-at");
      expect(isOktaToken()).toBe(true);
    });

    it("internal mode: drops a stale bearer token so it cannot outrank the new session cookie", async () => {
      stubConfig({ authEnabled: true });
      let cookie = false;
      call.mockImplementation(async (input: { path: string; token: string }) => {
        if (input.path.endsWith("/auth/tokens")) {
          cookie = true;
          return { httpStatus: 200, ok: true, body: "{}" };
        }
        if (!input.path.endsWith("/scim2/Me")) return { httpStatus: 200, ok: true, body: "{}" };
        // Like AuthDecorator: a bearer token is checked before the cookie.
        if (input.token) return { httpStatus: 401, ok: false, body: "" };
        return cookie
          ? { httpStatus: 200, ok: true, body: JSON.stringify({ displayName: "Ada" }) }
          : { httpStatus: 401, ok: false, body: "" };
      });
      setToken("stale-pasted");
      markOktaToken(true);
      const { result } = renderHook(() => useAuth(), { wrapper });
      await waitFor(() => expect(result.current.loading).toBe(false));

      await act(async () => {
        await result.current.signInWithOkta(tokens);
      });

      expect(getToken()).toBe("");
      expect(isOktaToken()).toBe(false);
      await waitFor(() => expect(result.current.currentUser?.displayName).toBe("Ada"));
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
