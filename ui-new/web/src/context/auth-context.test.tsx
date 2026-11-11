import {
  describe,
  expect,
  it,
  vi,
  beforeEach,
  afterEach,
  type Mock,
} from "vitest";
import type { ReactNode } from "react";
import { act, renderHook, waitFor } from "@testing-library/react";

vi.mock("@/lib/transport", () => ({ proxyClient: { call: vi.fn() } }));

import { proxyClient } from "@/lib/transport";
import { makeQueryClient, Providers } from "@/test/providers";
import { AuthProvider, useAuth } from "@/context/auth-context";
import { getToken, setToken } from "@/lib/session";

const call = proxyClient.call as unknown as Mock;

function stubConfig(cfg: Record<string, unknown>) {
  vi.stubGlobal(
    "fetch",
    vi.fn().mockResolvedValue({ ok: true, json: async () => cfg }),
  );
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
    call.mockResolvedValue({
      httpStatus: 200,
      ok: true,
      body: JSON.stringify({ displayName: "Ada" }),
    });
    const { result } = renderHook(() => useAuth(), { wrapper });
    await waitFor(() =>
      expect(result.current.currentUser?.displayName).toBe("Ada"),
    );
  });

  it("shows authentication progress until the new session is verified", async () => {
    stubConfig({ authEnabled: true });
    let tokenExchanged = false;
    let resolveCurrentUser!: (value: {
      httpStatus: number;
      ok: boolean;
      body: string;
    }) => void;
    const currentUserResponse = new Promise<{
      httpStatus: number;
      ok: boolean;
      body: string;
    }>((resolve) => {
      resolveCurrentUser = resolve;
    });

    call.mockImplementation(async (input: { path: string }) => {
      if (input.path.endsWith("/auth/tokens")) {
        tokenExchanged = true;
        return { httpStatus: 200, ok: true, body: "{}" };
      }
      if (!tokenExchanged) {
        return { httpStatus: 401, ok: false, body: "" };
      }
      return currentUserResponse;
    });

    const { result } = renderHook(() => useAuth(), { wrapper });
    await waitFor(() => expect(result.current.loading).toBe(false));

    let loginPromise!: Promise<void>;
    act(() => {
      loginPromise = result.current.loginWithToken("google-id-token");
    });
    await waitFor(() => expect(result.current.authenticating).toBe(true));

    await act(async () => {
      resolveCurrentUser({
        httpStatus: 200,
        ok: true,
        body: JSON.stringify({ displayName: "Ada" }),
      });
      await loginPromise;
    });

    expect(result.current.authenticating).toBe(false);
    expect(result.current.currentUser?.displayName).toBe("Ada");
  });

  it("signInWithAccessToken stores the token and authenticates via bearer", async () => {
    stubConfig({ authEnabled: true });
    // A user is only returned when a bearer token is forwarded.
    call.mockImplementation(async (input: { token: string }) =>
      input.token
        ? {
            httpStatus: 200,
            ok: true,
            body: JSON.stringify({ displayName: "Ada" }),
          }
        : { httpStatus: 401, ok: false, body: "" },
    );

    const { result } = renderHook(() => useAuth(), { wrapper });
    await waitFor(() => expect(result.current.loading).toBe(false));
    expect(result.current.currentUser).toBeNull();

    await act(async () => {
      await result.current.signInWithAccessToken("jwt-xyz");
    });
    expect(getToken()).toBe("jwt-xyz");
    await waitFor(() => expect(result.current.hasAccessToken).toBe(true));
    await waitFor(() =>
      expect(result.current.currentUser?.displayName).toBe("Ada"),
    );
  });

  it("clears a pasted token when current-user validation fails", async () => {
    stubConfig({ authEnabled: true });
    call.mockResolvedValue({ httpStatus: 401, ok: false, body: "" });

    const { result } = renderHook(() => useAuth(), { wrapper });
    await waitFor(() => expect(result.current.loading).toBe(false));

    await act(async () => {
      await expect(
        result.current.signInWithAccessToken("bad-jwt"),
      ).rejects.toThrow("rejected this access token");
    });
    expect(getToken()).toBe("");
  });

  it("replaces a leftover pasted token when signing in with a provider", async () => {
    stubConfig({ authEnabled: true });
    // UC checks a Bearer token before the session cookie, so the current user
    // resolves only once the stale token is no longer sent.
    call.mockImplementation(async (input: { path: string }) => {
      if (input.path.endsWith("/auth/tokens")) {
        return { httpStatus: 200, ok: true, body: "{}" };
      }
      return getToken()
        ? { httpStatus: 401, ok: false, body: "" }
        : {
            httpStatus: 200,
            ok: true,
            body: JSON.stringify({ displayName: "Ada" }),
          };
    });
    setToken("expired-jwt");

    const { result } = renderHook(() => useAuth(), { wrapper });
    await waitFor(() => expect(result.current.loading).toBe(false));

    await act(async () => {
      await result.current.loginWithToken("google-id-token");
    });
    expect(getToken()).toBe("");
    expect(result.current.currentUser?.displayName).toBe("Ada");
  });

  it("rejects a provider login whose session cannot be verified", async () => {
    stubConfig({ authEnabled: true });
    call.mockImplementation(async (input: { path: string }) =>
      input.path.endsWith("/auth/tokens")
        ? { httpStatus: 200, ok: true, body: "{}" }
        : { httpStatus: 401, ok: false, body: "" },
    );

    const { result } = renderHook(() => useAuth(), { wrapper });
    await waitFor(() => expect(result.current.loading).toBe(false));

    await act(async () => {
      await expect(
        result.current.loginWithToken("google-id-token"),
      ).rejects.toThrow(
        "Login succeeded, but the session could not be verified.",
      );
    });
    expect(result.current.authenticating).toBe(false);
    expect(result.current.currentUser).toBeNull();
  });

  it("logout clears local state and rejects when UC cannot end the session", async () => {
    stubConfig({ authEnabled: true });
    call.mockImplementation(async (input: { path: string }) =>
      input.path.endsWith("/auth/logout")
        ? { httpStatus: 500, ok: false, body: "{}" }
        : {
            httpStatus: 200,
            ok: true,
            body: JSON.stringify({ displayName: "Ada" }),
          },
    );
    setToken("jwt-xyz");
    const client = makeQueryClient();
    client.setQueryData(["private-catalog-data"], { name: "secret" });
    const clientWrapper = ({ children }: { children: ReactNode }) => (
      <Providers client={client}>
        <AuthProvider>{children}</AuthProvider>
      </Providers>
    );
    const { result } = renderHook(() => useAuth(), { wrapper: clientWrapper });
    await waitFor(() => expect(result.current.loading).toBe(false));

    await act(async () => {
      await expect(result.current.logout()).rejects.toThrow("HTTP 500");
    });
    expect(getToken()).toBe("");
    expect(client.getQueryData(["private-catalog-data"])).toBeUndefined();
  });

  it("logout clears the pasted token", async () => {
    stubConfig({ authEnabled: true });
    call.mockResolvedValue({ httpStatus: 200, ok: true, body: "{}" });
    const { result } = renderHook(() => useAuth(), { wrapper });
    await waitFor(() => expect(result.current.loading).toBe(false));

    await act(async () => {
      await result.current.signInWithAccessToken("jwt-xyz");
    });
    await waitFor(() => expect(getToken()).toBe("jwt-xyz"));

    await act(async () => {
      await result.current.logout();
    });
    expect(getToken()).toBe("");
  });

  it("clears cached catalog data when the identity changes", async () => {
    stubConfig({ authEnabled: true });
    call.mockImplementation(async () =>
      getToken()
        ? {
            httpStatus: 200,
            ok: true,
            body: JSON.stringify({ displayName: "Ada" }),
          }
        : { httpStatus: 401, ok: false, body: "" },
    );
    const client = makeQueryClient();
    client.setQueryData(["private-catalog-data"], { name: "secret" });
    const identityWrapper = ({ children }: { children: ReactNode }) => (
      <Providers client={client}>
        <AuthProvider>{children}</AuthProvider>
      </Providers>
    );
    const { result } = renderHook(() => useAuth(), {
      wrapper: identityWrapper,
    });
    await waitFor(() => expect(result.current.loading).toBe(false));

    await act(async () => {
      await result.current.signInWithAccessToken("jwt-xyz");
    });

    expect(client.getQueryData(["private-catalog-data"])).toBeUndefined();
  });

  it("useAuth throws outside a provider", () => {
    expect(() => renderHook(() => useAuth())).toThrow(/AuthProvider/);
  });
});
