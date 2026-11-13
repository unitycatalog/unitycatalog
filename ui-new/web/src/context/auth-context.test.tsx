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
import { CURRENT_USER_QUERY_KEY } from "@/hooks/user";

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

type Reply = { httpStatus: number; ok: boolean; body: string };
const OK: Reply = { httpStatus: 200, ok: true, body: "{}" };
const UNAUTHORIZED: Reply = { httpStatus: 401, ok: false, body: "" };
const ADA: Reply = {
  httpStatus: 200,
  ok: true,
  body: JSON.stringify({ displayName: "Ada" }),
};

// A UC reply the test releases later, to order overlapping requests.
function deferred() {
  let resolve!: (reply: Reply) => void;
  const promise = new Promise<Reply>((done) => (resolve = done));
  return { promise, resolve };
}

// A UC with one session cookie, applied like a browser applies Set-Cookie: when
// the reply arrives. A provider exchange signs in as its id token's name, logout
// clears the cookie, and /scim2/Me answers for the cookie the request carried.
// `hold` delays the next reply to a request key until the test resolves it.
function fakeCookieUc() {
  const held = new Map<string, ReturnType<typeof deferred>>();
  const uc = {
    cookie: "",
    requests: [] as string[],
    hold(key: string) {
      const reply = deferred();
      held.set(key, reply);
      return reply;
    },
  };
  call.mockImplementation(async (input: { path: string; jsonBody: string }) => {
    const key = input.path.endsWith("/auth/tokens")
      ? `exchange:${new URLSearchParams(input.jsonBody).get("subject_token")}`
      : input.path.endsWith("/auth/logout")
        ? "logout"
        : "me";
    uc.requests.push(key);
    const sentWith = uc.cookie;
    const pending = held.get(key);
    held.delete(key);
    const reply = pending ? await pending.promise : OK;
    if (!reply.ok) return reply;
    if (key === "me") {
      return sentWith
        ? { ...OK, body: JSON.stringify({ displayName: sentWith }) }
        : UNAUTHORIZED;
    }
    uc.cookie = key === "logout" ? "" : key.slice("exchange:".length);
    return reply;
  });
  return uc;
}

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
    call.mockImplementation(async () =>
      getToken()
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

  it("keeps the runtime config across a sign-in and sign-out", async () => {
    stubConfig({ authEnabled: true });
    call.mockImplementation(async (input: { path: string }) =>
      getToken() || input.path.endsWith("/auth/logout")
        ? {
            httpStatus: 200,
            ok: true,
            body: JSON.stringify({ displayName: "Ada" }),
          }
        : { httpStatus: 401, ok: false, body: "" },
    );
    const { result } = renderHook(() => useAuth(), { wrapper });
    await waitFor(() => expect(result.current.loading).toBe(false));

    await act(async () => {
      await result.current.signInWithAccessToken("jwt-xyz");
      await result.current.logout();
    });

    expect(result.current.authEnabled).toBe(true);
    await waitFor(() => expect(result.current.loading).toBe(false));
    expect(fetch).toHaveBeenCalledTimes(1);
  });

  it("lets a newer token sign-in finish when it supersedes an older one", async () => {
    stubConfig({ authEnabled: true });
    const replies: Record<string, ReturnType<typeof deferred>> = {
      "first-jwt": deferred(),
      "second-jwt": deferred(),
    };
    call.mockImplementation(async () =>
      getToken() in replies
        ? replies[getToken()].promise
        : { httpStatus: 401, ok: false, body: "" },
    );
    const { result } = renderHook(() => useAuth(), { wrapper });
    await waitFor(() => expect(result.current.loading).toBe(false));

    let first!: Promise<void>;
    let second!: Promise<void>;
    act(() => {
      first = result.current.signInWithAccessToken("first-jwt");
      second = result.current.signInWithAccessToken("second-jwt");
    });
    await act(async () => {
      replies["first-jwt"].resolve({ httpStatus: 401, ok: false, body: "" });
      replies["second-jwt"].resolve({
        httpStatus: 200,
        ok: true,
        body: JSON.stringify({ displayName: "Ada" }),
      });
      await Promise.all([first, second]);
    });

    expect(getToken()).toBe("second-jwt");
    await waitFor(() =>
      expect(result.current.currentUser?.displayName).toBe("Ada"),
    );
  });

  it("ends a pasted-token session with its Authorization header", async () => {
    stubConfig({ authEnabled: true });
    // Like UC with auth enabled: logout needs the bearer token.
    call.mockImplementation(async () => (getToken() ? ADA : UNAUTHORIZED));
    setToken("jwt-xyz");
    const { result } = renderHook(() => useAuth(), { wrapper });
    await waitFor(() => expect(result.current.loading).toBe(false));

    await act(async () => {
      await result.current.logout();
    });

    expect(getToken()).toBe("");
  });

  it("sends a sign-out only after the provider exchange it supersedes", async () => {
    stubConfig({ authEnabled: true });
    const uc = fakeCookieUc();
    const exchange = uc.hold("exchange:Ada");
    const { result } = renderHook(() => useAuth(), { wrapper });
    await waitFor(() => expect(result.current.loading).toBe(false));

    let login!: Promise<void>;
    let logout!: Promise<void>;
    act(() => {
      login = result.current.loginWithToken("Ada");
    });
    await waitFor(() => expect(uc.requests).toContain("exchange:Ada"));
    act(() => {
      logout = result.current.logout();
    });
    expect(uc.requests).not.toContain("logout");
    await act(async () => {
      exchange.resolve(OK);
      await Promise.all([login, logout]);
    });

    expect(uc.requests.filter((r) => r !== "me")).toEqual([
      "exchange:Ada",
      "logout",
    ]);
    expect(uc.cookie).toBe("");
    expect(result.current.authenticating).toBe(false);
    await waitFor(() => expect(result.current.loading).toBe(false));
    expect(result.current.currentUser).toBeNull();
  });

  it("applies overlapping provider exchanges in call order", async () => {
    stubConfig({ authEnabled: true });
    const uc = fakeCookieUc();
    const ada = uc.hold("exchange:Ada");
    const bob = uc.hold("exchange:Bob");
    const shown: (string | undefined)[] = [];
    const { result } = renderHook(
      () => {
        const auth = useAuth();
        shown.push(auth.currentUser?.displayName);
        return auth;
      },
      { wrapper },
    );
    await waitFor(() => expect(result.current.loading).toBe(false));

    let first!: Promise<void>;
    let second!: Promise<void>;
    act(() => {
      first = result.current.loginWithToken("Ada");
    });
    await waitFor(() => expect(uc.requests).toContain("exchange:Ada"));
    act(() => {
      second = result.current.loginWithToken("Bob");
    });
    // The newer reply is ready first, but its request waits for the older one.
    await act(async () => {
      bob.resolve(OK);
      await bob.promise;
    });
    expect(uc.requests).not.toContain("exchange:Bob");
    await act(async () => {
      ada.resolve(OK);
      await Promise.all([first, second]);
    });

    expect(uc.requests).toEqual(["me", "exchange:Ada", "exchange:Bob", "me"]);
    expect(uc.cookie).toBe("Bob");
    expect(result.current.currentUser?.displayName).toBe("Bob");
    expect(shown).not.toContain("Ada");
  });

  it("does not publish the user of a sign-in superseded during verification", async () => {
    stubConfig({ authEnabled: true });
    const uc = fakeCookieUc();
    const client = makeQueryClient();
    const cached: unknown[] = [];
    client.getQueryCache().subscribe(({ query }) => {
      if (query.queryKey[0] === CURRENT_USER_QUERY_KEY[0]) {
        cached.push(query.state.data);
      }
    });
    const { result } = renderHook(() => useAuth(), {
      wrapper: ({ children }: { children: ReactNode }) => (
        <Providers client={client}>
          <AuthProvider>{children}</AuthProvider>
        </Providers>
      ),
    });
    await waitFor(() => expect(result.current.loading).toBe(false));
    const adaVerified = uc.hold("me");

    let first!: Promise<void>;
    let second!: Promise<void>;
    act(() => {
      first = result.current.loginWithToken("Ada");
    });
    await waitFor(() => expect(result.current.authenticating).toBe(true));
    act(() => {
      second = result.current.loginWithToken("Bob");
    });
    await act(async () => {
      adaVerified.resolve(OK);
      await Promise.all([first, second]);
    });

    expect(uc.requests).toEqual([
      "me",
      "exchange:Ada",
      "me",
      "exchange:Bob",
      "me",
    ]);
    expect(result.current.currentUser?.displayName).toBe("Bob");
    expect(cached).not.toContainEqual({ displayName: "Ada" });
  });

  it("starts a provider sign-in only after a pending sign-out", async () => {
    stubConfig({ authEnabled: true });
    const uc = fakeCookieUc();
    uc.cookie = "Old";
    const logoutReply = uc.hold("logout");
    const { result } = renderHook(() => useAuth(), { wrapper });
    await waitFor(() =>
      expect(result.current.currentUser?.displayName).toBe("Old"),
    );

    let logout!: Promise<void>;
    let superseded!: Promise<void>;
    let login!: Promise<void>;
    act(() => {
      logout = result.current.logout();
      superseded = result.current.loginWithToken("Ada");
      login = result.current.loginWithToken("Bob");
    });
    await waitFor(() => expect(uc.requests).toContain("logout"));
    expect(uc.requests).not.toContain("exchange:Bob");
    await act(async () => {
      logoutReply.resolve(OK);
      await Promise.all([logout, superseded, login]);
    });

    // A sign-in superseded before it started never creates a session.
    expect(uc.requests).toEqual(["me", "logout", "exchange:Bob", "me"]);
    expect(uc.cookie).toBe("Bob");
    expect(result.current.currentUser?.displayName).toBe("Bob");
  });

  it("reconciles the cookie identity when a newer provider exchange fails", async () => {
    stubConfig({ authEnabled: true });
    const uc = fakeCookieUc();
    uc.cookie = "Old";
    const ada = uc.hold("exchange:Ada");
    const bob = uc.hold("exchange:Bob");
    const client = makeQueryClient();
    client.setQueryData(["private-catalog-data"], { owner: "Old" });
    const { result } = renderHook(() => useAuth(), {
      wrapper: ({ children }: { children: ReactNode }) => (
        <Providers client={client}>
          <AuthProvider>{children}</AuthProvider>
        </Providers>
      ),
    });
    await waitFor(() =>
      expect(result.current.currentUser?.displayName).toBe("Old"),
    );

    let first!: Promise<void>;
    let second!: Promise<void>;
    act(() => {
      first = result.current.loginWithToken("Ada");
    });
    await waitFor(() => expect(uc.requests).toContain("exchange:Ada"));
    expect(result.current.loading).toBe(true);
    expect(result.current.currentUser).toBeNull();
    act(() => {
      second = result.current.loginWithToken("Bob");
    });
    const rejected = expect(second).rejects.toThrow("HTTP 500");
    await act(async () => {
      bob.resolve({ httpStatus: 500, ok: false, body: "{}" });
      ada.resolve(OK);
      await first;
      await rejected;
    });

    expect(uc.cookie).toBe("Ada");
    await waitFor(() =>
      expect(result.current.currentUser?.displayName).toBe("Ada"),
    );
    expect(client.getQueryData(["private-catalog-data"])).toBeUndefined();
  });

  it("runs queued sign-ins after a sign-out that fails", async () => {
    stubConfig({ authEnabled: true });
    const uc = fakeCookieUc();
    const logoutReply = uc.hold("logout");
    const { result } = renderHook(() => useAuth(), { wrapper });
    await waitFor(() => expect(result.current.loading).toBe(false));

    let logout!: Promise<void>;
    let login!: Promise<void>;
    act(() => {
      logout = result.current.logout();
      login = result.current.loginWithToken("Ada");
    });
    const loggedOut = expect(logout).rejects.toThrow("HTTP 500");
    await act(async () => {
      logoutReply.resolve({ httpStatus: 500, ok: false, body: "{}" });
      await loggedOut;
      await login;
    });

    expect(uc.cookie).toBe("Ada");
    expect(result.current.currentUser?.displayName).toBe("Ada");
  });

  it("lets a sign-in that starts during a sign-out keep its session", async () => {
    stubConfig({ authEnabled: true });
    const logoutReply = deferred();
    const logoutTokens: string[] = [];
    call.mockImplementation(async (input: { path: string }) => {
      if (input.path.endsWith("/auth/logout")) {
        logoutTokens.push(getToken());
        return logoutReply.promise;
      }
      return getToken() ? ADA : UNAUTHORIZED;
    });
    setToken("old-jwt");
    const client = makeQueryClient();
    const { result } = renderHook(() => useAuth(), {
      wrapper: ({ children }: { children: ReactNode }) => (
        <Providers client={client}>
          <AuthProvider>{children}</AuthProvider>
        </Providers>
      ),
    });
    await waitFor(() => expect(result.current.loading).toBe(false));

    let logout!: Promise<void>;
    let signIn!: Promise<void>;
    act(() => {
      logout = result.current.logout();
      signIn = result.current.signInWithAccessToken("jwt-xyz");
    });
    await waitFor(() => expect(logoutTokens).toHaveLength(1));
    await act(async () => {
      logoutReply.resolve(OK);
      await Promise.all([logout, signIn]);
    });

    // The sign-out ends the old token's session, then the newer sign-in keeps
    // its token and cached user.
    expect(logoutTokens).toEqual(["old-jwt"]);
    expect(getToken()).toBe("jwt-xyz");
    expect(client.getQueryData(CURRENT_USER_QUERY_KEY)).toMatchObject({
      displayName: "Ada",
    });
  });

  it("useAuth throws outside a provider", () => {
    expect(() => renderHook(() => useAuth())).toThrow(/AuthProvider/);
  });
});
