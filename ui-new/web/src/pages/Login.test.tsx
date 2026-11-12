import { describe, expect, it, vi, beforeEach, afterEach } from "vitest";
import { act, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

vi.mock("@tanstack/react-router", () => import("@/test/tanstack-router-mock"));

const { signInWithAccessToken, loginWithToken, signInWithOkta } = vi.hoisted(() => ({
  signInWithAccessToken: vi.fn(),
  loginWithToken: vi.fn().mockResolvedValue(undefined),
  signInWithOkta: vi.fn().mockResolvedValue(undefined),
}));
const { auth } = vi.hoisted(() => ({ auth: { current: {} as Record<string, unknown> } }));
vi.mock("@/context/auth-context", () => ({
  useAuth: () => ({
    loginWithToken,
    signInWithAccessToken,
    signInWithOkta,
    loading: false,
    currentUser: null,
    ...auth.current,
  }),
}));

const { appConfig } = vi.hoisted(() => ({ appConfig: { current: {} as Record<string, unknown> } }));
vi.mock("@/lib/appConfig", () => ({
  useAppConfig: () => ({ data: appConfig.current }),
}));

import Login from "@/pages/Login";
import OktaCallback from "@/pages/OktaCallback";
import { navigateMock } from "@/test/tanstack-router-mock";
import { beginOktaLogin, markOktaToken, type OktaConfig } from "@/lib/okta";
import { getToken, setToken } from "@/lib/session";
import { CURRENT_USER_QUERY_KEY } from "@/hooks/user";
import { makeQueryClient, renderWithProviders } from "@/test/providers";

const oktaAppConfig = {
  oktaEnabled: true,
  oktaIssuer: "https://dev-1.okta.com/oauth2/aus1",
  oktaClientId: "client-1",
};
const oktaCfg: OktaConfig = {
  issuer: oktaAppConfig.oktaIssuer,
  clientId: "client-1",
  scopes: "openid profile email",
  redirectUri: `${window.location.origin}/login/callback`,
};
const assignMock = vi.fn();
let client = makeQueryClient();
const renderLogin = () => renderWithProviders(<Login />, { client });

beforeEach(() => {
  client = makeQueryClient();
  auth.current = {};
  sessionStorage.clear(); // the Okta retry guard lives there
  navigateMock.mockReset();
  signInWithAccessToken.mockReset();
  signInWithOkta.mockReset().mockResolvedValue(undefined);
  assignMock.mockReset();
  vi.spyOn(window.location, "assign").mockImplementation(assignMock);
  appConfig.current = {};
});

afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});

describe("Login", () => {
  it("shows a no-providers message when nothing is enabled", () => {
    renderLogin();
    expect(screen.getByText(/No auth providers are enabled/)).toBeInTheDocument();
  });

  it("advertises Okta and Keycloak when enabled", () => {
    appConfig.current = { oktaEnabled: true, keycloakEnabled: true };
    renderLogin();
    expect(screen.getByText(/Okta sign-in is enabled/)).toBeInTheDocument();
    expect(screen.getByText(/Keycloak sign-in is enabled/)).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /Sign in with Okta/ })).not.toBeInTheDocument();
  });

  it("validates an empty pasted token", async () => {
    renderLogin();
    await userEvent.click(screen.getByRole("button", { name: /Use token/ }));
    expect(screen.getByText("Paste a JWT access token.")).toBeInTheDocument();
    expect(signInWithAccessToken).not.toHaveBeenCalled();
  });

  it("signs in with a pasted token and navigates home", async () => {
    renderLogin();
    await userEvent.type(screen.getByLabelText("JWT access token"), "jwt-abc");
    await userEvent.click(screen.getByRole("button", { name: /Use token/ }));
    expect(signInWithAccessToken).toHaveBeenCalledWith("jwt-abc");
    expect(navigateMock).toHaveBeenCalledWith({ to: "/" });
  });

  it("sends the browser to the Okta authorize URL with a PKCE challenge", async () => {
    appConfig.current = oktaAppConfig;
    renderLogin();
    await userEvent.click(screen.getByRole("button", { name: /Sign in with Okta/ }));
    await waitFor(() => expect(assignMock).toHaveBeenCalledTimes(1));
    const url = new URL(assignMock.mock.calls[0][0]);
    expect(`${url.origin}${url.pathname}`).toBe("https://dev-1.okta.com/oauth2/aus1/v1/authorize");
    expect(url.searchParams.get("client_id")).toBe("client-1");
    expect(url.searchParams.get("redirect_uri")).toBe(`${window.location.origin}/login/callback`);
    expect(url.searchParams.get("code_challenge_method")).toBe("S256");
  });

  it("starts a new Okta sign-in when the stored Okta token was refused, once", async () => {
    appConfig.current = oktaAppConfig;
    setToken("expired-okta-token");
    markOktaToken(true);
    client.setQueryData(CURRENT_USER_QUERY_KEY, null); // /scim2/Me answered 401
    const { unmount } = renderLogin();
    await waitFor(() => expect(assignMock).toHaveBeenCalledTimes(1));
    expect(getToken()).toBe("");

    // Okta's new token is refused too: no second automatic redirect.
    unmount();
    setToken("refused-okta-token");
    markOktaToken(true);
    renderLogin();
    expect(await screen.findByText(/refused your Okta sign-in/)).toBeInTheDocument();
    expect(assignMock).toHaveBeenCalledTimes(1);
  });

  it.each([
    ["the user was not fetched yet (a manual visit)", () => {}],
    [
      "the user is still being fetched",
      () => void client.fetchQuery({ queryKey: CURRENT_USER_QUERY_KEY, queryFn: () => new Promise(() => {}) }),
    ],
    [
      "/scim2/Me failed with a 5xx",
      () =>
        client
          .fetchQuery({
            queryKey: CURRENT_USER_QUERY_KEY,
            queryFn: () => Promise.reject(new Error("Failed to fetch user (HTTP 503)")),
          })
          .catch(() => {}),
    ],
  ])("keeps the stored Okta token when %s", async (_name, setUp) => {
    appConfig.current = oktaAppConfig;
    setToken("valid-okta-token");
    markOktaToken(true);
    await setUp();
    renderLogin();
    await new Promise((r) => setTimeout(r, 50));
    expect(getToken()).toBe("valid-okta-token");
    expect(assignMock).not.toHaveBeenCalled();
  });

  it("acts on a 401 that arrives by a background refetch while the login page is showing", async () => {
    appConfig.current = oktaAppConfig;
    setToken("valid-okta-token");
    markOktaToken(true);
    await client
      .fetchQuery({
        queryKey: CURRENT_USER_QUERY_KEY,
        queryFn: () => Promise.reject(new Error("Failed to fetch user (HTTP 503)")),
      })
      .catch(() => {});
    renderLogin();
    await new Promise((r) => setTimeout(r, 50));
    expect(assignMock).not.toHaveBeenCalled();

    await act(async () => {
      await client.fetchQuery({ queryKey: CURRENT_USER_QUERY_KEY, queryFn: async () => null }); // reconnect, now 401
    });
    await waitFor(() => expect(assignMock).toHaveBeenCalledTimes(1));
    expect(getToken()).toBe("");
  });

  it("goes home as soon as a user is known", () => {
    auth.current = { currentUser: { displayName: "Ada" } };
    renderLogin();
    expect(navigateMock).toHaveBeenCalledWith({ to: "/", replace: true });
  });

  it("shows no Okta button or message when OKTA_AUTH_ENABLED is false", () => {
    appConfig.current = { ...oktaAppConfig, oktaEnabled: false };
    renderLogin();
    expect(screen.queryByRole("button", { name: /Sign in with Okta/ })).not.toBeInTheDocument();
    expect(screen.queryByText(/OKTA_ISSUER and OKTA_CLIENT_ID/)).not.toBeInTheDocument();
  });

  it("does not redirect to Okta for a refused pasted token", () => {
    appConfig.current = oktaAppConfig;
    setToken("pasted-token");
    renderLogin();
    expect(assignMock).not.toHaveBeenCalled();
    expect(getToken()).toBe("pasted-token");
  });
});

describe("OktaCallback", () => {
  beforeEach(() => {
    appConfig.current = oktaAppConfig;
  });

  async function startLogin() {
    return new URL(await beginOktaLogin(oktaCfg)).searchParams.get("state")!;
  }

  function stubOkta(tokenResponse: unknown = { access_token: "at", id_token: "idt" }) {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue({ ok: true, json: async () => tokenResponse }));
  }

  it("exchanges the code, signs in and goes home", async () => {
    const state = await startLogin();
    window.history.replaceState({}, "", `/login/callback?code=abc&state=${state}`);
    stubOkta();
    render(<OktaCallback />);
    await waitFor(() => expect(signInWithOkta).toHaveBeenCalledWith({ accessToken: "at", idToken: "idt" }));
    expect(navigateMock).toHaveBeenCalledWith({ to: "/", replace: true });
    window.history.replaceState({}, "", "/");
  });

  it("shows the failure and a way back when the state does not match", async () => {
    await startLogin();
    window.history.replaceState({}, "", "/login/callback?code=abc&state=forged");
    stubOkta();
    render(<OktaCallback />);
    expect(await screen.findByText(/state does not match/)).toBeInTheDocument();
    expect(signInWithOkta).not.toHaveBeenCalled();
    expect(screen.getByRole("link", { name: /Back to login/ })).toHaveAttribute("href", "/login");
    window.history.replaceState({}, "", "/");
  });

  it("shows the failure when Unity Catalog refuses the login", async () => {
    const state = await startLogin();
    window.history.replaceState({}, "", `/login/callback?code=abc&state=${state}`);
    stubOkta();
    signInWithOkta.mockRejectedValue(new Error("Login failed (HTTP 403)"));
    render(<OktaCallback />);
    expect(await screen.findByText("Login failed (HTTP 403)")).toBeInTheDocument();
    expect(navigateMock).not.toHaveBeenCalled();
    window.history.replaceState({}, "", "/");
  });
});
