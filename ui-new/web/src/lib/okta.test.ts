import { describe, expect, it, vi, afterEach } from "vitest";
import {
  authorizeUrl,
  beginOktaLogin,
  canRetryOktaSignIn,
  codeChallenge,
  createCodeVerifier,
  exchangeCode,
  oktaConfigFrom,
  takeCallback,
  type OktaConfig,
} from "@/lib/okta";

const cfg: OktaConfig = {
  issuer: "https://dev-1.okta.com/oauth2/aus1",
  clientId: "client-1",
  scopes: "openid profile email",
  redirectUri: "https://uc.example.com/login/callback",
};

afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});

describe("PKCE helpers", () => {
  it("creates a URL-safe verifier of 43 characters, different each time", () => {
    const a = createCodeVerifier();
    expect(a).toMatch(/^[A-Za-z0-9_-]{43}$/);
    expect(createCodeVerifier()).not.toBe(a);
  });

  it("derives the S256 challenge from RFC 7636 appendix B", async () => {
    expect(await codeChallenge("dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk")).toBe(
      "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM",
    );
  });

  it("builds the Okta authorize URL for the code flow with the S256 challenge", () => {
    const url = new URL(authorizeUrl(cfg, { state: "st", challenge: "ch" }));
    expect(`${url.origin}${url.pathname}`).toBe("https://dev-1.okta.com/oauth2/aus1/v1/authorize");
    expect(Object.fromEntries(url.searchParams)).toEqual({
      client_id: "client-1",
      response_type: "code",
      scope: "openid profile email",
      redirect_uri: "https://uc.example.com/login/callback",
      state: "st",
      code_challenge: "ch",
      code_challenge_method: "S256",
    });
  });

  it("tolerates a trailing slash on the issuer", () => {
    const url = authorizeUrl({ ...cfg, issuer: `${cfg.issuer}/` }, { state: "s", challenge: "c" });
    expect(url).toContain("/oauth2/aus1/v1/authorize?");
  });
});

describe("callback state check", () => {
  it("returns the code and the stored verifier when the state matches, once", async () => {
    const url = new URL(await beginOktaLogin(cfg));
    const state = url.searchParams.get("state");
    const got = takeCallback(`?code=abc&state=${state}`);
    expect(got.code).toBe("abc");
    // The verifier kept across the redirect is the one the challenge was made from.
    expect(await codeChallenge(got.verifier)).toBe(url.searchParams.get("code_challenge"));
    // The pending login is consumed: a replay of the same callback is refused.
    expect(() => takeCallback(`?code=abc&state=${state}`)).toThrow("No Okta sign-in is in progress");
  });

  it.each([
    ["a different state", "?code=abc&state=forged", "state does not match"],
    ["no state", "?code=abc", "state does not match"],
    [
      "an Okta error with a different state",
      "?error=access_denied&error_description=Call+helpdesk&state=forged",
      "state does not match",
    ],
    ["no code", "?state=SAME", "did not return a code"],
    ["an Okta error", "?error=access_denied&error_description=User+is+not+assigned&state=SAME", "User is not assigned"],
  ])("refuses %s", async (_name, search, message) => {
    const state = new URL(await beginOktaLogin(cfg)).searchParams.get("state")!;
    expect(() => takeCallback(search.replace("SAME", state))).toThrow(message);
    // A failed callback also consumes the pending login.
    expect(() => takeCallback(`?code=abc&state=${state}`)).toThrow("No Okta sign-in is in progress");
  });

  it("refuses a callback when no sign-in was started in this tab", () => {
    expect(() => takeCallback("?code=abc&state=x")).toThrow("No Okta sign-in is in progress");
  });
});

describe("exchangeCode", () => {
  it("posts the code and verifier to the token endpoint and returns both tokens", async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      ok: true,
      json: async () => ({
        access_token: "at",
        id_token: "idt",
        token_type: "Bearer",
      }),
    });
    vi.stubGlobal("fetch", fetchMock);
    const tokens = await exchangeCode(cfg, "the-code", "the-verifier");
    expect(tokens).toEqual({ accessToken: "at", idToken: "idt" });
    const [url, init] = fetchMock.mock.calls[0];
    expect(url).toBe("https://dev-1.okta.com/oauth2/aus1/v1/token");
    expect(init.method).toBe("POST");
    expect(init.headers["Content-Type"]).toBe("application/x-www-form-urlencoded");
    expect(Object.fromEntries(new URLSearchParams(init.body))).toEqual({
      grant_type: "authorization_code",
      client_id: "client-1",
      redirect_uri: "https://uc.example.com/login/callback",
      code: "the-code",
      code_verifier: "the-verifier",
    });
  });

  it("surfaces Okta's error description", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue({
        ok: false,
        status: 400,
        json: async () => ({
          error: "invalid_grant",
          error_description: "The code has expired.",
        }),
      }),
    );
    await expect(exchangeCode(cfg, "c", "v")).rejects.toThrow("The code has expired.");
  });

  it("fails when the response carries no access token", async () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue({ ok: true, json: async () => ({}) }));
    await expect(exchangeCode(cfg, "c", "v")).rejects.toThrow("no access token");
  });
});

describe("oktaConfigFrom", () => {
  it("is null unless Okta is enabled and both the issuer and the client id are set", () => {
    expect(oktaConfigFrom({ oktaEnabled: true })).toBeNull();
    expect(oktaConfigFrom({ oktaEnabled: true, oktaIssuer: "https://i" })).toBeNull();
    expect(oktaConfigFrom({ oktaEnabled: true, oktaClientId: "c" })).toBeNull();
    expect(oktaConfigFrom({ oktaEnabled: false, oktaIssuer: "https://i", oktaClientId: "c" })).toBeNull();
  });

  it("defaults the scopes and the redirect URI", () => {
    expect(oktaConfigFrom({ oktaEnabled: true, oktaIssuer: "https://i", oktaClientId: "c" })).toEqual({
      issuer: "https://i",
      clientId: "c",
      scopes: "openid profile email",
      redirectUri: `${window.location.origin}/login/callback`,
    });
  });

  it("keeps configured scopes and redirect URI", () => {
    expect(
      oktaConfigFrom({
        oktaEnabled: true,
        oktaIssuer: "https://i",
        oktaClientId: "c",
        oktaScopes: "openid email",
        oktaRedirectUri: "https://x/cb",
      }),
    ).toMatchObject({ scopes: "openid email", redirectUri: "https://x/cb" });
  });
});

describe("canRetryOktaSignIn", () => {
  it("allows one automatic retry per minute", () => {
    expect(canRetryOktaSignIn(1_000_000)).toBe(true);
    expect(canRetryOktaSignIn(1_000_000 + 59_999)).toBe(false);
    expect(canRetryOktaSignIn(1_000_000 + 60_000)).toBe(true);
  });
});
