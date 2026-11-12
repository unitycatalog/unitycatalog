import type { AppConfig } from "@/lib/appConfig";

// "Sign in with Okta": OAuth 2.0 authorization code flow with PKCE (RFC 7636)
// from the browser, as a public client, against an Okta authorization server.
// The Okta admin sets the audience of the access token in that server; the SPA
// only chooses the client and the scopes.

export type OktaConfig = {
  issuer: string;
  clientId: string;
  scopes: string;
  redirectUri: string;
};

export type OktaTokens = { accessToken: string; idToken: string };

// The verifier and state live in sessionStorage so they survive the redirect to
// Okta in this tab only.
const VERIFIER_KEY = "uc-okta-pkce-verifier";
const STATE_KEY = "uc-okta-pkce-state";
// Marks that the stored bearer token came from Okta, so a 401 on it can start
// a new Okta sign-in instead of showing the paste box.
const OKTA_TOKEN_KEY = "uc-okta-token";

// A refused Okta token restarts the sign-in automatically, but not again within
// this window, so a token the server keeps refusing cannot loop through Okta.
const RETRY_KEY = "uc-okta-retry-at";
const RETRY_WINDOW_MS = 60_000;

const DEFAULT_SCOPES = "openid profile email";

// oktaConfigFrom reads the Okta client settings the bridge serves at /config.
// Null unless Okta is enabled and both the issuer and the client id are set.
export function oktaConfigFrom(
  config: Pick<AppConfig, "oktaEnabled" | "oktaIssuer" | "oktaClientId" | "oktaScopes" | "oktaRedirectUri">,
): OktaConfig | null {
  if (!config.oktaEnabled || !config.oktaIssuer || !config.oktaClientId) return null;
  return {
    issuer: config.oktaIssuer,
    clientId: config.oktaClientId,
    scopes: config.oktaScopes || DEFAULT_SCOPES,
    redirectUri: config.oktaRedirectUri || `${window.location.origin}/login/callback`,
  };
}

function base64Url(bytes: Uint8Array): string {
  let binary = "";
  for (const b of bytes) binary += String.fromCharCode(b);
  return btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

function randomString(): string {
  return base64Url(crypto.getRandomValues(new Uint8Array(32)));
}

// RFC 7636 section 4.1: 43 to 128 URL-safe characters; 32 random bytes give 43.
export function createCodeVerifier(): string {
  return randomString();
}

export async function codeChallenge(verifier: string): Promise<string> {
  const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(verifier));
  return base64Url(new Uint8Array(digest));
}

function endpoint(cfg: OktaConfig, name: "authorize" | "token"): string {
  return `${cfg.issuer.replace(/\/+$/, "")}/v1/${name}`;
}

export function authorizeUrl(cfg: OktaConfig, p: { state: string; challenge: string }): string {
  const query = new URLSearchParams({
    client_id: cfg.clientId,
    response_type: "code",
    scope: cfg.scopes,
    redirect_uri: cfg.redirectUri,
    state: p.state,
    code_challenge: p.challenge,
    code_challenge_method: "S256",
  });
  return `${endpoint(cfg, "authorize")}?${query}`;
}

// beginOktaLogin remembers a fresh verifier and state and returns the URL to
// send the browser to.
export async function beginOktaLogin(cfg: OktaConfig): Promise<string> {
  const verifier = createCodeVerifier();
  const state = randomString();
  sessionStorage.setItem(VERIFIER_KEY, verifier);
  sessionStorage.setItem(STATE_KEY, state);
  return authorizeUrl(cfg, { state, challenge: await codeChallenge(verifier) });
}

// takeCallback checks Okta's redirect query against the sign-in started in this
// tab and returns the code with its verifier. The pending sign-in is consumed
// even on failure, so a callback cannot be replayed.
export function takeCallback(search: string): {
  code: string;
  verifier: string;
} {
  const expectedState = sessionStorage.getItem(STATE_KEY);
  const verifier = sessionStorage.getItem(VERIFIER_KEY);
  sessionStorage.removeItem(STATE_KEY);
  sessionStorage.removeItem(VERIFIER_KEY);
  if (!expectedState || !verifier) {
    throw new Error("No Okta sign-in is in progress. Start again from the login page.");
  }
  const params = new URLSearchParams(search);
  if (params.get("state") !== expectedState) {
    throw new Error("Okta sign-in failed: the state does not match. Start again from the login page.");
  }
  const error = params.get("error");
  if (error) throw new Error(params.get("error_description") || `Okta sign-in failed: ${error}`);
  const code = params.get("code");
  if (!code) throw new Error("Okta did not return a code. Start again from the login page.");
  return { code, verifier };
}

export async function exchangeCode(cfg: OktaConfig, code: string, verifier: string): Promise<OktaTokens> {
  const res = await fetch(endpoint(cfg, "token"), {
    method: "POST",
    headers: {
      "Content-Type": "application/x-www-form-urlencoded",
      Accept: "application/json",
    },
    body: new URLSearchParams({
      grant_type: "authorization_code",
      client_id: cfg.clientId,
      redirect_uri: cfg.redirectUri,
      code,
      code_verifier: verifier,
    }).toString(),
  });
  const body = (await res.json().catch(() => ({}))) as {
    access_token?: string;
    id_token?: string;
    error?: string;
    error_description?: string;
  };
  if (!res.ok) {
    throw new Error(body.error_description || body.error || `Okta token request failed (HTTP ${res.status})`);
  }
  if (!body.access_token) throw new Error("Okta returned no access token.");
  return { accessToken: body.access_token, idToken: body.id_token ?? "" };
}

export function markOktaToken(isOkta: boolean) {
  if (isOkta) localStorage.setItem(OKTA_TOKEN_KEY, "1");
  else localStorage.removeItem(OKTA_TOKEN_KEY);
}

export function isOktaToken(): boolean {
  return localStorage.getItem(OKTA_TOKEN_KEY) === "1";
}

// canRetryOktaSignIn says whether a refused Okta token may start a new sign-in
// now, and records the attempt.
export function canRetryOktaSignIn(now = Date.now()): boolean {
  const last = Number(sessionStorage.getItem(RETRY_KEY) ?? 0);
  if (now - last < RETRY_WINDOW_MS) return false;
  sessionStorage.setItem(RETRY_KEY, String(now));
  return true;
}
