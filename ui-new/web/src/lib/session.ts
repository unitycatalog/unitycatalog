import { useSyncExternalStore } from "react";

// A client-side bearer token that bypasses the cookie login: when set, it is
// threaded into every UnityProxyService.Call as `token`, which the bridge turns
// into an `Authorization: Bearer` header on the UC request. This mirrors the
// reference console's "paste a token" mode.
//
// The token is persisted in localStorage and can be seeded once from a `?token=`
// URL parameter (which is then scrubbed from the URL so it does not linger in
// browser history or shared links).

const KEY = "uc-ui-token";
const listeners = new Set<() => void>();

function bootstrap(): string {
  try {
    const url = new URL(window.location.href);
    const fromQuery = url.searchParams.get("token");
    if (fromQuery) {
      const t = fromQuery.trim();
      localStorage.setItem(KEY, t);
      url.searchParams.delete("token");
      window.history.replaceState({}, "", url.toString());
      return t;
    }
    return localStorage.getItem(KEY) ?? "";
  } catch {
    return "";
  }
}

let token = bootstrap();

function emit() {
  for (const l of listeners) l();
}

export function getToken(): string {
  return token;
}

export function setToken(next: string) {
  token = next.trim();
  try {
    if (token) localStorage.setItem(KEY, token);
    else localStorage.removeItem(KEY);
  } catch {
    // Ignore storage failures (private mode / disabled storage); the in-memory
    // value still drives this session.
  }
  emit();
}

export function clearToken() {
  setToken("");
}

export function hasToken(): boolean {
  return token.length > 0;
}

function subscribe(cb: () => void) {
  listeners.add(cb);
  return () => {
    listeners.delete(cb);
  };
}

// useToken subscribes a component to token changes so queries re-key when the
// caller signs in/out with a pasted token.
export function useToken(): string {
  return useSyncExternalStore(subscribe, getToken, getToken);
}
