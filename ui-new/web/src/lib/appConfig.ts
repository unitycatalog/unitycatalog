import { useQuery } from "@tanstack/react-query";

// AppConfig is the runtime config the Rust bridge exposes to the SPA at GET
// /config. It is the runtime equivalent of the current ui's build-time
// REACT_APP_*_AUTH_ENABLED flags: the SPA learns whether auth is enabled and
// which providers to render without an image rebuild.
export type AppConfig = {
  // authEnabled gates the whole login flow. When false the SPA renders the app
  // directly (the bridge proxies UC without requiring a session cookie).
  authEnabled: boolean;
  // googleClientId, when set, enables the Google Identity Services button.
  googleClientId: string;
  oktaEnabled: boolean;
  keycloakEnabled: boolean;
};

const EMPTY: AppConfig = {
  authEnabled: false,
  googleClientId: "",
  oktaEnabled: false,
  keycloakEnabled: false,
};

// useAppConfig fetches the bridge's runtime SPA config once. It never throws — a
// missing/erroring endpoint yields auth-disabled defaults so the SPA still loads.
export function useAppConfig() {
  return useQuery({
    queryKey: ["app-config"],
    staleTime: Infinity,
    queryFn: async (): Promise<AppConfig> => {
      try {
        const res = await fetch("/config");
        if (!res.ok) return EMPTY;
        return { ...EMPTY, ...((await res.json()) as Partial<AppConfig>) };
      } catch {
        return EMPTY;
      }
    },
  });
}
