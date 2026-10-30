import { createContext, useCallback, useContext, useMemo } from "react";
import type { ReactNode } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { useAppConfig } from "@/lib/appConfig";
import { clearToken, setToken, useToken } from "@/lib/session";
import {
  CURRENT_USER_QUERY_KEY,
  useGetCurrentUser,
  useLoginWithToken,
  useLogoutCurrentUser,
  type UserInterface,
} from "@/hooks/user";

interface AuthContextValue {
  // Whether auth is enabled at all. When false the app renders without a login
  // gate and currentUser is null.
  authEnabled: boolean;
  // Still resolving initial auth state (config + current user).
  loading: boolean;
  currentUser: UserInterface | null;
  // Whether a pasted bearer token is active for this session.
  hasAccessToken: boolean;
  // OAuth id_token exchange (Google) -> UC session cookie.
  loginWithToken: (idToken: string) => Promise<void>;
  // Paste-a-token bypass: store a UC access JWT and use it as a bearer on every
  // call. Refetches the current user so the gate updates immediately.
  signInWithAccessToken: (accessToken: string) => void;
  logout: () => Promise<void>;
}

const AuthContext = createContext<AuthContextValue | undefined>(undefined);
AuthContext.displayName = "AuthContext";

export function AuthProvider({ children }: { children: ReactNode }) {
  const queryClient = useQueryClient();
  const { data: appConfig, isLoading: configLoading } = useAppConfig();
  const authEnabled = appConfig?.authEnabled ?? false;
  const token = useToken();

  // Resolve the current user whenever auth is enabled OR a bearer token is
  // present (a pasted token authenticates even if config didn't advertise a
  // provider). Auth-disabled mode with no token skips the SCIM round-trip.
  const { data: currentUser, isLoading: userLoading } = useGetCurrentUser(
    authEnabled || token.length > 0,
  );

  const loginMutation = useLoginWithToken();
  const logoutMutation = useLogoutCurrentUser();

  const loginWithToken = useCallback(
    (idToken: string) => loginMutation.mutateAsync(idToken),
    [loginMutation],
  );

  const signInWithAccessToken = useCallback(
    (accessToken: string) => {
      setToken(accessToken);
      queryClient.invalidateQueries({ queryKey: CURRENT_USER_QUERY_KEY });
    },
    [queryClient],
  );

  const logout = useCallback(async () => {
    clearToken();
    try {
      await logoutMutation.mutateAsync();
    } finally {
      queryClient.invalidateQueries({ queryKey: CURRENT_USER_QUERY_KEY });
    }
  }, [logoutMutation, queryClient]);

  const value = useMemo<AuthContextValue>(
    () => ({
      authEnabled,
      loading: configLoading || ((authEnabled || token.length > 0) && userLoading),
      currentUser: currentUser ?? null,
      hasAccessToken: token.length > 0,
      loginWithToken,
      signInWithAccessToken,
      logout,
    }),
    [authEnabled, configLoading, userLoading, currentUser, token, loginWithToken, signInWithAccessToken, logout],
  );

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}

export function useAuth(): AuthContextValue {
  const ctx = useContext(AuthContext);
  if (!ctx) throw new Error("useAuth must be used within an AuthProvider");
  return ctx;
}
