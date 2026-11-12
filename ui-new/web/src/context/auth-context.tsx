import {
  createContext,
  useCallback,
  useContext,
  useMemo,
  useRef,
  useState,
} from "react";
import type { ReactNode } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { APP_CONFIG_QUERY_KEY, useAppConfig } from "@/lib/appConfig";
import { clearToken, setToken, useToken } from "@/lib/session";
import {
  CURRENT_USER_QUERY_KEY,
  fetchCurrentUser,
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
  // The provider exchange succeeded and the resulting session is being verified.
  authenticating: boolean;
  // OAuth id_token exchange (Google) -> UC session cookie.
  loginWithToken: (idToken: string) => Promise<void>;
  // Paste-a-token bypass: store a UC access JWT and use it as a bearer on every
  // call. Validates it against the current-user endpoint before signing in.
  signInWithAccessToken: (accessToken: string) => Promise<void>;
  // Clears the local session, then ends the UC session. Rejects if UC could not
  // end it, so the caller can tell the user they may still be signed in.
  logout: () => Promise<void>;
}

const AuthContext = createContext<AuthContextValue | undefined>(undefined);
AuthContext.displayName = "AuthContext";

export function AuthProvider({ children }: { children: ReactNode }) {
  const queryClient = useQueryClient();
  const { data: appConfig, isLoading: configLoading } = useAppConfig();
  const authEnabled = appConfig?.authEnabled ?? false;
  const token = useToken();
  const [authenticating, setAuthenticating] = useState(false);

  // Resolve the current user whenever auth is enabled OR a bearer token is
  // present (a pasted token authenticates even if config didn't advertise a
  // provider). Auth-disabled mode with no token skips the SCIM round-trip.
  const { data: currentUser, isLoading: userLoading } = useGetCurrentUser(
    authEnabled || token.length > 0,
  );

  const loginMutation = useLoginWithToken();
  const logoutMutation = useLogoutCurrentUser();

  // Counts sign-ins and sign-outs. A token verification that a newer one
  // superseded must not clear the token or cache that the newer one set up.
  const identityChanges = useRef(0);

  // Removes every cached result that belongs to the signed-in identity. The
  // runtime config does not, so it stays and the app keeps its auth settings.
  const clearIdentityQueries = useCallback(
    () =>
      queryClient.removeQueries({
        predicate: ({ queryKey }) => queryKey[0] !== APP_CONFIG_QUERY_KEY[0],
      }),
    [queryClient],
  );

  const loginWithToken = useCallback(
    async (idToken: string) => {
      identityChanges.current += 1;
      // UC prefers a Bearer header over the session cookie, so a leftover pasted
      // token would make the new provider session fail verification.
      clearToken();
      await loginMutation.mutateAsync(idToken);
      clearIdentityQueries();
      setAuthenticating(true);
      try {
        const user = await queryClient.fetchQuery({
          queryKey: CURRENT_USER_QUERY_KEY,
          queryFn: fetchCurrentUser,
          staleTime: 0,
        });
        if (!user) {
          throw new Error(
            "Login succeeded, but the session could not be verified.",
          );
        }
      } finally {
        setAuthenticating(false);
      }
    },
    [clearIdentityQueries, loginMutation, queryClient],
  );

  const signInWithAccessToken = useCallback(
    async (accessToken: string) => {
      const attempt = ++identityChanges.current;
      setToken(accessToken);
      clearIdentityQueries();
      try {
        const user = await queryClient.fetchQuery({
          queryKey: CURRENT_USER_QUERY_KEY,
          queryFn: fetchCurrentUser,
        });
        if (!user) {
          throw new Error("Unity Catalog rejected this access token.");
        }
      } catch (error) {
        // A newer sign-in or sign-out cancelled this one and owns the session.
        if (attempt !== identityChanges.current) return;
        clearToken();
        clearIdentityQueries();
        throw error;
      }
    },
    [clearIdentityQueries, queryClient],
  );

  const logout = useCallback(async () => {
    identityChanges.current += 1;
    clearToken();
    clearIdentityQueries();
    try {
      await logoutMutation.mutateAsync();
    } finally {
      clearIdentityQueries();
    }
  }, [clearIdentityQueries, logoutMutation]);

  const value = useMemo<AuthContextValue>(
    () => ({
      authEnabled,
      loading:
        configLoading || ((authEnabled || token.length > 0) && userLoading),
      currentUser: currentUser ?? null,
      hasAccessToken: token.length > 0,
      authenticating,
      loginWithToken,
      signInWithAccessToken,
      logout,
    }),
    [
      authEnabled,
      configLoading,
      userLoading,
      currentUser,
      token,
      authenticating,
      loginWithToken,
      signInWithAccessToken,
      logout,
    ],
  );

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}

export function useAuth(): AuthContextValue {
  const ctx = useContext(AuthContext);
  if (!ctx) throw new Error("useAuth must be used within an AuthProvider");
  return ctx;
}

export function useOptionalAuth(): AuthContextValue | undefined {
  return useContext(AuthContext);
}
