import {
  createContext,
  useCallback,
  useContext,
  useMemo,
  useState,
} from "react";
import type { ReactNode } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { useAppConfig } from "@/lib/appConfig";
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

  const loginWithToken = useCallback(
    async (idToken: string) => {
      await loginMutation.mutateAsync(idToken);
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
    [loginMutation, queryClient],
  );

  const signInWithAccessToken = useCallback(
    async (accessToken: string) => {
      setToken(accessToken);
      try {
        const user = await queryClient.fetchQuery({
          queryKey: CURRENT_USER_QUERY_KEY,
          queryFn: fetchCurrentUser,
        });
        if (!user) {
          throw new Error("Unity Catalog rejected this access token.");
        }
      } catch (error) {
        clearToken();
        queryClient.removeQueries({ queryKey: CURRENT_USER_QUERY_KEY });
        throw error;
      }
    },
    [queryClient],
  );

  const logout = useCallback(async () => {
    clearToken();
    try {
      await logoutMutation.mutateAsync();
    } finally {
      queryClient.invalidateQueries();
    }
  }, [logoutMutation, queryClient]);

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
