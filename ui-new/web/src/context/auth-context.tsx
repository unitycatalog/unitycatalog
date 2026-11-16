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
  // Resolving auth state (config, queued session changes, or current user).
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
  // Ends the UC session, then clears the local one even if UC could not end it.
  // Rejects in that case, so the caller can tell the user they may still be
  // signed in.
  logout: () => Promise<void>;
}

// Whether the call that queued a session change is still the newest one.
type IsLatest = () => boolean;

const AuthContext = createContext<AuthContextValue | undefined>(undefined);
AuthContext.displayName = "AuthContext";

export function AuthProvider({ children }: { children: ReactNode }) {
  const queryClient = useQueryClient();
  const { data: appConfig, isLoading: configLoading } = useAppConfig();
  const authEnabled = appConfig?.authEnabled ?? false;
  const token = useToken();
  const [authenticating, setAuthenticating] = useState(false);

  // Sign-ins and sign-outs run one at a time, in call order. A provider
  // exchange or logout rewrites the session cookie when its response arrives,
  // so overlapping ones could leave the browser on an older session than the
  // UI shows. Each change sees its predecessors' state; only the newest sign-in
  // publishes its user.
  const sessionChanges = useRef<Promise<void>>(Promise.resolve());
  const latestChange = useRef(0);
  const [queuedChanges, setQueuedChanges] = useState(0);

  // Resolve the current user whenever auth is enabled OR a bearer token is
  // present (a pasted token authenticates even if config didn't advertise a
  // provider). Auth-disabled mode with no token skips the SCIM round-trip.
  // While session changes are queued the query waits, so it cannot cache the
  // user of a session that a queued change is about to replace.
  const { data: currentUser, isLoading: userLoading } = useGetCurrentUser(
    (authEnabled || token.length > 0) && queuedChanges === 0,
  );

  const loginMutation = useLoginWithToken();
  const logoutMutation = useLogoutCurrentUser();

  const enqueue = useCallback(
    (change: (isLatest: IsLatest) => Promise<void>) => {
      const id = ++latestChange.current;
      setQueuedChanges((n) => n + 1);
      const run = sessionChanges.current
        .then(() => change(() => id === latestChange.current))
        .finally(() => setQueuedChanges((n) => n - 1));
      // A failed change must not stop the ones queued after it.
      sessionChanges.current = run.catch(() => undefined);
      return run;
    },
    [],
  );

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
    (idToken: string) =>
      enqueue(async (isLatest) => {
        // A newer call replaces this sign-in before it starts: skip its cookie.
        if (!isLatest()) return;
        // UC prefers a Bearer header over the session cookie, so a leftover
        // pasted token would make the new provider session fail verification.
        clearToken();
        // An earlier exchange may have changed the cookie even if this one fails.
        clearIdentityQueries();
        try {
          await loginMutation.mutateAsync(idToken);
          // A newer call queued behind this one replaces the session; it
          // verifies and publishes its own user.
          if (!isLatest()) return;
          setAuthenticating(true);
          const user = await fetchCurrentUser();
          if (!isLatest()) return;
          if (!user) {
            throw new Error(
              "Login succeeded, but the session could not be verified.",
            );
          }
          queryClient.setQueryData(CURRENT_USER_QUERY_KEY, user);
        } catch (error) {
          if (isLatest()) throw error;
        } finally {
          if (isLatest()) setAuthenticating(false);
        }
      }),
    [clearIdentityQueries, enqueue, loginMutation, queryClient],
  );

  const signInWithAccessToken = useCallback(
    (accessToken: string) => {
      setAuthenticating(false);
      return enqueue(async (isLatest) => {
        if (!isLatest()) return;
        setToken(accessToken);
        clearIdentityQueries();
        try {
          const user = await fetchCurrentUser();
          if (!user) {
            throw new Error("Unity Catalog rejected this access token.");
          }
          if (isLatest()) {
            queryClient.setQueryData(CURRENT_USER_QUERY_KEY, user);
          }
        } catch (error) {
          // No queued change has run yet, so the rejected token is still ours.
          clearToken();
          clearIdentityQueries();
          if (isLatest()) throw error;
        }
      });
    },
    [clearIdentityQueries, enqueue, queryClient],
  );

  // Runs even when a newer call supersedes it, to end whatever session the
  // changes queued before it left behind.
  const logout = useCallback(() => {
    setAuthenticating(false);
    return enqueue(async () => {
      try {
        // Sent before the token is cleared: UC ends a pasted-token session only
        // when the request carries its Authorization header.
        await logoutMutation.mutateAsync();
      } finally {
        clearToken();
        clearIdentityQueries();
      }
    });
  }, [clearIdentityQueries, enqueue, logoutMutation]);

  const value = useMemo<AuthContextValue>(
    () => ({
      authEnabled,
      loading:
        queuedChanges > 0 ||
        configLoading ||
        ((authEnabled || token.length > 0) && userLoading),
      currentUser: queuedChanges > 0 ? null : (currentUser ?? null),
      hasAccessToken: token.length > 0,
      authenticating,
      loginWithToken,
      signInWithAccessToken,
      logout,
    }),
    [
      authEnabled,
      configLoading,
      queuedChanges,
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
