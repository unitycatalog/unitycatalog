import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { ucCall, ucJson, UC_AUTH_API_PREFIX } from "@/lib/uc";

// A SCIM 2.0 user resource, as returned by GET /scim2/Me. Typed to just the
// fields the UI renders; the endpoint returns more.
export interface UserInterface {
  id?: string;
  userName?: string;
  displayName?: string;
  active?: boolean;
  emails?: { value: string; primary?: boolean }[];
}

export const CURRENT_USER_QUERY_KEY = ["getCurrentUser"] as const;

// useGetCurrentUser fetches the caller's identity from the control-plane SCIM
// endpoint. A 401 means "not signed in" and resolves to null (not an error) so
// the auth gate can redirect to /login without surfacing a failure.
export function useGetCurrentUser(enabled = true) {
  return useQuery<UserInterface | null>({
    queryKey: CURRENT_USER_QUERY_KEY,
    enabled,
    retry: false,
    queryFn: async () => {
      const res = await ucCall("GET", `${UC_AUTH_API_PREFIX}/scim2/Me`);
      if (res.httpStatus === 401) return null;
      if (!res.ok) throw new Error(`Failed to fetch user (HTTP ${res.httpStatus})`);
      return res.body ? (JSON.parse(res.body) as UserInterface) : null;
    },
  });
}

// The subset of GrantType / TokenType values the token-exchange login uses.
// Mirrors the current ui's control.gen enums without pulling the whole spec.
const GRANT_TYPE_TOKEN_EXCHANGE = "urn:ietf:params:oauth:grant-type:token-exchange";
const TOKEN_TYPE_ACCESS = "urn:ietf:params:oauth:token-type:access_token";
const TOKEN_TYPE_ID = "urn:ietf:params:oauth:token-type:id_token";

// useLoginWithToken exchanges a provider id_token (e.g. Google) for a UC session
// cookie via POST /auth/tokens with ext=cookie. The endpoint requires
// application/x-www-form-urlencoded; the bridge propagates the Set-Cookie back
// to the browser so subsequent same-origin calls carry the session.
export function useLoginWithToken() {
  const queryClient = useQueryClient();
  return useMutation<void, Error, string>({
    mutationFn: async (idToken: string) => {
      const form = new URLSearchParams({
        grant_type: GRANT_TYPE_TOKEN_EXCHANGE,
        requested_token_type: TOKEN_TYPE_ACCESS,
        subject_token_type: TOKEN_TYPE_ID,
        subject_token: idToken,
      }).toString();
      const res = await ucCall("POST", `${UC_AUTH_API_PREFIX}/auth/tokens`, {
        query: { ext: "cookie" },
        body: form,
        contentType: "application/x-www-form-urlencoded",
      });
      if (!res.ok) throw new Error(`Login failed (HTTP ${res.httpStatus})`);
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: CURRENT_USER_QUERY_KEY });
    },
  });
}

// useLogoutCurrentUser clears the session cookie via POST /auth/logout.
export function useLogoutCurrentUser() {
  const queryClient = useQueryClient();
  return useMutation<void, Error, void>({
    mutationFn: async () => {
      await ucJson("POST", `${UC_AUTH_API_PREFIX}/auth/logout`);
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: CURRENT_USER_QUERY_KEY });
    },
  });
}
