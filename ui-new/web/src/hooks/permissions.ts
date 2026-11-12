import {
  createConnectQueryKey,
  type ConnectQueryKey,
} from "@connectrpc/connect-query";
import { useQueryClient } from "@tanstack/react-query";
import { UnityProxyService } from "@/gen/uc/v1/proxy_pb";
import { UC_API_PREFIX } from "@/lib/uc";
import { useUcQuery } from "@/lib/ucQuery";

export type SecurableType =
  "catalog" | "schema" | "table" | "volume" | "function" | "registered_model";

type PrivilegeAssignment = { principal: string; privileges: string[] };
type PermissionsResponse = { privilege_assignments?: PrivilegeAssignment[] };

const PERMISSIONS_PATH = `${UC_API_PREFIX}/permissions/`;

function permissionsPath(securableType: SecurableType, fullName: string) {
  return `${PERMISSIONS_PATH}${securableType}/${encodeURIComponent(fullName)}`;
}

// usePermissions reads the privilege assignments on a securable through the
// generic proxy: GET /permissions/{type}/{full_name}.
export function usePermissions(securableType: SecurableType, fullName: string) {
  return useUcQuery<PermissionsResponse>(
    "GET",
    permissionsPath(securableType, fullName),
    { queryOptions: { enabled: !!fullName } },
  );
}

// useMarkPermissionsStale returns a callback that marks cached permission
// reads stale without refetching: one securable's, or every securable's when
// called without one. Call it after a delete or rename, so an object created
// later under the old name does not show the old object's grants.
export function useMarkPermissionsStale() {
  const queryClient = useQueryClient();
  return (securable?: { type: SecurableType; fullName: string }) => {
    const matches = securable
      ? (path: string) =>
          path === permissionsPath(securable.type, securable.fullName)
      : (path: string) => path.startsWith(PERMISSIONS_PATH);
    void queryClient.invalidateQueries({
      queryKey: createConnectQueryKey({
        schema: UnityProxyService.method.call,
        cardinality: undefined,
      }),
      predicate: ({ queryKey }) => {
        const input = (queryKey as ConnectQueryKey)[1].input;
        return (
          typeof input === "object" &&
          typeof input.path === "string" &&
          matches(input.path)
        );
      },
      refetchType: "none",
    });
  };
}
