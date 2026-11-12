import type {
  DescMessage,
  DescMethodUnary,
  DescService,
} from "@bufbuild/protobuf";
import { createConnectQueryKey } from "@connectrpc/connect-query";
import { useQueryClient } from "@tanstack/react-query";
import { useMarkPermissionsStale } from "@/hooks/permissions";

export async function collectAllPages<
  Key extends PropertyKey,
  Response extends { page?: { nextPageToken: string } } & Record<
    Key,
    readonly unknown[]
  >,
>(
  fetchPage: (pageToken: string) => Promise<Response>,
  itemKey: Key,
): Promise<Response> {
  const seen = new Set<string>();
  const first = await fetchPage("");
  const items = [...(first[itemKey] as readonly unknown[])];
  let response = first;
  let pageToken = response.page?.nextPageToken ?? "";

  while (pageToken) {
    if (seen.has(pageToken)) {
      throw new Error("Unity Catalog returned a repeated page token");
    }
    seen.add(pageToken);
    response = await fetchPage(pageToken);
    items.push(...(response[itemKey] as readonly unknown[]));
    pageToken = response.page?.nextPageToken ?? "";
  }

  return {
    ...first,
    [itemKey]: items,
    page: first.page ? { ...first.page, nextPageToken: "" } : undefined,
  } as Response;
}

// useInvalidateMethod returns a callback that marks every cached result of the
// method stale and refetches the ones on screen. With `refetch: false` it only
// marks them stale: use that for a deleted or renamed entity, whose old name
// would fail to load while its page navigates away, but must not be served
// from cache on a later visit.
export function useInvalidateMethod<
  Input extends DescMessage,
  Output extends DescMessage,
>(method: DescMethodUnary<Input, Output>, { refetch = true } = {}) {
  const queryClient = useQueryClient();
  return () => {
    void queryClient.invalidateQueries({
      queryKey: createConnectQueryKey({
        schema: method,
        cardinality: undefined,
      }),
      refetchType: refetch ? "active" : "none",
    });
  };
}

// useMarkDescendantsStale returns a callback for a renamed or deleted parent.
// Cached pages of its descendants, and its grants, sit at the old name, which
// no longer resolves. The callback marks every cached result of the given
// services or methods, plus every cached permissions read, stale without
// refetching. That is broader than the parent, but renames and parent deletes
// are rare, and a stale mark only costs a refetch on the next visit.
export function useMarkDescendantsStale(
  descendants: readonly (DescService | DescMethodUnary)[],
) {
  const queryClient = useQueryClient();
  const markPermissionsStale = useMarkPermissionsStale();
  return () => {
    for (const schema of descendants) {
      void queryClient.invalidateQueries({
        // The branches are identical, but each createConnectQueryKey overload
        // takes only a service or only a method, so the union must be narrowed.
        queryKey:
          schema.kind === "service"
            ? createConnectQueryKey({ schema, cardinality: undefined })
            : createConnectQueryKey({ schema, cardinality: undefined }),
        refetchType: "none",
      });
    }
    markPermissionsStale();
  };
}
