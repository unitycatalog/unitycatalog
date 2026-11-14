import type {
  DescMessage,
  DescMethodUnary,
  DescService,
} from "@bufbuild/protobuf";
import { createConnectQueryKey } from "@connectrpc/connect-query";
import { useQueryClient } from "@tanstack/react-query";
import { useMarkPermissionsStale } from "@/hooks/permissions";
import { markStale } from "@/lib/queryClient";

export async function collectAllPages<
  Key extends PropertyKey,
  Response extends { page?: { nextPageToken: string } } & Record<
    Key,
    readonly unknown[]
  >,
>(
  fetchPage: (pageToken: string, signal: AbortSignal) => Promise<Response>,
  itemKey: Key,
  signal: AbortSignal,
): Promise<Response> {
  const seen = new Set<string>();
  signal.throwIfAborted();
  const first = await fetchPage("", signal);
  signal.throwIfAborted();
  const items = [...(first[itemKey] as readonly unknown[])];
  let response = first;
  let pageToken = response.page?.nextPageToken ?? "";

  while (pageToken) {
    signal.throwIfAborted();
    if (seen.has(pageToken)) {
      throw new Error("Unity Catalog returned a repeated page token");
    }
    seen.add(pageToken);
    response = await fetchPage(pageToken, signal);
    signal.throwIfAborted();
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
// method stale and refetches active queries. With `refetch: false`, only reads
// already in flight are restarted. Use that for a deleted or renamed entity,
// whose old name would fail to load while its page navigates away, but must not
// be served from cache on a later visit.
export function useInvalidateMethod<
  Input extends DescMessage,
  Output extends DescMessage,
>(method: DescMethodUnary<Input, Output>, { refetch = true } = {}) {
  const queryClient = useQueryClient();
  return () => {
    const queryKey = createConnectQueryKey({
      schema: method,
      cardinality: undefined,
    });
    void markStale(queryClient, { queryKey }, { refetchActive: refetch });
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
      markStale(queryClient, {
        // The branches are identical, but each createConnectQueryKey overload
        // takes only a service or only a method, so the union must be narrowed.
        queryKey:
          schema.kind === "service"
            ? createConnectQueryKey({ schema, cardinality: undefined })
            : createConnectQueryKey({ schema, cardinality: undefined }),
      });
    }
    markPermissionsStale();
  };
}
