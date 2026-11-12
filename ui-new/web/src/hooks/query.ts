import type { DescMessage, DescMethodUnary } from "@bufbuild/protobuf";
import { createConnectQueryKey } from "@connectrpc/connect-query";
import { useQueryClient } from "@tanstack/react-query";

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
