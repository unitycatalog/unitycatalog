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

export function useInvalidateMethod<
  Input extends DescMessage,
  Output extends DescMessage,
>(method: DescMethodUnary<Input, Output>) {
  const queryClient = useQueryClient();
  return () => {
    void queryClient.invalidateQueries({
      queryKey: createConnectQueryKey({
        schema: method,
        cardinality: undefined,
      }),
    });
  };
}
