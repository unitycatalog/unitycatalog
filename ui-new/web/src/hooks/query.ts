import type { DescMessage, DescMethodUnary } from "@bufbuild/protobuf";
import { createConnectQueryKey } from "@connectrpc/connect-query";
import { useQueryClient } from "@tanstack/react-query";

export async function collectAllPages<Response, Item>(
  fetchPage: (pageToken: string) => Promise<Response>,
  getItems: (response: Response) => readonly Item[],
  getNextPageToken: (response: Response) => string,
  mergeItems: (first: Response, items: Item[]) => Response,
): Promise<Response> {
  const seen = new Set<string>();
  const first = await fetchPage("");
  const items = [...getItems(first)];
  let response = first;
  let pageToken = getNextPageToken(response);

  while (pageToken) {
    if (seen.has(pageToken)) {
      throw new Error("Unity Catalog returned a repeated page token");
    }
    seen.add(pageToken);
    response = await fetchPage(pageToken);
    items.push(...getItems(response));
    pageToken = getNextPageToken(response);
  }

  return mergeItems(first, items);
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
