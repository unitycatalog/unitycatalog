import { createClient } from "@connectrpc/connect";
import {
  createConnectQueryKey,
  useMutation,
  useQuery,
  useTransport,
} from "@connectrpc/connect-query";
import { useQuery as useTanstackQuery } from "@tanstack/react-query";
import { ViewService } from "@/gen/uc/v1/view_pb";
import { collectAllPages, useInvalidateMethod } from "@/hooks/query";

export function useListViews(
  catalogName: string,
  schemaName: string,
  enabled = true,
) {
  const transport = useTransport();
  const client = createClient(ViewService, transport);
  const input = { schema: { catalogName, name: schemaName } };
  return useTanstackQuery({
    queryKey: createConnectQueryKey({
      schema: ViewService.method.listViews,
      input,
      transport,
      cardinality: "finite",
    }),
    enabled: enabled && !!catalogName && !!schemaName,
    queryFn: () =>
      collectAllPages(
        (pageToken) =>
          client.listViews({
            ...input,
            page: pageToken ? { pageToken } : undefined,
          }),
        (response) => response.views,
        (response) => response.page?.nextPageToken ?? "",
        (response, views) => ({
          ...response,
          views,
          page: response.page
            ? { ...response.page, nextPageToken: "" }
            : undefined,
        }),
      ),
  });
}

export function useGetView(fullName: string) {
  const [catalogName = "", schemaName = "", name = ""] = fullName.split(".");
  return useQuery(
    ViewService.method.getView,
    { view: { catalogName, schemaName, name } },
    {
      enabled: !!catalogName && !!schemaName && !!name,
      select: (response) => response.view,
    },
  );
}

export function useCreateView() {
  const invalidateList = useInvalidateMethod(ViewService.method.listViews);
  return useMutation(ViewService.method.createView, {
    onSuccess: invalidateList,
  });
}

export function useDeleteView() {
  const invalidateList = useInvalidateMethod(ViewService.method.listViews);
  return useMutation(ViewService.method.deleteView, {
    onSuccess: invalidateList,
  });
}
