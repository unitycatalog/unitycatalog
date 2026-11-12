import { createClient } from "@connectrpc/connect";
import {
  createConnectQueryKey,
  useMutation,
  useQuery,
  useTransport,
} from "@connectrpc/connect-query";
import { useQuery as useTanstackQuery } from "@tanstack/react-query";
import { ViewService } from "@/gen/uc/v1/view_pb";
import { objectFullName, splitFullName } from "@/lib/uc";
import { collectAllPages, useInvalidateMethod } from "@/hooks/query";
import { useMarkPermissionsStale } from "@/hooks/permissions";

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
        "views",
      ),
  });
}

export function useGetView(fullName: string) {
  const [catalogName, schemaName, name] = splitFullName(fullName);
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
  const markDetailStale = useInvalidateMethod(ViewService.method.getView, {
    refetch: false,
  });
  const markPermissionsStale = useMarkPermissionsStale();
  return useMutation(ViewService.method.deleteView, {
    onSuccess: (_response, request) => {
      invalidateList();
      markDetailStale();
      markPermissionsStale({
        type: "table",
        fullName: objectFullName(request.view),
      });
    },
  });
}
