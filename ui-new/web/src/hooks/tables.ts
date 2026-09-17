import { createClient } from "@connectrpc/connect";
import {
  createConnectQueryKey,
  useMutation,
  useQuery,
  useTransport,
} from "@connectrpc/connect-query";
import { useQuery as useTanstackQuery } from "@tanstack/react-query";
import { TableService } from "@/gen/uc/v1/table_pb";
import { collectAllPages, useInvalidateMethod } from "@/hooks/query";

export function useListTables(
  catalogName: string,
  schemaName: string,
  enabled = true,
) {
  const transport = useTransport();
  const client = createClient(TableService, transport);
  const input = { schema: { catalogName, name: schemaName } };
  return useTanstackQuery({
    queryKey: createConnectQueryKey({
      schema: TableService.method.listTables,
      input,
      transport,
      cardinality: "finite",
    }),
    enabled: enabled && !!catalogName && !!schemaName,
    queryFn: () =>
      collectAllPages(
        (pageToken) =>
          client.listTables({
            ...input,
            page: pageToken ? { pageToken } : undefined,
          }),
        (response) => response.tables,
        (response) => response.page?.nextPageToken ?? "",
        (response, tables) => ({
          ...response,
          tables,
          page: response.page
            ? { ...response.page, nextPageToken: "" }
            : undefined,
        }),
      ),
  });
}

export function useGetTable(fullName: string) {
  const [catalogName = "", schemaName = "", name = ""] = fullName.split(".");
  return useQuery(
    TableService.method.getTable,
    { table: { catalogName, schemaName, name } },
    {
      enabled: !!catalogName && !!schemaName && !!name,
      select: (response) => response.table,
    },
  );
}

export function useCreateTable() {
  const invalidateList = useInvalidateMethod(TableService.method.listTables);
  return useMutation(TableService.method.createTable, {
    onSuccess: invalidateList,
  });
}

export function useDeleteTable() {
  const invalidateList = useInvalidateMethod(TableService.method.listTables);
  return useMutation(TableService.method.deleteTable, {
    onSuccess: invalidateList,
  });
}
