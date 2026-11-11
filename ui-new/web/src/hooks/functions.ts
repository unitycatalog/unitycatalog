import { createClient } from "@connectrpc/connect";
import {
  createConnectQueryKey,
  useMutation,
  useQuery,
  useTransport,
} from "@connectrpc/connect-query";
import { useQuery as useTanstackQuery } from "@tanstack/react-query";
import { FunctionService } from "@/gen/uc/v1/function_pb";
import { collectAllPages, useInvalidateMethod } from "@/hooks/query";

export function useListFunctions(
  catalogName: string,
  schemaName: string,
  enabled = true,
) {
  const transport = useTransport();
  const client = createClient(FunctionService, transport);
  const input = { schema: { catalogName, name: schemaName } };
  return useTanstackQuery({
    queryKey: createConnectQueryKey({
      schema: FunctionService.method.listFunctions,
      input,
      transport,
      cardinality: "finite",
    }),
    enabled: enabled && !!catalogName && !!schemaName,
    queryFn: () =>
      collectAllPages(
        (pageToken) =>
          client.listFunctions({
            ...input,
            page: pageToken ? { pageToken } : undefined,
          }),
        "functions",
      ),
  });
}

export function useGetFunction(fullName: string) {
  const [catalogName = "", schemaName = "", name = ""] = fullName.split(".");
  return useQuery(
    FunctionService.method.getFunction,
    { function: { catalogName, schemaName, name } },
    {
      enabled: !!catalogName && !!schemaName && !!name,
      select: (response) => response.function,
    },
  );
}

export function useCreateFunction() {
  const invalidateList = useInvalidateMethod(
    FunctionService.method.listFunctions,
  );
  return useMutation(FunctionService.method.createFunction, {
    onSuccess: invalidateList,
  });
}

export function useDeleteFunction() {
  const invalidateList = useInvalidateMethod(
    FunctionService.method.listFunctions,
  );
  return useMutation(FunctionService.method.deleteFunction, {
    onSuccess: invalidateList,
  });
}
