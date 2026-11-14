import { createClient } from "@connectrpc/connect";
import {
  createConnectQueryKey,
  useMutation,
  useQuery,
  useTransport,
} from "@connectrpc/connect-query";
import { useQuery as useTanstackQuery } from "@tanstack/react-query";
import { FunctionService } from "@/gen/uc/v1/function_pb";
import { objectFullName, splitFullName } from "@/lib/uc";
import { collectAllPages, useInvalidateMethod } from "@/hooks/query";
import { useMarkPermissionsStale } from "@/hooks/permissions";

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
    queryFn: ({ signal }) =>
      collectAllPages(
        (pageToken, requestSignal) =>
          client.listFunctions(
            {
              ...input,
              page: pageToken ? { pageToken } : undefined,
            },
            { signal: requestSignal },
          ),
        "functions",
        signal,
      ),
  });
}

export function useGetFunction(fullName: string) {
  const [catalogName, schemaName, name] = splitFullName(fullName);
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
  const markDetailStale = useInvalidateMethod(
    FunctionService.method.getFunction,
    { refetch: false },
  );
  const markPermissionsStale = useMarkPermissionsStale();
  return useMutation(FunctionService.method.deleteFunction, {
    onSuccess: (_response, request) => {
      invalidateList();
      markDetailStale();
      markPermissionsStale({
        type: "function",
        fullName: objectFullName(request.function),
      });
    },
  });
}
