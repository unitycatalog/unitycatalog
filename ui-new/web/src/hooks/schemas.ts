import { createClient } from "@connectrpc/connect";
import {
  createConnectQueryKey,
  useMutation,
  useQuery,
  useTransport,
} from "@connectrpc/connect-query";
import { useQuery as useTanstackQuery } from "@tanstack/react-query";
import { SchemaService } from "@/gen/uc/v1/schema_pb";
import { collectAllPages, useInvalidateMethod } from "@/hooks/query";

export function useListSchemas(catalogName: string, enabled = true) {
  const transport = useTransport();
  const client = createClient(SchemaService, transport);
  const input = { catalog: { name: catalogName } };
  return useTanstackQuery({
    queryKey: createConnectQueryKey({
      schema: SchemaService.method.listSchemas,
      input,
      transport,
      cardinality: "finite",
    }),
    enabled: enabled && !!catalogName,
    queryFn: () =>
      collectAllPages(
        (pageToken) =>
          client.listSchemas({
            ...input,
            page: pageToken ? { pageToken } : undefined,
          }),
        "schemas",
      ),
  });
}

export function useGetSchema(fullName: string) {
  const [catalogName = "", name = ""] = fullName.split(".");
  return useQuery(
    SchemaService.method.getSchema,
    { schema: { catalogName, name } },
    {
      enabled: !!catalogName && !!name,
      select: (response) => response.schema,
    },
  );
}

export function useCreateSchema() {
  const invalidateList = useInvalidateMethod(SchemaService.method.listSchemas);
  return useMutation(SchemaService.method.createSchema, {
    onSuccess: invalidateList,
  });
}

export function useUpdateSchema() {
  const invalidateList = useInvalidateMethod(SchemaService.method.listSchemas);
  const invalidateDetail = useInvalidateMethod(SchemaService.method.getSchema);
  return useMutation(SchemaService.method.updateSchema, {
    onSuccess: (_response, request) => {
      invalidateList();
      if (!request.newName) invalidateDetail();
    },
  });
}

export function useDeleteSchema() {
  const invalidateList = useInvalidateMethod(SchemaService.method.listSchemas);
  return useMutation(SchemaService.method.deleteSchema, {
    onSuccess: invalidateList,
  });
}
