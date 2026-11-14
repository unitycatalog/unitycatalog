import { createClient } from "@connectrpc/connect";
import {
  createConnectQueryKey,
  useMutation,
  useQuery,
  useTransport,
} from "@connectrpc/connect-query";
import { useQuery as useTanstackQuery } from "@tanstack/react-query";
import { FunctionService } from "@/gen/uc/v1/function_pb";
import { ModelService } from "@/gen/uc/v1/model_pb";
import { SchemaService } from "@/gen/uc/v1/schema_pb";
import { TableService } from "@/gen/uc/v1/table_pb";
import { ViewService } from "@/gen/uc/v1/view_pb";
import { VolumeService } from "@/gen/uc/v1/volume_pb";
import { splitFullName } from "@/lib/uc";
import {
  collectAllPages,
  useInvalidateMethod,
  useMarkDescendantsStale,
} from "@/hooks/query";

// The services whose objects live in a schema.
export const SCHEMA_OBJECT_SERVICES = [
  TableService,
  ViewService,
  VolumeService,
  FunctionService,
  ModelService,
] as const;

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
    queryFn: ({ signal }) =>
      collectAllPages(
        (pageToken, requestSignal) =>
          client.listSchemas(
            {
              ...input,
              page: pageToken ? { pageToken } : undefined,
            },
            { signal: requestSignal },
          ),
        "schemas",
        signal,
      ),
  });
}

export function useGetSchema(fullName: string) {
  const [catalogName, name] = splitFullName(fullName);
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
  const markDetailStale = useInvalidateMethod(SchemaService.method.getSchema, {
    refetch: false,
  });
  const markDescendantsStale = useMarkDescendantsStale(SCHEMA_OBJECT_SERVICES);
  return useMutation(SchemaService.method.updateSchema, {
    onSuccess: (_response, request) => {
      invalidateList();
      if (request.newName) {
        markDetailStale();
        markDescendantsStale();
      } else {
        invalidateDetail();
      }
    },
  });
}

export function useDeleteSchema() {
  const invalidateList = useInvalidateMethod(SchemaService.method.listSchemas);
  const markDetailStale = useInvalidateMethod(SchemaService.method.getSchema, {
    refetch: false,
  });
  const markDescendantsStale = useMarkDescendantsStale(SCHEMA_OBJECT_SERVICES);
  return useMutation(SchemaService.method.deleteSchema, {
    onSuccess: () => {
      invalidateList();
      markDetailStale();
      markDescendantsStale();
    },
  });
}
