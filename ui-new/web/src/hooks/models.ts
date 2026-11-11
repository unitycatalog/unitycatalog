import { createClient } from "@connectrpc/connect";
import {
  createConnectQueryKey,
  useMutation,
  useQuery,
  useTransport,
} from "@connectrpc/connect-query";
import { useQuery as useTanstackQuery } from "@tanstack/react-query";
import { ModelService } from "@/gen/uc/v1/model_pb";
import { collectAllPages, useInvalidateMethod } from "@/hooks/query";

export function useListModels(
  catalogName: string,
  schemaName: string,
  enabled = true,
) {
  const transport = useTransport();
  const client = createClient(ModelService, transport);
  const input = { schema: { catalogName, name: schemaName } };
  return useTanstackQuery({
    queryKey: createConnectQueryKey({
      schema: ModelService.method.listRegisteredModels,
      input,
      transport,
      cardinality: "finite",
    }),
    enabled: enabled && !!catalogName && !!schemaName,
    queryFn: () =>
      collectAllPages(
        (pageToken) =>
          client.listRegisteredModels({
            ...input,
            page: pageToken ? { pageToken } : undefined,
          }),
        "models",
      ),
  });
}

export function useGetModel(fullName: string) {
  const [catalogName = "", schemaName = "", name = ""] = fullName.split(".");
  return useQuery(
    ModelService.method.getRegisteredModel,
    { model: { catalogName, schemaName, name } },
    {
      enabled: !!catalogName && !!schemaName && !!name,
      select: (response) => response.model,
    },
  );
}

export function useListModelVersions(fullName: string) {
  const [catalogName = "", schemaName = "", name = ""] = fullName.split(".");
  const transport = useTransport();
  const client = createClient(ModelService, transport);
  const input = { model: { catalogName, schemaName, name } };
  return useTanstackQuery({
    queryKey: createConnectQueryKey({
      schema: ModelService.method.listModelVersions,
      input,
      transport,
      cardinality: "finite",
    }),
    enabled: !!catalogName && !!schemaName && !!name,
    queryFn: () =>
      collectAllPages(
        (pageToken) =>
          client.listModelVersions({
            ...input,
            page: pageToken ? { pageToken } : undefined,
          }),
        "versions",
      ),
  });
}

export function useGetModelVersion(fullName: string, version: number | string) {
  const [catalogName = "", schemaName = "", name = ""] = fullName.split(".");
  const parsedVersion = parseModelVersion(version);
  return useQuery(
    ModelService.method.getModelVersion,
    {
      modelVersion: {
        model: { catalogName, schemaName, name },
        version: parsedVersion ?? 0n,
      },
    },
    {
      enabled:
        !!catalogName && !!schemaName && !!name && parsedVersion !== undefined,
      select: (response) => response.modelVersion,
    },
  );
}

export function parseModelVersion(version: number | string) {
  try {
    const parsed = BigInt(version);
    return parsed > 0n ? parsed : undefined;
  } catch {
    return undefined;
  }
}

export function useCreateModel() {
  const invalidateList = useInvalidateMethod(
    ModelService.method.listRegisteredModels,
  );
  return useMutation(ModelService.method.createRegisteredModel, {
    onSuccess: invalidateList,
  });
}

export function useUpdateModel() {
  const invalidateList = useInvalidateMethod(
    ModelService.method.listRegisteredModels,
  );
  const invalidateDetail = useInvalidateMethod(
    ModelService.method.getRegisteredModel,
  );
  return useMutation(ModelService.method.updateRegisteredModel, {
    onSuccess: (_response, request) => {
      invalidateList();
      if (!request.newName) invalidateDetail();
    },
  });
}

export function useDeleteModel() {
  const invalidateList = useInvalidateMethod(
    ModelService.method.listRegisteredModels,
  );
  return useMutation(ModelService.method.deleteRegisteredModel, {
    onSuccess: invalidateList,
  });
}

export function useCreateModelVersion() {
  const invalidateList = useInvalidateMethod(
    ModelService.method.listModelVersions,
  );
  return useMutation(ModelService.method.createModelVersion, {
    onSuccess: invalidateList,
  });
}

export function useUpdateModelVersion() {
  const invalidateList = useInvalidateMethod(
    ModelService.method.listModelVersions,
  );
  const invalidateDetail = useInvalidateMethod(
    ModelService.method.getModelVersion,
  );
  return useMutation(ModelService.method.updateModelVersion, {
    onSuccess: () => {
      invalidateList();
      invalidateDetail();
    },
  });
}

export function useFinalizeModelVersion() {
  const invalidateList = useInvalidateMethod(
    ModelService.method.listModelVersions,
  );
  const invalidateDetail = useInvalidateMethod(
    ModelService.method.getModelVersion,
  );
  return useMutation(ModelService.method.finalizeModelVersion, {
    onSuccess: () => {
      invalidateList();
      invalidateDetail();
    },
  });
}

export function useDeleteModelVersion() {
  const invalidateList = useInvalidateMethod(
    ModelService.method.listModelVersions,
  );
  return useMutation(ModelService.method.deleteModelVersion, {
    onSuccess: invalidateList,
  });
}
