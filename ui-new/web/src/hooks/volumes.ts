import { createClient } from "@connectrpc/connect";
import {
  createConnectQueryKey,
  useMutation,
  useQuery,
  useTransport,
} from "@connectrpc/connect-query";
import { useQuery as useTanstackQuery } from "@tanstack/react-query";
import { VolumeService } from "@/gen/uc/v1/volume_pb";
import { collectAllPages, useInvalidateMethod } from "@/hooks/query";

export function useListVolumes(
  catalogName: string,
  schemaName: string,
  enabled = true,
) {
  const transport = useTransport();
  const client = createClient(VolumeService, transport);
  const input = { schema: { catalogName, name: schemaName } };
  return useTanstackQuery({
    queryKey: createConnectQueryKey({
      schema: VolumeService.method.listVolumes,
      input,
      transport,
      cardinality: "finite",
    }),
    enabled: enabled && !!catalogName && !!schemaName,
    queryFn: () =>
      collectAllPages(
        (pageToken) =>
          client.listVolumes({
            ...input,
            page: pageToken ? { pageToken } : undefined,
          }),
        "volumes",
      ),
  });
}

export function useGetVolume(fullName: string) {
  const [catalogName = "", schemaName = "", name = ""] = fullName.split(".");
  return useQuery(
    VolumeService.method.getVolume,
    { volume: { catalogName, schemaName, name } },
    {
      enabled: !!catalogName && !!schemaName && !!name,
      select: (response) => response.volume,
    },
  );
}

export function useCreateVolume() {
  const invalidateList = useInvalidateMethod(VolumeService.method.listVolumes);
  return useMutation(VolumeService.method.createVolume, {
    onSuccess: invalidateList,
  });
}

export function useUpdateVolume() {
  const invalidateList = useInvalidateMethod(VolumeService.method.listVolumes);
  const invalidateDetail = useInvalidateMethod(VolumeService.method.getVolume);
  return useMutation(VolumeService.method.updateVolume, {
    onSuccess: (_response, request) => {
      invalidateList();
      if (!request.newName) invalidateDetail();
    },
  });
}

export function useDeleteVolume() {
  const invalidateList = useInvalidateMethod(VolumeService.method.listVolumes);
  return useMutation(VolumeService.method.deleteVolume, {
    onSuccess: invalidateList,
  });
}
