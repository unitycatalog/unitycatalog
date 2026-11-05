import { useUcQuery } from "@/lib/ucQuery";
import { UC_API_PREFIX } from "@/lib/uc";
import type { ListVolumesResponse, VolumeInfo } from "@/lib/types";

export function useListVolumes(catalogName: string, schemaName: string) {
  return useUcQuery<ListVolumesResponse>("GET", `${UC_API_PREFIX}/volumes`, {
    query: { catalog_name: catalogName, schema_name: schemaName },
    queryOptions: { enabled: !!catalogName && !!schemaName },
  });
}

export function useGetVolume(fullName: string) {
  return useUcQuery<VolumeInfo>("GET", `${UC_API_PREFIX}/volumes/${encodeURIComponent(fullName)}`, {
    queryOptions: { enabled: !!fullName },
  });
}
