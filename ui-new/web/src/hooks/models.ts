import { useUcQuery } from "@/lib/ucQuery";
import { UC_API_PREFIX } from "@/lib/uc";
import type {
  ListModelsResponse,
  ListModelVersionsResponse,
  ModelInfo,
  ModelVersionInfo,
} from "@/lib/types";

export function useListModels(catalogName: string, schemaName: string) {
  return useUcQuery<ListModelsResponse>("GET", `${UC_API_PREFIX}/models`, {
    query: { catalog_name: catalogName, schema_name: schemaName },
    queryOptions: { enabled: !!catalogName && !!schemaName },
  });
}

export function useGetModel(fullName: string) {
  return useUcQuery<ModelInfo>("GET", `${UC_API_PREFIX}/models/${encodeURIComponent(fullName)}`, {
    queryOptions: { enabled: !!fullName },
  });
}

export function useListModelVersions(fullName: string) {
  return useUcQuery<ListModelVersionsResponse>(
    "GET",
    `${UC_API_PREFIX}/models/${encodeURIComponent(fullName)}/versions`,
    { queryOptions: { enabled: !!fullName } },
  );
}

export function useGetModelVersion(fullName: string, version: number | string) {
  return useUcQuery<ModelVersionInfo>(
    "GET",
    `${UC_API_PREFIX}/models/${encodeURIComponent(fullName)}/versions/${version}`,
    { queryOptions: { enabled: !!fullName && version !== undefined && version !== "" } },
  );
}
