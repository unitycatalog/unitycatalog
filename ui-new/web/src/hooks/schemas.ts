import { useUcQuery } from "@/lib/ucQuery";
import { UC_API_PREFIX } from "@/lib/uc";
import type { ListSchemasResponse, SchemaInfo } from "@/lib/types";

export function useListSchemas(catalogName: string) {
  return useUcQuery<ListSchemasResponse>("GET", `${UC_API_PREFIX}/schemas`, {
    query: { catalog_name: catalogName },
    queryOptions: { enabled: !!catalogName },
  });
}

export function useGetSchema(fullName: string) {
  return useUcQuery<SchemaInfo>("GET", `${UC_API_PREFIX}/schemas/${encodeURIComponent(fullName)}`, {
    queryOptions: { enabled: !!fullName },
  });
}
