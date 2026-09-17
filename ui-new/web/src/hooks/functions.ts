import { useUcQuery } from "@/lib/ucQuery";
import { UC_API_PREFIX } from "@/lib/uc";
import type { FunctionInfo, ListFunctionsResponse } from "@/lib/types";

export function useListFunctions(catalogName: string, schemaName: string) {
  return useUcQuery<ListFunctionsResponse>("GET", `${UC_API_PREFIX}/functions`, {
    query: { catalog_name: catalogName, schema_name: schemaName },
    queryOptions: { enabled: !!catalogName && !!schemaName },
  });
}

export function useGetFunction(fullName: string) {
  return useUcQuery<FunctionInfo>("GET", `${UC_API_PREFIX}/functions/${encodeURIComponent(fullName)}`, {
    queryOptions: { enabled: !!fullName },
  });
}
