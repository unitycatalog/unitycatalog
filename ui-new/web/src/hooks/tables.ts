import { useUcQuery } from "@/lib/ucQuery";
import { UC_API_PREFIX, withListTablesQuery } from "@/lib/uc";
import type { ListTablesResponse, TableInfo } from "@/lib/types";

export function useListTables(catalogName: string, schemaName: string) {
  return useUcQuery<ListTablesResponse>("GET", `${UC_API_PREFIX}/tables`, {
    query: withListTablesQuery({ catalog_name: catalogName, schema_name: schemaName }),
    queryOptions: { enabled: !!catalogName && !!schemaName },
  });
}

export function useGetTable(fullName: string) {
  return useUcQuery<TableInfo>("GET", `${UC_API_PREFIX}/tables/${encodeURIComponent(fullName)}`, {
    queryOptions: { enabled: !!fullName },
  });
}
