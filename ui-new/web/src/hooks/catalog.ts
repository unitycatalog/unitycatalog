import { useUcQuery } from "@/lib/ucQuery";
import { UC_API_PREFIX } from "@/lib/uc";
import type { CatalogInfo, ListCatalogsResponse } from "@/lib/types";

export function useListCatalogs() {
  return useUcQuery<ListCatalogsResponse>("GET", `${UC_API_PREFIX}/catalogs`);
}

export function useGetCatalog(name: string) {
  return useUcQuery<CatalogInfo>("GET", `${UC_API_PREFIX}/catalogs/${encodeURIComponent(name)}`, {
    queryOptions: { enabled: !!name },
  });
}
