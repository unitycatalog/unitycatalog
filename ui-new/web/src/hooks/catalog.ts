import { createClient } from "@connectrpc/connect";
import {
  createConnectQueryKey,
  useMutation,
  useQuery,
  useTransport,
} from "@connectrpc/connect-query";
import { useQuery as useTanstackQuery } from "@tanstack/react-query";
import { CatalogService } from "@/gen/uc/v1/catalog_pb";
import { collectAllPages, useInvalidateMethod } from "@/hooks/query";

export function useListCatalogs() {
  const transport = useTransport();
  const client = createClient(CatalogService, transport);
  return useTanstackQuery({
    queryKey: createConnectQueryKey({
      schema: CatalogService.method.listCatalogs,
      input: {},
      transport,
      cardinality: "finite",
    }),
    queryFn: () =>
      collectAllPages(
        (pageToken) =>
          client.listCatalogs({
            page: pageToken ? { pageToken } : undefined,
          }),
        (response) => response.catalogs,
        (response) => response.page?.nextPageToken ?? "",
        (response, catalogs) => ({
          ...response,
          catalogs,
          page: response.page
            ? { ...response.page, nextPageToken: "" }
            : undefined,
        }),
      ),
  });
}

export function useGetCatalog(name: string) {
  return useQuery(
    CatalogService.method.getCatalog,
    { catalog: { name } },
    {
      enabled: !!name,
      select: (response) => response.catalog,
    },
  );
}

export function useCreateCatalog() {
  const invalidateList = useInvalidateMethod(
    CatalogService.method.listCatalogs,
  );
  return useMutation(CatalogService.method.createCatalog, {
    onSuccess: invalidateList,
  });
}

export function useUpdateCatalog() {
  const invalidateList = useInvalidateMethod(
    CatalogService.method.listCatalogs,
  );
  const invalidateDetail = useInvalidateMethod(
    CatalogService.method.getCatalog,
  );
  return useMutation(CatalogService.method.updateCatalog, {
    onSuccess: (_response, request) => {
      invalidateList();
      if (!request.newName) invalidateDetail();
    },
  });
}

export function useDeleteCatalog() {
  const invalidateList = useInvalidateMethod(
    CatalogService.method.listCatalogs,
  );
  return useMutation(CatalogService.method.deleteCatalog, {
    onSuccess: invalidateList,
  });
}
