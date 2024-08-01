import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { UC_API_PREFIX } from '../utils/constants';

export interface CatalogInterface {
  id: string;
  name: string;
  comment: string;
  created_at: number;
  updated_at: number | null;
}

interface ListCatalogsResponse {
  catalogs: CatalogInterface[];
  next_page_token: string | null;
}

// Fetch the list of catalogs
export function useListCatalogs() {
  return useQuery<ListCatalogsResponse>({
    queryKey: ['listCatalogs'],
    queryFn: async () => {
      const response = await fetch(`${UC_API_PREFIX}/catalogs`);
      if (!response.ok) {
        throw new Error('Failed to fetch catalogs');
      }
      return response.json();
    },
  });
}

interface GetCatalogParams {
  catalog: string;
}

// Fetch a single catalog by name
export function useGetCatalog({ catalog }: GetCatalogParams) {
  return useQuery<CatalogInterface>({
    queryKey: ['getCatalog', catalog],
    queryFn: async () => {
      const response = await fetch(`${UC_API_PREFIX}/catalogs/${catalog}`);
      if (!response.ok) {
        throw new Error('Failed to fetch catalog');
      }
      return response.json();
    },
  });
}

export interface CreateCatalogMutationParams
  extends Pick<CatalogInterface, 'name' | 'comment'> {}

interface CreateCatalogParams {
  onSuccessCallback?: (catalog: CatalogInterface) => void;
}

// Create a new catalog
export function useCreateCatalog({
  onSuccessCallback,
}: CreateCatalogParams = {}) {
  const queryClient = useQueryClient();

  return useMutation<CatalogInterface, unknown, CreateCatalogMutationParams>({
    mutationFn: async (params: CreateCatalogMutationParams) => {
      const response = await fetch(`${UC_API_PREFIX}/catalogs`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(params),
      });
      if (!response.ok) {
        // TODO: Expose error message
        throw new Error('Failed to create catalog');
      }
      return response.json();
    },
    onSuccess: (catalog) => {
      queryClient.invalidateQueries({
        queryKey: ['listCatalogs'],
      });
      onSuccessCallback?.(catalog);
    },
  });
}

export interface DeleteCatalogMutationParams
  extends Pick<CatalogInterface, 'name'> {}

interface DeleteCatalogParams {
  onSuccessCallback?: () => void;
}

// Delete a catalog
export function useDeleteCatalog({
  onSuccessCallback,
}: DeleteCatalogParams = {}) {
  const queryClient = useQueryClient();

  return useMutation<void, Error, DeleteCatalogMutationParams>({
    mutationFn: async (params: DeleteCatalogMutationParams) => {
      const response = await fetch(`${UC_API_PREFIX}/catalogs/${params.name}`, {
        method: 'DELETE',
      });
      if (!response.ok) {
        const errorData = await response.json();
        throw new Error(errorData.message || 'Failed to delete catalog');
      }
    },
    onSuccess: () => {
      queryClient.invalidateQueries({
        queryKey: ['listCatalogs'],
      });
      onSuccessCallback?.();
    },
  });
}
