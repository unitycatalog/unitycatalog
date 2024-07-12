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

export function useListCatalogs() {
  return useQuery<ListCatalogsResponse>({
    queryKey: ['listCatalogs'],
    queryFn: async () => {
      const response = await fetch(`${UC_API_PREFIX}/catalogs`);
      return response.json();
    },
  });
}

interface GetCatalogParams {
  catalog: string;
}

export function useGetCatalog({ catalog }: GetCatalogParams) {
  return useQuery<CatalogInterface>({
    queryKey: ['getCatalog', catalog],
    queryFn: async () => {
      const response = await fetch(`${UC_API_PREFIX}/catalogs/${catalog}`);
      return response.json();
    },
  });
}

export interface CreateCatalogMutationParams
  extends Pick<CatalogInterface, 'name' | 'comment'> {}

interface CreateCatalogParams {
  onSuccessCallback?: (catalog: CatalogInterface) => void;
}

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
