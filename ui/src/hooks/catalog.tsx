import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import apiClient from '../context/client';

export interface CatalogInterface {
  id: string;
  name: string;
  comment: string;
  created_at: number;
  updated_at: number | null;
  owner: string | null;
  created_by: string | null;
  updated_by: string | null;
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
      return apiClient
        .get('/catalogs')
        .then((response) => response.data)
        .catch((e) => {
          throw new Error('Failed to fetch catalogs');
        });
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
      return apiClient
        .get(`/catalogs/${catalog}`)
        .then((response) => response.data)
        .catch((e) => {
          throw new Error('Failed to fetch catalog');
        });
    },
  });
}

export interface CreateCatalogMutationParams
  extends Pick<CatalogInterface, 'name' | 'comment'> {}

// Create a new catalog
export function useCreateCatalog() {
  const queryClient = useQueryClient();

  return useMutation<CatalogInterface, Error, CreateCatalogMutationParams>({
    mutationFn: async (params: CreateCatalogMutationParams) => {
      return apiClient
        .post(`/catalogs`, JSON.stringify(params))
        .then((response) => response.data)
        .catch((e) => {
          throw new Error('Failed to create catalog');
        });
    },
    onSuccess: () => {
      queryClient.invalidateQueries({
        queryKey: ['listCatalogs'],
      });
    },
  });
}

export interface UpdateCatalogMutationParams
  extends Pick<CatalogInterface, 'comment'> {}

// Update a new catalog
export function useUpdateCatalog(catalog: string) {
  const queryClient = useQueryClient();

  return useMutation<CatalogInterface, Error, UpdateCatalogMutationParams>({
    mutationFn: async (params: UpdateCatalogMutationParams) => {
      return apiClient
        .patch(`/catalogs/${catalog}`, JSON.stringify(params))
        .then((response) => response.data)
        .catch((e) => {
          throw new Error(
            e.response?.data?.message || 'Failed to update catalog',
          );
        });
    },
    onSuccess: (catalog) => {
      queryClient.invalidateQueries({
        queryKey: ['getCatalog', catalog.name],
      });
    },
  });
}

export interface DeleteCatalogMutationParams
  extends Pick<CatalogInterface, 'name'> {}

// Delete a catalog
export function useDeleteCatalog() {
  const queryClient = useQueryClient();

  return useMutation<void, Error, DeleteCatalogMutationParams>({
    mutationFn: async (params: DeleteCatalogMutationParams) => {
      return apiClient
        .delete(`/catalogs/${params.name}`)
        .then((response) => response.data)
        .catch((e) => {
          throw new Error(
            e.response?.data?.message || 'Failed to delete catalog',
          );
        });
    },
    onSuccess: () => {
      queryClient.invalidateQueries({
        queryKey: ['listCatalogs'],
      });
    },
  });
}
