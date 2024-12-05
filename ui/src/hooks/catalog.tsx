import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { CLIENT } from '../context/catalog';
import { route } from '../utils/openapi';
import type {
  paths as CatalogApi,
  components as CatalogComponent,
} from '../types/api/catalog.gen';
import type {
  ApiInterface,
  ApiSuccessResponse,
  ApiRequestPathParam,
  ApiRequestBody,
} from '../utils/openapi';

export type CatalogInterface = ApiInterface<CatalogComponent, 'CatalogInfo'>;

// Fetch the list of catalogs
export function useListCatalogs() {
  return useQuery<ApiSuccessResponse<CatalogApi, '/catalogs', 'get'>>({
    queryKey: ['listCatalogs'],
    queryFn: async () => {
      const api = route({
        client: CLIENT,
        request: {
          path: '/catalogs',
          method: 'get',
        },
        unexpectedErrorMessage: 'Failed to fetch catalogs',
      });
      const response = await api.call();
      if (response.result !== 'success') {
        // NOTE:
        // When an expected error occurs, as defined in the OpenAPI specification, the following line will
        // be executed. This block serves as a placeholder for expected errors.
      }
      return response.data;
    },
  });
}

export type UseGetCatalogArgs = ApiRequestPathParam<
  CatalogApi,
  '/catalogs/{name}',
  'get'
>;

// Fetch a single catalog by name
export function useGetCatalog({ name }: UseGetCatalogArgs) {
  return useQuery<ApiSuccessResponse<CatalogApi, '/catalogs/{name}', 'get'>>({
    queryKey: ['getCatalog', name],
    queryFn: async () => {
      const api = route({
        client: CLIENT,
        request: {
          path: '/catalogs/{name}',
          method: 'get',
          params: {
            paths: {
              name,
            },
          },
        },
        unexpectedErrorMessage: 'Failed to fetch catalog',
      });
      const response = await api.call();
      if (response.result !== 'success') {
        // NOTE:
        // When an expected error occurs, as defined in the OpenAPI specification, the following line will
        // be executed. This block serves as a placeholder for expected errors.
      }
      return response.data;
    },
  });
}

export type CreateCatalogMutationParams = ApiRequestBody<
  CatalogApi,
  '/catalogs',
  'post'
>;

// Create a new catalog
export function useCreateCatalog() {
  const queryClient = useQueryClient();

  return useMutation<
    ApiSuccessResponse<CatalogApi, '/catalogs', 'post'>,
    Error,
    CreateCatalogMutationParams
  >({
    mutationFn: async ({
      name,
      comment,
      properties,
    }: CreateCatalogMutationParams) => {
      const api = route({
        client: CLIENT,
        request: {
          path: '/catalogs',
          method: 'post',
          params: {
            body: {
              name,
              comment,
              properties,
            },
          },
        },
        unexpectedErrorMessage: 'Failed to create catalog',
      });
      const response = await api.call();
      if (response.result !== 'success') {
        // NOTE:
        // When an expected error occurs, as defined in the OpenAPI specification, the following line will
        // be executed. This block serves as a placeholder for expected errors.
      }
      return response.data;
    },
    onSuccess: () => {
      queryClient.invalidateQueries({
        queryKey: ['listCatalogs'],
      });
    },
  });
}

export type UseUpdateCatalogArgs = ApiRequestPathParam<
  CatalogApi,
  '/catalogs/{name}',
  'patch'
>;

export type UpdateCatalogMutationParams = ApiRequestBody<
  CatalogApi,
  '/catalogs/{name}',
  'patch'
>;

// Update a new catalog
export function useUpdateCatalog({ name }: UseUpdateCatalogArgs) {
  const queryClient = useQueryClient();

  return useMutation<
    ApiSuccessResponse<CatalogApi, '/catalogs/{name}', 'patch'>,
    Error,
    UpdateCatalogMutationParams
  >({
    mutationFn: async ({
      comment,
      properties,
      new_name,
    }: UpdateCatalogMutationParams) => {
      const api = route({
        client: CLIENT,
        request: {
          path: '/catalogs/{name}',
          method: 'patch',
          params: {
            paths: {
              name,
            },
            body: {
              comment,
              properties,
              new_name,
            },
          },
        },
        unexpectedErrorMessage: 'Failed to update catalog',
      });
      const response = await api.call();
      if (response.result !== 'success') {
        // NOTE:
        // When an expected error occurs, as defined in the OpenAPI specification, the following line will
        // be executed. This block serves as a placeholder for expected errors.
      }
      return response.data;
    },
    onSuccess: (catalog) => {
      queryClient.invalidateQueries({
        queryKey: ['getCatalog', catalog.name],
      });
    },
  });
}

export type DeleteCatalogMutationParams = ApiRequestPathParam<
  CatalogApi,
  '/catalogs/{name}',
  'patch'
>;

// Delete a catalog
export function useDeleteCatalog() {
  const queryClient = useQueryClient();

  return useMutation<
    ApiSuccessResponse<CatalogApi, '/catalogs/{name}', 'delete'>,
    Error,
    DeleteCatalogMutationParams
  >({
    mutationFn: async ({ name }: DeleteCatalogMutationParams) => {
      const api = route({
        client: CLIENT,
        request: {
          path: '/catalogs/{name}',
          method: 'delete',
          params: {
            paths: {
              name,
            },
          },
        },
        unexpectedErrorMessage: 'Failed to delete catalog',
      });
      const response = await api.call();
      if (response.result !== 'success') {
        // NOTE:
        // When an expected error occurs, as defined in the OpenAPI specification, the following line will
        // be executed. This block serves as a placeholder for expected errors.
      }
      return response.data;
    },
    onSuccess: () => {
      queryClient.invalidateQueries({
        queryKey: ['listCatalogs'],
      });
    },
  });
}
