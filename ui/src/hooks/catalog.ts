import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { CLIENT } from '../context/client';
import { route, isError, assertNever } from '../utils/openapi';
import type {
  paths as CatalogApi,
  components as CatalogComponent,
} from '../types/api/catalog.gen';
import type {
  Model,
  PathParam,
  RequestBody,
  Route,
  SuccessResponseBody,
} from '../utils/openapi';

export interface CatalogInterface
  extends Model<CatalogComponent, 'CatalogInfo'> {}

export function useListCatalogs() {
  return useQuery<SuccessResponseBody<CatalogApi, '/catalogs', 'get'>>({
    queryKey: ['listCatalogs'],
    queryFn: async () => {
      const response = await (route as Route<CatalogApi>)({
        client: CLIENT,
        request: {
          path: '/catalogs',
          method: 'get',
        },
        errorMessage: 'Failed to fetch catalogs',
      }).call();
      if (isError(response)) {
        // NOTE:
        // When an expected error occurs, as defined in the OpenAPI specification, the following line will
        // be executed. This block serves as a placeholder for expected errors.
        return assertNever(response.data.status);
      } else {
        return response.data;
      }
    },
  });
}

export interface UseGetCatalogArgs
  extends PathParam<CatalogApi, '/catalogs/{name}', 'get'> {}

export function useGetCatalog({ name }: UseGetCatalogArgs) {
  return useQuery<SuccessResponseBody<CatalogApi, '/catalogs/{name}', 'get'>>({
    queryKey: ['getCatalog', name],
    queryFn: async () => {
      const response = await (route as Route<CatalogApi>)({
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
        errorMessage: 'Failed to fetch catalog',
      }).call();
      if (isError(response)) {
        // NOTE:
        // When an expected error occurs, as defined in the OpenAPI specification, the following line will
        // be executed. This block serves as a placeholder for expected errors.
        return assertNever(response.data.status);
      } else {
        return response.data;
      }
    },
  });
}

export interface CreateCatalogMutationParams
  extends RequestBody<CatalogApi, '/catalogs', 'post'> {}

export function useCreateCatalog() {
  const queryClient = useQueryClient();

  return useMutation<
    SuccessResponseBody<CatalogApi, '/catalogs', 'post'>,
    Error,
    CreateCatalogMutationParams
  >({
    mutationFn: async ({
      name,
      comment,
      properties,
    }: CreateCatalogMutationParams) => {
      const response = await (route as Route<CatalogApi>)({
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
        errorMessage: 'Failed to create catalog',
      }).call();
      if (isError(response)) {
        // NOTE:
        // When an expected error occurs, as defined in the OpenAPI specification, the following line will
        // be executed. This block serves as a placeholder for expected errors.
        return assertNever(response.data.status);
      } else {
        return response.data;
      }
    },
    onSuccess: () => {
      queryClient.invalidateQueries({
        queryKey: ['listCatalogs'],
      });
    },
  });
}

export interface UseUpdateCatalogArgs
  extends PathParam<CatalogApi, '/catalogs/{name}', 'patch'> {}

export interface UpdateCatalogMutationParams
  extends RequestBody<CatalogApi, '/catalogs/{name}', 'patch'> {}

export function useUpdateCatalog({ name }: UseUpdateCatalogArgs) {
  const queryClient = useQueryClient();

  return useMutation<
    SuccessResponseBody<CatalogApi, '/catalogs/{name}', 'patch'>,
    Error,
    UpdateCatalogMutationParams
  >({
    mutationFn: async ({
      comment,
      properties,
      new_name,
    }: UpdateCatalogMutationParams) => {
      const response = await (route as Route<CatalogApi>)({
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
        errorMessage: 'Failed to update catalog',
      }).call();
      if (isError(response)) {
        // NOTE:
        // When an expected error occurs, as defined in the OpenAPI specification, the following line will
        // be executed. This block serves as a placeholder for expected errors.
        return assertNever(response.data.status);
      } else {
        return response.data;
      }
    },
    onSuccess: (catalog) => {
      queryClient.invalidateQueries({
        queryKey: ['getCatalog', catalog.name],
      });
    },
  });
}

export interface DeleteCatalogMutationParams
  extends PathParam<CatalogApi, '/catalogs/{name}', 'delete'> {}

export function useDeleteCatalog() {
  const queryClient = useQueryClient();

  return useMutation<
    SuccessResponseBody<CatalogApi, '/catalogs/{name}', 'delete'>,
    Error,
    DeleteCatalogMutationParams
  >({
    mutationFn: async ({ name }: DeleteCatalogMutationParams) => {
      const response = await (route as Route<CatalogApi>)({
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
        errorMessage: 'Failed to delete catalog',
      }).call();
      if (isError(response)) {
        // NOTE:
        // When an expected error occurs, as defined in the OpenAPI specification, the following line will
        // be executed. This block serves as a placeholder for expected errors.
        return assertNever(response.data.status);
      } else {
        return response.data;
      }
    },
    onSuccess: () => {
      queryClient.invalidateQueries({
        queryKey: ['listCatalogs'],
      });
    },
  });
}
