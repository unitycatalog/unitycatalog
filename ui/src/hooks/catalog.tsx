import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { CLIENT } from '../context/catalog';
import { route, isError } from '../utils/openapi';
import type {
  paths as CatalogApi,
  components as CatalogComponent,
} from '../types/api/catalog.gen';
import type {
  Model,
  PathParam,
  RequestBody,
  SuccessResponseBody,
} from '../utils/openapi';

export type CatalogInterface = Model<CatalogComponent, 'CatalogInfo'>;

export function useListCatalogs() {
  return useQuery<SuccessResponseBody<CatalogApi, '/catalogs', 'get'>>({
    queryKey: ['listCatalogs'],
    queryFn: async () => {
      const api = route({
        client: CLIENT,
        request: {
          path: '/catalogs',
          method: 'get',
        },
        errorMessage: 'Failed to fetch catalogs',
      });
      const response = await api.call();
      if (isError(response)) {
        // NOTE:
        // When an expected error occurs, as defined in the OpenAPI specification, the following line will
        // be executed. This block serves as a placeholder for expected errors.
        //
        // NOTE:
        // As of 14/12/2024, all properties of the models defined in the OpenAPI specification are marked as
        // optional. Consequently, any `object` can match the type of the `SuccessResponseBody` for any API
        // (effectively disabling meaningful type checking). In the future, as the OpenAPI specification is
        // updated, additional changes may be required for this type guard clause, such as incorporating
        // `return response.data` below.
      }
      return response.data;
    },
  });
}

export type UseGetCatalogArgs = PathParam<
  CatalogApi,
  '/catalogs/{name}',
  'get'
>;

export function useGetCatalog({ name }: UseGetCatalogArgs) {
  return useQuery<SuccessResponseBody<CatalogApi, '/catalogs/{name}', 'get'>>({
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
        errorMessage: 'Failed to fetch catalog',
      });
      const response = await api.call();
      if (isError(response)) {
        // NOTE:
        // When an expected error occurs, as defined in the OpenAPI specification, the following line will
        // be executed. This block serves as a placeholder for expected errors.
        //
        // NOTE:
        // As of 14/12/2024, all properties of the models defined in the OpenAPI specification are marked as
        // optional. Consequently, any `object` can match the type of the `SuccessResponseBody` for any API
        // (effectively disabling meaningful type checking). In the future, as the OpenAPI specification is
        // updated, additional changes may be required for this type guard clause, such as incorporating
        // `return response.data` below.
      }
      return response.data;
    },
  });
}

export type CreateCatalogMutationParams = RequestBody<
  CatalogApi,
  '/catalogs',
  'post'
>;

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
        errorMessage: 'Failed to create catalog',
      });
      const response = await api.call();
      if (isError(response)) {
        // NOTE:
        // When an expected error occurs, as defined in the OpenAPI specification, the following line will
        // be executed. This block serves as a placeholder for expected errors.
        //
        // NOTE:
        // As of 14/12/2024, all properties of the models defined in the OpenAPI specification are marked as
        // optional. Consequently, any `object` can match the type of the `SuccessResponseBody` for any API
        // (effectively disabling meaningful type checking). In the future, as the OpenAPI specification is
        // updated, additional changes may be required for this type guard clause, such as incorporating
        // `return response.data` below.
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

export type UseUpdateCatalogArgs = PathParam<
  CatalogApi,
  '/catalogs/{name}',
  'patch'
>;

export type UpdateCatalogMutationParams = RequestBody<
  CatalogApi,
  '/catalogs/{name}',
  'patch'
>;

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
        errorMessage: 'Failed to update catalog',
      });
      const response = await api.call();
      if (isError(response)) {
        // NOTE:
        // When an expected error occurs, as defined in the OpenAPI specification, the following line will
        // be executed. This block serves as a placeholder for expected errors.
        //
        // NOTE:
        // As of 14/12/2024, all properties of the models defined in the OpenAPI specification are marked as
        // optional. Consequently, any `object` can match the type of the `SuccessResponseBody` for any API
        // (effectively disabling meaningful type checking). In the future, as the OpenAPI specification is
        // updated, additional changes may be required for this type guard clause, such as incorporating
        // `return response.data` below.
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

export type DeleteCatalogMutationParams = PathParam<
  CatalogApi,
  '/catalogs/{name}',
  'patch'
>;

export function useDeleteCatalog() {
  const queryClient = useQueryClient();

  return useMutation<
    SuccessResponseBody<CatalogApi, '/catalogs/{name}', 'delete'>,
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
        errorMessage: 'Failed to delete catalog',
      });
      const response = await api.call();
      if (isError(response)) {
        // NOTE:
        // When an expected error occurs, as defined in the OpenAPI specification, the following line will
        // be executed. This block serves as a placeholder for expected errors.
        //
        // NOTE:
        // As of 14/12/2024, all properties of the models defined in the OpenAPI specification are marked as
        // optional. Consequently, any `object` can match the type of the `SuccessResponseBody` for any API
        // (effectively disabling meaningful type checking). In the future, as the OpenAPI specification is
        // updated, additional changes may be required for this type guard clause, such as incorporating
        // `return response.data` below.
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
