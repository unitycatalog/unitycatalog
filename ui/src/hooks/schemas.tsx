import {
  useMutation,
  useQuery,
  useQueryClient,
  UseQueryOptions,
} from '@tanstack/react-query';
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
  ApiRequestQueryParam,
  ApiRequestBody,
} from '../utils/openapi';

export type SchemaInterface = ApiInterface<CatalogComponent, 'SchemaInfo'>;

// TODO:
// The queries `max_results` and `page_token` are not properly handled as of [25/11/2024].
// These queries need to be implemented.
export type UseListSchemasArgs = ApiRequestQueryParam<
  CatalogApi,
  '/schemas',
  'get'
> & {
  options?: Omit<
    UseQueryOptions<ApiSuccessResponse<CatalogApi, '/schemas', 'get'>>,
    'queryKey' | 'queryFn'
  >;
};

export function useListSchemas({
  catalog_name,
  max_results,
  page_token,
  options,
}: UseListSchemasArgs) {
  return useQuery<ApiSuccessResponse<CatalogApi, '/schemas', 'get'>>({
    queryKey: ['listSchemas', catalog_name],
    queryFn: async () => {
      const api = route(CLIENT, {
        path: '/schemas',
        method: 'get',
        params: {
          query: {
            catalog_name,
            max_results,
            page_token,
          },
        },
      });
      const response = await api.call();
      if (response.result !== 'success') {
        // NOTE:
        // When an expected error occurs, as defined in the OpenAPI specification, the following line will
        // be executed. Unexpected errors will throw `Error("Unexpected error")`. The following block serves
        // as a placeholder for expected errors.
        throw new Error('Failed to list schemas');
      }
      return response.data;
    },
    ...options,
  });
}

export type UseGetSchemaArgs = ApiRequestPathParam<
  CatalogApi,
  '/schemas/{full_name}',
  'get'
>;

export function useGetSchema({ full_name }: UseGetSchemaArgs) {
  const [catalog, schema] = full_name.split('.');

  return useQuery<
    ApiSuccessResponse<CatalogApi, '/schemas/{full_name}', 'get'>
  >({
    queryKey: ['getSchema', catalog, schema],
    queryFn: async () => {
      const api = route(CLIENT, {
        path: '/schemas/{full_name}',
        method: 'get',
        params: {
          paths: {
            full_name,
          },
        },
      });
      const response = await api.call();
      if (response.result !== 'success') {
        // NOTE:
        // When an expected error occurs, as defined in the OpenAPI specification, the following line will
        // be executed. Unexpected errors will throw `Error("Unexpected error")`. The following block serves
        // as a placeholder for expected errors.
        throw new Error('Failed to fetch schema');
      }
      return response.data;
    },
  });
}

export type CreateSchemaMutationParams = ApiRequestBody<
  CatalogApi,
  '/schemas',
  'post'
>;

export function useCreateSchema() {
  const queryClient = useQueryClient();

  return useMutation<
    ApiSuccessResponse<CatalogApi, '/schemas', 'post'>,
    Error,
    CreateSchemaMutationParams
  >({
    mutationFn: async ({
      name,
      catalog_name,
      comment,
      properties,
    }: CreateSchemaMutationParams) => {
      const api = route(CLIENT, {
        path: '/schemas',
        method: 'post',
        params: {
          body: {
            name,
            catalog_name,
            comment,
            properties,
          },
        },
      });
      const response = await api.call();
      if (response.result !== 'success') {
        // NOTE:
        // When an expected error occurs, as defined in the OpenAPI specification, the following line will
        // be executed. Unexpected errors will throw `Error("Unexpected error")`. The following block serves
        // as a placeholder for expected errors.
        throw new Error('Failed to create schema');
      }
      return response.data;
    },
    onSuccess: (schema) => {
      queryClient.invalidateQueries({
        queryKey: ['listSchemas', schema.catalog_name],
      });
    },
  });
}

export type UseUpdateSchemaArgs = ApiRequestPathParam<
  CatalogApi,
  '/schemas/{full_name}',
  'patch'
>;

export type UpdateSchemaMutationParams = ApiRequestBody<
  CatalogApi,
  '/schemas/{full_name}',
  'patch'
>;

// Update a new schema
export function useUpdateSchema({ full_name }: UseUpdateSchemaArgs) {
  const queryClient = useQueryClient();

  const [catalog, schema] = full_name.split('.');

  return useMutation<
    ApiSuccessResponse<CatalogApi, '/schemas/{full_name}', 'patch'>,
    Error,
    UpdateSchemaMutationParams
  >({
    mutationFn: async ({
      comment,
      properties,
      new_name,
    }: UpdateSchemaMutationParams) => {
      const api = route(CLIENT, {
        path: '/schemas/{full_name}',
        method: 'patch',
        params: {
          paths: {
            full_name,
          },
          body: {
            comment,
            properties,
            new_name,
          },
        },
      });
      const response = await api.call();
      if (response.result !== 'success') {
        // NOTE:
        // When an expected error occurs, as defined in the OpenAPI specification, the following line will
        // be executed. Unexpected errors will throw `Error("Unexpected error")`. The following block serves
        // as a placeholder for expected errors.
        throw new Error('Failed to update schema');
      }
      return response.data;
    },
    onSuccess: () => {
      queryClient.invalidateQueries({
        queryKey: ['getSchema', catalog, schema],
      });
    },
  });
}

export type UseDeleteSchemaArgs = ApiRequestPathParam<
  CatalogApi,
  '/schemas/{full_name}',
  'delete'
>;

export type DeleteSchemaMutationParams = ApiRequestPathParam<
  CatalogApi,
  '/schemas/{full_name}',
  'delete'
>;

export function useDeleteSchema({ full_name }: UseDeleteSchemaArgs) {
  const queryClient = useQueryClient();

  const [catalog] = full_name.split('.');

  return useMutation<
    ApiSuccessResponse<CatalogApi, '/schemas/{full_name}', 'delete'>,
    Error,
    DeleteSchemaMutationParams
  >({
    mutationFn: async ({ full_name }: DeleteSchemaMutationParams) => {
      const api = route(CLIENT, {
        path: '/schemas/{full_name}',
        method: 'delete',
        params: {
          paths: {
            full_name,
          },
        },
      });
      const response = await api.call();
      if (response.result !== 'success') {
        // NOTE:
        // When an expected error occurs, as defined in the OpenAPI specification, the following line will
        // be executed. Unexpected errors will throw `Error("Unexpected error")`. The following block serves
        // as a placeholder for expected errors.
        throw new Error('Failed to delete schema');
      }
      return response.data;
    },
    onSuccess: () => {
      queryClient.invalidateQueries({
        queryKey: ['listSchemas', catalog],
      });
    },
  });
}
