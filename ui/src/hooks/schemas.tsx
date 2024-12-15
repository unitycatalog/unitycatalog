import {
  useMutation,
  useQuery,
  useQueryClient,
  UseQueryOptions,
} from '@tanstack/react-query';
import { CLIENT } from '../context/catalog';
import { route as _route, isError, assertNever } from '../utils/openapi';
import type {
  paths as CatalogApi,
  components as CatalogComponent,
} from '../types/api/catalog.gen';
import type {
  Router,
  Model,
  PathParam,
  QueryParam,
  RequestBody,
  SuccessResponseBody,
} from '../utils/openapi';

const route: Router<CatalogApi> = _route;

export type SchemaInterface = Model<CatalogComponent, 'SchemaInfo'>;

export type UseListSchemasArgs = QueryParam<CatalogApi, '/schemas', 'get'> & {
  options?: Omit<
    UseQueryOptions<SuccessResponseBody<CatalogApi, '/schemas', 'get'>>,
    'queryKey' | 'queryFn'
  >;
};

export function useListSchemas({ catalog_name, options }: UseListSchemasArgs) {
  return useQuery<SuccessResponseBody<CatalogApi, '/schemas', 'get'>>({
    queryKey: ['listSchemas', catalog_name],
    queryFn: async () => {
      const api = route({
        client: CLIENT,
        request: {
          path: '/schemas',
          method: 'get',
          params: {
            query: {
              catalog_name,
            },
          },
        },
        errorMessage: 'Failed to list schemas',
      });
      const response = await api.call();
      if (isError(response)) {
        // NOTE:
        // When an expected error occurs, as defined in the OpenAPI specification, the following line will
        // be executed. This block serves as a placeholder for expected errors.
        return assertNever(response.data.status);
      } else {
        return response.data;
      }
    },
    ...options,
  });
}

export type UseGetSchemaArgs = PathParam<
  CatalogApi,
  '/schemas/{full_name}',
  'get'
>;

export function useGetSchema({ full_name }: UseGetSchemaArgs) {
  const [catalog, schema] = full_name.split('.');

  return useQuery<
    SuccessResponseBody<CatalogApi, '/schemas/{full_name}', 'get'>
  >({
    queryKey: ['getSchema', catalog, schema],
    queryFn: async () => {
      const api = route({
        client: CLIENT,
        request: {
          path: '/schemas/{full_name}',
          method: 'get',
          params: {
            paths: {
              full_name,
            },
          },
        },
        errorMessage: 'Failed to fetch schema',
      });
      const response = await api.call();
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

export type CreateSchemaMutationParams = RequestBody<
  CatalogApi,
  '/schemas',
  'post'
>;

export function useCreateSchema() {
  const queryClient = useQueryClient();

  return useMutation<
    SuccessResponseBody<CatalogApi, '/schemas', 'post'>,
    Error,
    CreateSchemaMutationParams
  >({
    mutationFn: async ({
      name,
      catalog_name,
      comment,
      properties,
    }: CreateSchemaMutationParams) => {
      const api = route({
        client: CLIENT,
        request: {
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
        },
        errorMessage: 'Failed to create schema',
      });
      const response = await api.call();
      if (isError(response)) {
        // NOTE:
        // When an expected error occurs, as defined in the OpenAPI specification, the following line will
        // be executed. This block serves as a placeholder for expected errors.
        return assertNever(response.data.status);
      } else {
        return response.data;
      }
    },
    onSuccess: (schema) => {
      queryClient.invalidateQueries({
        queryKey: ['listSchemas', schema.catalog_name],
      });
    },
  });
}

export type UseUpdateSchemaArgs = PathParam<
  CatalogApi,
  '/schemas/{full_name}',
  'patch'
>;

export type UpdateSchemaMutationParams = RequestBody<
  CatalogApi,
  '/schemas/{full_name}',
  'patch'
>;

export function useUpdateSchema({ full_name }: UseUpdateSchemaArgs) {
  const queryClient = useQueryClient();

  const [catalog, schema] = full_name.split('.');

  return useMutation<
    SuccessResponseBody<CatalogApi, '/schemas/{full_name}', 'patch'>,
    Error,
    UpdateSchemaMutationParams
  >({
    mutationFn: async ({ comment }: UpdateSchemaMutationParams) => {
      const api = route({
        client: CLIENT,
        request: {
          path: '/schemas/{full_name}',
          method: 'patch',
          params: {
            paths: {
              full_name,
            },
            body: {
              comment,
            },
          },
        },
        errorMessage: 'Failed to update schema',
      });
      const response = await api.call();
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
        queryKey: ['getSchema', catalog, schema],
      });
    },
  });
}

export type UseDeleteSchemaArgs = PathParam<
  CatalogApi,
  '/schemas/{full_name}',
  'delete'
>;

export type DeleteSchemaMutationParams = PathParam<
  CatalogApi,
  '/schemas/{full_name}',
  'delete'
>;

export function useDeleteSchema({ full_name }: UseDeleteSchemaArgs) {
  const queryClient = useQueryClient();

  const [catalog] = full_name.split('.');

  return useMutation<
    SuccessResponseBody<CatalogApi, '/schemas/{full_name}', 'delete'>,
    Error,
    DeleteSchemaMutationParams
  >({
    mutationFn: async ({ full_name }: DeleteSchemaMutationParams) => {
      const api = route({
        client: CLIENT,
        request: {
          path: '/schemas/{full_name}',
          method: 'delete',
          params: {
            paths: {
              full_name,
            },
          },
        },
        errorMessage: 'Failed to delete schema',
      });
      const response = await api.call();
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
        queryKey: ['listSchemas', catalog],
      });
    },
  });
}
