import {
  useMutation,
  useQuery,
  useQueryClient,
  UseQueryOptions,
} from '@tanstack/react-query';
import { CLIENT } from '../context/client';
import { route, isError, assertNever } from '../utils/openapi';
import type {
  paths as CatalogApi,
  components as CatalogComponent,
} from '../types/api/catalog.gen';
import type {
  Model,
  PathParam,
  QueryParam,
  RequestBody,
  Route,
  SuccessResponseBody,
} from '../utils/openapi';

export interface ModelInterface
  extends Model<CatalogComponent, 'RegisteredModelInfo'> {}

export interface UseListModelsArgs
  extends QueryParam<CatalogApi, '/models', 'get'> {
  options?: Omit<
    UseQueryOptions<SuccessResponseBody<CatalogApi, '/models', 'get'>>,
    'queryKey' | 'queryFn'
  >;
}

export function useListModels({
  catalog_name,
  schema_name,
  options,
}: UseListModelsArgs) {
  return useQuery<SuccessResponseBody<CatalogApi, '/models', 'get'>>({
    queryKey: ['listModels', catalog_name, schema_name],
    queryFn: async () => {
      const response = await (route as Route<CatalogApi>)({
        client: CLIENT,
        request: {
          path: '/models',
          method: 'get',
          params: {
            query: {
              catalog_name,
              schema_name,
            },
          },
        },
        errorMessage: 'Failed to list models',
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
    ...options,
  });
}

export interface UseGetModelArgs
  extends PathParam<CatalogApi, '/models/{full_name}', 'get'> {}

export function useGetModel({ full_name }: UseGetModelArgs) {
  const [catalog, schema, model] = full_name.split('.');

  return useQuery<
    SuccessResponseBody<CatalogApi, '/models/{full_name}', 'get'>
  >({
    queryKey: ['getModel', catalog, schema, model],
    queryFn: async () => {
      const response = await (route as Route<CatalogApi>)({
        client: CLIENT,
        request: {
          path: '/models/{full_name}',
          method: 'get',
          params: {
            paths: {
              full_name,
            },
          },
        },
        errorMessage: 'Failed to fetch model',
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

export interface CreateModelMutationParams
  extends RequestBody<CatalogApi, '/models', 'post'> {}

export function useCreateModel() {
  const queryClient = useQueryClient();

  return useMutation<
    SuccessResponseBody<CatalogApi, '/models', 'post'>,
    Error,
    CreateModelMutationParams
  >({
    mutationFn: async ({
      name,
      catalog_name,
      schema_name,
      comment,
    }: CreateModelMutationParams) => {
      const response = await (route as Route<CatalogApi>)({
        client: CLIENT,
        request: {
          path: '/models',
          method: 'post',
          params: {
            body: {
              name,
              catalog_name,
              schema_name,
              comment,
            },
          },
        },
        errorMessage: 'Failed to create model',
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
    onSuccess: (model) => {
      queryClient.invalidateQueries({
        queryKey: ['listModels', model.catalog_name, model.schema_name],
      });
    },
  });
}

export interface UseUpdateModelArgs
  extends PathParam<CatalogApi, '/models/{full_name}', 'patch'> {}

export interface UpdateModelMutationParams
  extends RequestBody<CatalogApi, '/models/{full_name}', 'patch'> {}

export function useUpdateModel({ full_name }: UseUpdateModelArgs) {
  const queryClient = useQueryClient();

  const [catalog, schema, model] = full_name.split('.');

  return useMutation<
    SuccessResponseBody<CatalogApi, '/models/{full_name}', 'patch'>,
    Error,
    UpdateModelMutationParams
  >({
    mutationFn: async ({ comment }: UpdateModelMutationParams) => {
      const response = await (route as Route<CatalogApi>)({
        client: CLIENT,
        request: {
          path: '/models/{full_name}',
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
        errorMessage: 'Failed to update model',
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
        queryKey: ['getModel', catalog, schema, model],
      });
    },
  });
}

export interface UseDeleteModelArgs
  extends PathParam<CatalogApi, '/models/{full_name}', 'delete'> {}

export interface DeleteModelMutationParams
  extends PathParam<CatalogApi, '/models/{full_name}', 'delete'> {}

export function useDeleteModel({ full_name }: UseDeleteModelArgs) {
  const queryClient = useQueryClient();

  const [catalog, schema] = full_name.split('.');

  return useMutation<
    SuccessResponseBody<CatalogApi, '/models/{full_name}', 'delete'>,
    Error,
    DeleteModelMutationParams
  >({
    mutationFn: async ({ full_name }: DeleteModelMutationParams) => {
      const response = await (route as Route<CatalogApi>)({
        client: CLIENT,
        request: {
          path: '/models/{full_name}',
          method: 'delete',
          params: {
            paths: {
              full_name,
            },
          },
        },
        errorMessage: 'Failed to delete model',
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
        queryKey: ['listModels', catalog, schema],
      });
    },
  });
}

// NOTE:
// TypeScript enums require their values, so re-exported here as `const`.
export { ModelVersionStatus } from '../types/api/catalog.gen';

export interface ModelVersionInterface
  extends Model<CatalogComponent, 'ModelVersionInfo'> {}

export interface UseListModelVersionsArgs
  extends PathParam<CatalogApi, '/models/{full_name}/versions', 'get'> {}

export function useListModelVersions({ full_name }: UseListModelVersionsArgs) {
  const [catalog, schema, model] = full_name.split('.');

  return useQuery<
    SuccessResponseBody<CatalogApi, '/models/{full_name}/versions', 'get'>
  >({
    queryKey: ['listModelVersions', catalog, schema, model],
    queryFn: async () => {
      const response = await (route as Route<CatalogApi>)({
        client: CLIENT,
        request: {
          path: '/models/{full_name}/versions',
          method: 'get',
          params: {
            paths: {
              full_name,
            },
          },
        },
        errorMessage: 'Failed to fetch model version',
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

export interface UseGetModelVersionArgs
  extends PathParam<
    CatalogApi,
    '/models/{full_name}/versions/{version}',
    'get'
  > {}

export function useGetModelVersion({
  full_name,
  version,
}: UseGetModelVersionArgs) {
  const [catalog, schema, model] = full_name.split('.');

  return useQuery<
    SuccessResponseBody<
      CatalogApi,
      '/models/{full_name}/versions/{version}',
      'get'
    >
  >({
    queryKey: ['getVersion', catalog, schema, model, version],
    queryFn: async () => {
      const response = await (route as Route<CatalogApi>)({
        client: CLIENT,
        request: {
          path: '/models/{full_name}/versions/{version}',
          method: 'get',
          params: {
            paths: {
              full_name,
              version,
            },
          },
        },
        errorMessage: 'Failed to fetch model version',
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

export interface UseUpdateModelVersionArgs
  extends PathParam<
    CatalogApi,
    '/models/{full_name}/versions/{version}',
    'patch'
  > {}

export interface UpdateModelVersionMutationParams
  extends RequestBody<
    CatalogApi,
    '/models/{full_name}/versions/{version}',
    'patch'
  > {}

export function useUpdateModelVersion({
  full_name,
  version,
}: UseUpdateModelVersionArgs) {
  const queryClient = useQueryClient();

  const [catalog, schema, model] = full_name.split('.');

  return useMutation<
    SuccessResponseBody<
      CatalogApi,
      '/models/{full_name}/versions/{version}',
      'patch'
    >,
    Error,
    UpdateModelVersionMutationParams
  >({
    mutationFn: async ({ comment }: UpdateModelVersionMutationParams) => {
      const response = await (route as Route<CatalogApi>)({
        client: CLIENT,
        request: {
          path: '/models/{full_name}/versions/{version}',
          method: 'patch',
          params: {
            paths: {
              full_name,
              version,
            },
            body: {
              comment,
            },
          },
        },
        errorMessage: 'Failed to update model version',
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
        queryKey: ['getVersion', catalog, schema, model, version],
      });
    },
  });
}

export interface UseDeleteModelVersionArgs
  extends PathParam<
    CatalogApi,
    '/models/{full_name}/versions/{version}',
    'delete'
  > {}

export interface DeleteModelVersionMutationParams
  extends PathParam<
    CatalogApi,
    '/models/{full_name}/versions/{version}',
    'delete'
  > {}

export function useDeleteModelVersion({
  full_name,
  version,
}: UseDeleteModelVersionArgs) {
  const queryClient = useQueryClient();

  const [catalog, schema, model] = full_name.split('.');

  return useMutation<
    SuccessResponseBody<
      CatalogApi,
      '/models/{full_name}/versions/{version}',
      'delete'
    >,
    Error,
    DeleteModelVersionMutationParams
  >({
    mutationFn: async ({
      full_name,
      version,
    }: DeleteModelVersionMutationParams) => {
      const response = await (route as Route<CatalogApi>)({
        client: CLIENT,
        request: {
          path: '/models/{full_name}/versions/{version}',
          method: 'delete',
          params: {
            paths: {
              full_name,
              version,
            },
          },
        },
        errorMessage: 'Failed to delete model version',
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
        queryKey: ['listModelVersions', catalog, schema, model],
      });
    },
  });
}
