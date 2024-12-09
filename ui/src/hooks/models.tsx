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

export type ModelInterface = ApiInterface<
  CatalogComponent,
  'RegisteredModelInfo'
>;

export type UseListModelsArgs = ApiRequestQueryParam<
  CatalogApi,
  '/models',
  'get'
> & {
  options?: Omit<
    UseQueryOptions<ApiSuccessResponse<CatalogApi, '/models', 'get'>>,
    'queryKey' | 'queryFn'
  >;
};

export function useListModels({
  catalog_name,
  schema_name,
  options,
}: UseListModelsArgs) {
  return useQuery<ApiSuccessResponse<CatalogApi, '/models', 'get'>>({
    queryKey: ['listModels', catalog_name, schema_name],
    queryFn: async () => {
      const api = route({
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
      });
      const response = await api.call();
      if (response.result !== 'success') {
        // NOTE:
        // When an expected error occurs, as defined in the OpenAPI specification, the following line will
        // be executed. This block serves as a placeholder for expected errors.
      }
      return response.data;
    },
    ...options,
  });
}

export type UseGetModelArgs = ApiRequestPathParam<
  CatalogApi,
  '/models/{full_name}',
  'get'
>;

export function useGetModel({ full_name }: UseGetModelArgs) {
  const [catalog, schema, model] = full_name.split('.');

  return useQuery<ApiSuccessResponse<CatalogApi, '/models/{full_name}', 'get'>>(
    {
      queryKey: ['getModel', catalog, schema, model],
      queryFn: async () => {
        const api = route({
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
        });
        const response = await api.call();
        if (response.result !== 'success') {
          // NOTE:
          // When an expected error occurs, as defined in the OpenAPI specification, the following line will
          // be executed. This block serves as a placeholder for expected errors.
        }
        return response.data;
      },
    },
  );
}

export type CreateModelMutationParams = ApiRequestBody<
  CatalogApi,
  '/models',
  'post'
>;

//create model
export function useCreateModel() {
  const queryClient = useQueryClient();

  return useMutation<
    ApiSuccessResponse<CatalogApi, '/models', 'post'>,
    Error,
    CreateModelMutationParams
  >({
    mutationFn: async ({
      name,
      catalog_name,
      schema_name,
      comment,
    }: CreateModelMutationParams) => {
      const api = route({
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
      });
      const response = await api.call();
      if (response.result !== 'success') {
        // NOTE:
        // When an expected error occurs, as defined in the OpenAPI specification, the following line will
        // be executed. This block serves as a placeholder for expected errors.
      }
      return response.data;
    },
    onSuccess: (model) => {
      queryClient.invalidateQueries({
        queryKey: ['listModels', model.catalog_name, model.schema_name],
      });
    },
  });
}

export type UseUpdateModelArgs = ApiRequestPathParam<
  CatalogApi,
  '/models/{full_name}',
  'patch'
>;

export type UpdateModelMutationParams = ApiRequestBody<
  CatalogApi,
  '/models/{full_name}',
  'patch'
>;

// update model
export function useUpdateModel({ full_name }: UseUpdateModelArgs) {
  const queryClient = useQueryClient();

  const [catalog, schema, model] = full_name.split('.');

  return useMutation<
    ApiSuccessResponse<CatalogApi, '/models/{full_name}', 'patch'>,
    Error,
    UpdateModelMutationParams
  >({
    mutationFn: async ({ comment }: UpdateModelMutationParams) => {
      const api = route({
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
        queryKey: ['getModel', catalog, schema, model],
      });
    },
  });
}

export type UseDeleteModelArgs = ApiRequestPathParam<
  CatalogApi,
  '/models/{full_name}',
  'delete'
>;

export type DeleteModelMutationParams = ApiRequestPathParam<
  CatalogApi,
  '/models/{full_name}',
  'delete'
>;

// Delete a model
export function useDeleteModel({ full_name }: UseDeleteModelArgs) {
  const queryClient = useQueryClient();

  const [catalog, schema] = full_name.split('.');

  return useMutation<
    ApiSuccessResponse<CatalogApi, '/models/{full_name}', 'delete'>,
    Error,
    DeleteModelMutationParams
  >({
    mutationFn: async ({ full_name }: DeleteModelMutationParams) => {
      const api = route({
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
        queryKey: ['listModels', catalog, schema],
      });
    },
  });
}

// NOTE:
// TypeScript enums require their values, so re-exported here as `const`.
export { ModelVersionStatus } from '../types/api/catalog.gen';

export type ModelVersionInterface = ApiInterface<
  CatalogComponent,
  'ModelVersionInfo'
>;

export type UseListModelVersionsArgs = ApiRequestPathParam<
  CatalogApi,
  '/models/{full_name}/versions',
  'get'
> &
  ApiRequestQueryParam<CatalogApi, '/models/{full_name}/versions', 'get'>;

export function useListModelVersions({ full_name }: UseListModelVersionsArgs) {
  const [catalog, schema, model] = full_name.split('.');

  return useQuery<
    ApiSuccessResponse<CatalogApi, '/models/{full_name}/versions', 'get'>
  >({
    queryKey: ['listModelVersions', catalog, schema, model],
    queryFn: async () => {
      const api = route({
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

export type UseGetModelVersionArgs = ApiRequestPathParam<
  CatalogApi,
  '/models/{full_name}/versions/{version}',
  'get'
>;

export function useGetModelVersion({
  full_name,
  version,
}: UseGetModelVersionArgs) {
  const [catalog, schema, model] = full_name.split('.');

  return useQuery<
    ApiSuccessResponse<
      CatalogApi,
      '/models/{full_name}/versions/{version}',
      'get'
    >
  >({
    queryKey: ['getVersion', catalog, schema, model, version],
    queryFn: async () => {
      const api = route({
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

export type UseUpdateModelVersionArgs = ApiRequestPathParam<
  CatalogApi,
  '/models/{full_name}/versions/{version}',
  'patch'
>;

export type UpdateModelVersionMutationParams = ApiRequestBody<
  CatalogApi,
  '/models/{full_name}/versions/{version}',
  'patch'
>;

// update model version
export function useUpdateModelVersion({
  full_name,
  version,
}: UseUpdateModelVersionArgs) {
  const queryClient = useQueryClient();

  const [catalog, schema, model] = full_name.split('.');

  return useMutation<
    ApiSuccessResponse<
      CatalogApi,
      '/models/{full_name}/versions/{version}',
      'patch'
    >,
    Error,
    UpdateModelVersionMutationParams
  >({
    mutationFn: async ({ comment }: UpdateModelVersionMutationParams) => {
      const api = route({
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
        queryKey: ['getVersion', catalog, schema, model, version],
      });
    },
  });
}

export type UseDeleteModelVersionArgs = ApiRequestPathParam<
  CatalogApi,
  '/models/{full_name}/versions/{version}',
  'delete'
>;

export type DeleteModelVersionMutationParams = ApiRequestPathParam<
  CatalogApi,
  '/models/{full_name}/versions/{version}',
  'delete'
>;

// Delete a model version
export function useDeleteModelVersion({
  full_name,
  version,
}: UseDeleteModelVersionArgs) {
  const queryClient = useQueryClient();

  const [catalog, schema, model] = full_name.split('.');

  return useMutation<
    ApiSuccessResponse<
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
      const api = route({
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
        queryKey: ['listModelVersions', catalog, schema, model],
      });
    },
  });
}
