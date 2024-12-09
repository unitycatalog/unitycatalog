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
} from '../utils/openapi';

export type FunctionInterface = ApiInterface<CatalogComponent, 'FunctionInfo'>;

export type UseListFunctionsArgs = ApiRequestQueryParam<
  CatalogApi,
  '/functions',
  'get'
> & {
  options?: Omit<
    UseQueryOptions<ApiSuccessResponse<CatalogApi, '/functions', 'get'>>,
    'queryKey' | 'queryFn'
  >;
};

export function useListFunctions({
  catalog_name,
  schema_name,
  max_results: _max_results,
  page_token: _page_token,
  options,
}: UseListFunctionsArgs) {
  return useQuery<ApiSuccessResponse<CatalogApi, '/functions', 'get'>>({
    queryKey: ['listFunctions', catalog_name, schema_name],
    queryFn: async () => {
      const api = route({
        client: CLIENT,
        request: {
          path: '/functions',
          method: 'get',
          params: {
            query: {
              catalog_name,
              schema_name,
            },
          },
        },
        errorMessage: 'Failed to list functions',
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

export type UseGetFunctionArgs = ApiRequestPathParam<
  CatalogApi,
  '/functions/{name}',
  'get'
>;

export function useGetFunction({ name }: UseGetFunctionArgs) {
  const [catalog, schema, ucFunction] = name.split('.');

  return useQuery<ApiSuccessResponse<CatalogApi, '/functions/{name}', 'get'>>({
    queryKey: ['getFunction', catalog, schema, ucFunction],
    queryFn: async () => {
      const api = route({
        client: CLIENT,
        request: {
          path: '/functions/{name}',
          method: 'get',
          params: {
            paths: {
              name,
            },
          },
        },
        errorMessage: 'Failed to fetch function',
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

export type UseDeleteFunctionArgs = ApiRequestPathParam<
  CatalogApi,
  '/functions/{name}',
  'delete'
>;

export type DeleteFunctionMutationParams = ApiRequestPathParam<
  CatalogApi,
  '/functions/{name}',
  'delete'
>;

// Delete a function
export function useDeleteFunction({ name }: UseDeleteFunctionArgs) {
  const queryClient = useQueryClient();

  const [catalog, schema] = name.split('.');

  return useMutation<
    ApiSuccessResponse<CatalogApi, '/functions/{name}', 'delete'>,
    Error,
    DeleteFunctionMutationParams
  >({
    mutationFn: async ({ name }: DeleteFunctionMutationParams) => {
      const api = route({
        client: CLIENT,
        request: {
          path: '/functions/{name}',
          method: 'delete',
          params: {
            paths: {
              name,
            },
          },
        },
        errorMessage: 'Failed to delete function',
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
        queryKey: ['listFunctions', catalog, schema],
      });
    },
  });
}
