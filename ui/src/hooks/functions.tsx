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

// TODO:
// The queries `max_results` and `page_token` are not properly handled as of [25/11/2024].
// These queries need to be implemented.
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
  max_results,
  page_token,
  options,
}: UseListFunctionsArgs) {
  return useQuery<ApiSuccessResponse<CatalogApi, '/functions', 'get'>>({
    queryKey: ['listFunctions', catalog_name, schema_name],
    queryFn: async () => {
      const api = route(CLIENT, {
        path: '/functions',
        method: 'get',
        params: {
          query: {
            catalog_name,
            schema_name,
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
        throw new Error('Failed to list functions');
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
      const api = route(CLIENT, {
        path: '/functions/{name}',
        method: 'get',
        params: {
          paths: {
            name,
          },
        },
      });
      const response = await api.call();
      if (response.result !== 'success') {
        // NOTE:
        // When an expected error occurs, as defined in the OpenAPI specification, the following line will
        // be executed. Unexpected errors will throw `Error("Unexpected error")`. The following block serves
        // as a placeholder for expected errors.
        throw new Error('Failed to fetch function');
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
      const api = route(CLIENT, {
        path: '/functions/{name}',
        method: 'delete',
        params: {
          paths: {
            name,
          },
        },
      });
      const response = await api.call();
      if (response.result !== 'success') {
        // NOTE:
        // When an expected error occurs, as defined in the OpenAPI specification, the following line will
        // be executed. Unexpected errors will throw `Error("Unexpected error")`. The following block serves
        // as a placeholder for expected errors.
        throw new Error('Failed to delete function');
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
