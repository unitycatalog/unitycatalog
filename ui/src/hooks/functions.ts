import {
  useMutation,
  useQuery,
  useQueryClient,
  UseQueryOptions,
} from '@tanstack/react-query';
import { CLIENT } from '../context/catalog';
import { route, isError, assertNever, Router } from '../utils/openapi';
import type {
  paths as CatalogApi,
  components as CatalogComponent,
} from '../types/api/catalog.gen';
import type {
  Model,
  PathParam,
  QueryParam,
  SuccessResponseBody,
} from '../utils/openapi';

export type FunctionInterface = Model<CatalogComponent, 'FunctionInfo'>;

export type UseListFunctionsArgs = QueryParam<
  CatalogApi,
  '/functions',
  'get'
> & {
  options?: Omit<
    UseQueryOptions<SuccessResponseBody<CatalogApi, '/functions', 'get'>>,
    'queryKey' | 'queryFn'
  >;
};

export function useListFunctions({
  catalog_name,
  schema_name,
  options,
}: UseListFunctionsArgs) {
  return useQuery<SuccessResponseBody<CatalogApi, '/functions', 'get'>>({
    queryKey: ['listFunctions', catalog_name, schema_name],
    queryFn: async () => {
      const api = (route as Router<CatalogApi>)({
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

export type UseGetFunctionArgs = PathParam<
  CatalogApi,
  '/functions/{name}',
  'get'
>;

export function useGetFunction({ name }: UseGetFunctionArgs) {
  const [catalog, schema, ucFunction] = name.split('.');

  return useQuery<SuccessResponseBody<CatalogApi, '/functions/{name}', 'get'>>({
    queryKey: ['getFunction', catalog, schema, ucFunction],
    queryFn: async () => {
      const api = (route as Router<CatalogApi>)({
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

export type UseDeleteFunctionArgs = PathParam<
  CatalogApi,
  '/functions/{name}',
  'delete'
>;

export type DeleteFunctionMutationParams = PathParam<
  CatalogApi,
  '/functions/{name}',
  'delete'
>;

export function useDeleteFunction({ name }: UseDeleteFunctionArgs) {
  const queryClient = useQueryClient();

  const [catalog, schema] = name.split('.');

  return useMutation<
    SuccessResponseBody<CatalogApi, '/functions/{name}', 'delete'>,
    Error,
    DeleteFunctionMutationParams
  >({
    mutationFn: async ({ name }: DeleteFunctionMutationParams) => {
      const api = (route as Router<CatalogApi>)({
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
        queryKey: ['listFunctions', catalog, schema],
      });
    },
  });
}
