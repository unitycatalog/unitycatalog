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
  Route,
  SuccessResponseBody,
} from '../utils/openapi';

export interface FunctionInterface
  extends Model<CatalogComponent, 'FunctionInfo'> {}

export interface UseListFunctionsArgs
  extends QueryParam<CatalogApi, '/functions', 'get'> {
  options?: Omit<
    UseQueryOptions<SuccessResponseBody<CatalogApi, '/functions', 'get'>>,
    'queryKey' | 'queryFn'
  >;
}

export function useListFunctions({
  catalog_name,
  schema_name,
  options,
}: UseListFunctionsArgs) {
  return useQuery<SuccessResponseBody<CatalogApi, '/functions', 'get'>>({
    queryKey: ['listFunctions', catalog_name, schema_name],
    queryFn: async () => {
      const response = await (route as Route<CatalogApi>)({
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

export interface UseGetFunctionArgs
  extends PathParam<CatalogApi, '/functions/{name}', 'get'> {}

export function useGetFunction({ name }: UseGetFunctionArgs) {
  const [catalog, schema, ucFunction] = name.split('.');

  return useQuery<SuccessResponseBody<CatalogApi, '/functions/{name}', 'get'>>({
    queryKey: ['getFunction', catalog, schema, ucFunction],
    queryFn: async () => {
      const response = await (route as Route<CatalogApi>)({
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

export interface UseDeleteFunctionArgs
  extends PathParam<CatalogApi, '/functions/{name}', 'delete'> {}

export interface DeleteFunctionMutationParams
  extends PathParam<CatalogApi, '/functions/{name}', 'delete'> {}

export function useDeleteFunction({ name }: UseDeleteFunctionArgs) {
  const queryClient = useQueryClient();

  const [catalog, schema] = name.split('.');

  return useMutation<
    SuccessResponseBody<CatalogApi, '/functions/{name}', 'delete'>,
    Error,
    DeleteFunctionMutationParams
  >({
    mutationFn: async ({ name }: DeleteFunctionMutationParams) => {
      const response = await (route as Route<CatalogApi>)({
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
        queryKey: ['listFunctions', catalog, schema],
      });
    },
  });
}
