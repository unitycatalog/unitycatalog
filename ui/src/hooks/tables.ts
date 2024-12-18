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

export interface TableInterface extends Model<CatalogComponent, 'TableInfo'> {}

export interface UseListTablesArgs
  extends QueryParam<CatalogApi, '/tables', 'get'> {
  options?: Omit<
    UseQueryOptions<SuccessResponseBody<CatalogApi, '/tables', 'get'>>,
    'queryKey' | 'queryFn'
  >;
}

export function useListTables({
  catalog_name,
  schema_name,
  options,
}: UseListTablesArgs) {
  return useQuery<SuccessResponseBody<CatalogApi, '/tables', 'get'>>({
    queryKey: ['listTables', catalog_name, schema_name],
    queryFn: async () => {
      const response = await (route as Route<CatalogApi>)({
        client: CLIENT,
        request: {
          path: '/tables',
          method: 'get',
          params: {
            query: {
              catalog_name,
              schema_name,
            },
          },
        },
        errorMessage: 'Failed to list tables',
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

export interface UseGetTableArgs
  extends PathParam<CatalogApi, '/tables/{full_name}', 'get'> {}

export function useGetTable({ full_name }: UseGetTableArgs) {
  const [catalog, schema, table] = full_name.split('.');

  return useQuery<
    SuccessResponseBody<CatalogApi, '/tables/{full_name}', 'get'>
  >({
    queryKey: ['getTable', catalog, schema, table],
    queryFn: async () => {
      const response = await (route as Route<CatalogApi>)({
        client: CLIENT,
        request: {
          path: '/tables/{full_name}',
          method: 'get',
          params: {
            paths: {
              full_name,
            },
          },
        },
        errorMessage: 'Failed to fetch table',
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

export interface UseDeleteTableArgs
  extends PathParam<CatalogApi, '/tables/{full_name}', 'delete'> {}

export interface DeleteTableMutationParams
  extends PathParam<CatalogApi, '/tables/{full_name}', 'delete'> {}

export function useDeleteTable({ full_name }: UseDeleteTableArgs) {
  const queryClient = useQueryClient();

  const [catalog, schema] = full_name.split('.');

  return useMutation<
    SuccessResponseBody<CatalogApi, '/tables/{full_name}', 'delete'>,
    Error,
    DeleteTableMutationParams
  >({
    mutationFn: async ({ full_name }: DeleteTableMutationParams) => {
      const response = await (route as Route<CatalogApi>)({
        client: CLIENT,
        request: {
          path: '/tables/{full_name}',
          method: 'delete',
          params: {
            paths: {
              full_name,
            },
          },
        },
        errorMessage: 'Failed to delete schema',
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
        queryKey: ['listTables', catalog, schema],
      });
    },
  });
}
