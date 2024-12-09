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

export type TableInterface = ApiInterface<CatalogComponent, 'TableInfo'>;

export type UseListTablesArgs = ApiRequestQueryParam<
  CatalogApi,
  '/tables',
  'get'
> & {
  options?: Omit<
    UseQueryOptions<ApiSuccessResponse<CatalogApi, '/tables', 'get'>>,
    'queryKey' | 'queryFn'
  >;
};

export function useListTables({
  catalog_name,
  schema_name,
  options,
}: UseListTablesArgs) {
  return useQuery<ApiSuccessResponse<CatalogApi, '/tables', 'get'>>({
    queryKey: ['listTables', catalog_name, schema_name],
    queryFn: async () => {
      const api = route({
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

export type UseGetTableArgs = ApiRequestPathParam<
  CatalogApi,
  '/tables/{full_name}',
  'get'
>;

export function useGetTable({ full_name }: UseGetTableArgs) {
  const [catalog, schema, table] = full_name.split('.');

  return useQuery<ApiSuccessResponse<CatalogApi, '/tables/{full_name}', 'get'>>(
    {
      queryKey: ['getTable', catalog, schema, table],
      queryFn: async () => {
        const api = route({
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

export type UseDeleteTableArgs = ApiRequestPathParam<
  CatalogApi,
  '/tables/{full_name}',
  'delete'
>;

export type DeleteTableMutationParams = ApiRequestPathParam<
  CatalogApi,
  '/tables/{full_name}',
  'delete'
>;

export function useDeleteTable({ full_name }: UseDeleteTableArgs) {
  const queryClient = useQueryClient();

  const [catalog, schema] = full_name.split('.');

  return useMutation<
    ApiSuccessResponse<CatalogApi, '/tables/{full_name}', 'delete'>,
    Error,
    DeleteTableMutationParams
  >({
    mutationFn: async ({
      full_name,
    }: DeleteTableMutationParams): Promise<void> => {
      const api = route({
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
        queryKey: ['listTables', catalog, schema],
      });
    },
  });
}
