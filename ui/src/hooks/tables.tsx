import {
  useMutation,
  useQuery,
  useQueryClient,
  UseQueryOptions,
} from '@tanstack/react-query';
import apiClient from '../context/client';

interface ColumnInterface {
  name: string;
  type_text: string;
  type_name: string;
  created_at: number;
}

export interface TableInterface {
  table_id: string;
  table_type: string;
  catalog_name: string;
  schema_name: string;
  name: string;
  comment: string;
  created_at: number;
  updated_at: number | null;
  data_source_format: string;
  columns: ColumnInterface[];
}
interface ListTablesResponse {
  tables: TableInterface[];
  next_page_token: string | null;
}

interface ListTablesParams {
  catalog: string;
  schema: string;
  options?: Omit<UseQueryOptions<ListTablesResponse>, 'queryKey' | 'queryFn'>;
}

export function useListTables({ catalog, schema, options }: ListTablesParams) {
  return useQuery<ListTablesResponse>({
    queryKey: ['listTables', catalog, schema],
    queryFn: async () => {
      const searchParams = new URLSearchParams({
        catalog_name: catalog,
        schema_name: schema,
      });

      return apiClient
        .get(`/tables?${searchParams.toString()}`)
        .then((response) => response.data);
    },
    ...options,
  });
}

interface GetTableParams {
  catalog: string;
  schema: string;
  table: string;
}

export function useGetTable({ catalog, schema, table }: GetTableParams) {
  return useQuery<TableInterface>({
    queryKey: ['getTable', catalog, schema, table],
    queryFn: async () => {
      const fullName = [catalog, schema, table].join('.');

      return apiClient
        .get(`/tables/${fullName}`)
        .then((response) => response.data);
    },
  });
}

interface UpdateTableParams {
  catalog: string;
  schema: string;
  table: string;
}
export interface UpdateTableMutationParams
  extends Pick<TableInterface, 'comment'> {}

// Update a new table
export function useUpdateTable({ catalog, schema, table }: UpdateTableParams) {
  const queryClient = useQueryClient();

  return useMutation<TableInterface, Error, UpdateTableMutationParams>({
    mutationFn: async (params: UpdateTableMutationParams) => {
      const fullTableName = [catalog, schema, table].join('.');

      return apiClient
        .patch(`/tables/${fullTableName}`, JSON.stringify(params))
        .then((response) => response.data)
        .catch((e) => {
          throw new Error(
            e.response?.data?.message || 'Failed to update table',
          );
        });
    },

    onSuccess: () => {
      queryClient.invalidateQueries({
        queryKey: ['getTable', catalog, schema, table],
      });
    },
  });
}

export interface DeleteTableMutationParams
  extends Pick<TableInterface, 'catalog_name' | 'schema_name' | 'name'> {}

interface DeleteTableParams {
  catalog: string;
  schema: string;
}

export function useDeleteTable({ catalog, schema }: DeleteTableParams) {
  const queryClient = useQueryClient();

  return useMutation<void, Error, DeleteTableMutationParams>({
    mutationFn: async ({
      catalog_name,
      schema_name,
      name,
    }: DeleteTableMutationParams): Promise<void> => {
      return apiClient
        .delete(`/tables/${catalog_name}.${schema_name}.${name}`)
        .then((response) => response.data)
        .catch((e) => {
          throw new Error(
            e.response?.data?.message || 'Failed to delete table',
          );
        });
    },
    onSuccess: () => {
      queryClient.invalidateQueries({
        queryKey: ['listTables', catalog, schema],
      });
    },
  });
}
