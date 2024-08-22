import {
  useMutation,
  useQuery,
  useQueryClient,
  UseQueryOptions,
} from '@tanstack/react-query';
import { UC_API_PREFIX } from '../utils/constants';

export interface SchemaInterface {
  schema_id: string;
  catalog_name: string;
  name: string;
  comment: string;
  created_at: number;
  updated_at: number | null;
}
interface ListSchemasResponse {
  schemas: SchemaInterface[];
  next_page_token: string | null;
}

interface ListSchemasParams {
  catalog: string;
  options?: Omit<UseQueryOptions<ListSchemasResponse>, 'queryKey' | 'queryFn'>;
}

export function useListSchemas({ catalog, options }: ListSchemasParams) {
  return useQuery<ListSchemasResponse>({
    queryKey: ['listSchemas', catalog],
    queryFn: async () => {
      const searchParams = new URLSearchParams({ catalog_name: catalog });

      const response = await fetch(
        `${UC_API_PREFIX}/schemas?${searchParams.toString()}`,
      );
      return response.json();
    },
    ...options,
  });
}

interface GetSchemaParams {
  catalog: string;
  schema: string;
}
export function useGetSchema({ catalog, schema }: GetSchemaParams) {
  return useQuery<SchemaInterface>({
    queryKey: ['getSchema', catalog, schema],
    queryFn: async () => {
      const fullName = [catalog, schema].join('.');

      const response = await fetch(`${UC_API_PREFIX}/schemas/${fullName}`);
      return response.json();
    },
  });
}

export interface CreateSchemaMutationParams
  extends Pick<SchemaInterface, 'name' | 'catalog_name' | 'comment'> {}

export function useCreateSchema() {
  const queryClient = useQueryClient();

  return useMutation<SchemaInterface, Error, CreateSchemaMutationParams>({
    mutationFn: async (params: CreateSchemaMutationParams) => {
      const response = await fetch(`${UC_API_PREFIX}/schemas`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(params),
      });
      if (!response.ok) {
        // TODO: Expose error message
        throw new Error('Failed to create schema');
      }
      return response.json();
    },
    onSuccess: (schema) => {
      queryClient.invalidateQueries({
        queryKey: ['listSchemas', schema.catalog_name],
      });
    },
  });
}
// =========================
interface UpdateSchemaParams {
  catalog: string;
  schema: string;
}
export interface UpdateSchemaMutationParams
  extends Pick<SchemaInterface, 'name' | 'comment'> {}

// Update a new schema
export function useUpdateSchema({ catalog, schema }: UpdateSchemaParams) {
  const queryClient = useQueryClient();

  return useMutation<SchemaInterface, Error, UpdateSchemaMutationParams>({
    mutationFn: async (params: UpdateSchemaMutationParams) => {
      const fullSchemaName = [catalog, schema].join('.');

      const response = await fetch(
        `${UC_API_PREFIX}/schemas/${fullSchemaName}`,
        {
          method: 'PATCH',
          headers: {
            'Content-Type': 'application/json',
          },
          body: JSON.stringify(params),
        },
      );
      if (!response.ok) {
        const errorData = await response.json();
        throw new Error(errorData.message || 'Failed to update schema');
      }
      return response.json();
    },
    onSuccess: () => {
      queryClient.invalidateQueries({
        queryKey: ['getSchema', catalog, schema],
      });
    },
  });
}

// =========================

export interface DeleteSchemaMutationParams
  extends Pick<SchemaInterface, 'catalog_name' | 'name'> {}

interface DeleteSchemaParams {
  catalog: string;
}

export function useDeleteSchema({ catalog }: DeleteSchemaParams) {
  const queryClient = useQueryClient();

  return useMutation<void, Error, DeleteSchemaMutationParams>({
    mutationFn: async (params: DeleteSchemaMutationParams) => {
      const response = await fetch(
        `${UC_API_PREFIX}/schemas/${params.catalog_name}.${params.name}`,
        {
          method: 'DELETE',
        },
      );
      if (!response.ok) {
        const errorData = await response.json();
        throw new Error(errorData.message || 'Failed to delete schema');
      }
    },
    onSuccess: () => {
      queryClient.invalidateQueries({
        queryKey: ['listSchemas', catalog],
      });
    },
  });
}
