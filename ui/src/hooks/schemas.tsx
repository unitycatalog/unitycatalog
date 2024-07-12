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
        `${UC_API_PREFIX}/schemas?${searchParams.toString()}`
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

interface CreateSchemaParams {
  onSuccessCallback?: (catalog: SchemaInterface) => void;
}

export function useCreateSchema({
  onSuccessCallback,
}: CreateSchemaParams = {}) {
  const queryClient = useQueryClient();

  return useMutation<SchemaInterface, unknown, CreateSchemaMutationParams>({
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
    onSuccess: (catalog) => {
      queryClient.invalidateQueries({
        queryKey: ['listSchemas', catalog.catalog_name],
      });
      onSuccessCallback?.(catalog);
    },
  });
}
