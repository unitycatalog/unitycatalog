import {
  useMutation,
  useQuery,
  useQueryClient,
  UseQueryOptions,
} from '@tanstack/react-query';
import { UC_API_PREFIX } from '../utils/constants';

export interface FunctionInterface {
  function_id: string;
  catalog_name: string;
  schema_name: string;
  name: string;
  comment: string;
  external_language: string;
  routine_definition: string;
  created_at: number;
  updated_at: number | null;
}
interface ListFunctionsResponse {
  functions: FunctionInterface[];
  next_page_token: string | null;
}

interface ListFunctionsParams {
  catalog: string;
  schema: string;
  options?: Omit<
    UseQueryOptions<ListFunctionsResponse>,
    'queryKey' | 'queryFn'
  >;
}

export function useListFunctions({
  catalog,
  schema,
  options,
}: ListFunctionsParams) {
  return useQuery<ListFunctionsResponse>({
    queryKey: ['listFunctions', catalog, schema],
    queryFn: async () => {
      const searchParams = new URLSearchParams({
        catalog_name: catalog,
        schema_name: schema,
      });

      const response = await fetch(
        `${UC_API_PREFIX}/functions?${searchParams.toString()}`,
      );
      return response.json();
    },
    ...options,
  });
}

interface GetFunctionParams {
  catalog: string;
  schema: string;
  ucFunction: string;
}

export function useGetFunction({
  catalog,
  schema,
  ucFunction,
}: GetFunctionParams) {
  return useQuery<FunctionInterface>({
    queryKey: ['getFunction', catalog, schema, ucFunction],
    queryFn: async () => {
      const fullFunctionName = [catalog, schema, ucFunction].join('.');

      const response = await fetch(
        `${UC_API_PREFIX}/functions/${fullFunctionName}`,
      );
      return response.json();
    },
  });
}

export interface DeleteFunctionMutationParams
  extends Pick<FunctionInterface, 'catalog_name' | 'schema_name' | 'name'> {}

interface DeleteFunctionParams {
  onSuccessCallback?: () => void;
  catalog: string;
  schema: string;
}

// Delete a function
export function useDeleteFunction({
  onSuccessCallback,
  catalog,
  schema,
}: DeleteFunctionParams) {
  const queryClient = useQueryClient();

  return useMutation<void, Error, DeleteFunctionMutationParams>({
    mutationFn: async ({
      catalog_name,
      schema_name,
      name,
    }: DeleteFunctionMutationParams) => {
      const response = await fetch(
        `${UC_API_PREFIX}/functions/${catalog_name}.${schema_name}.${name}`,
        {
          method: 'DELETE',
          headers: {
            'Content-Type': 'application/json',
          },
        },
      );
      if (!response.ok) {
        const errorData = await response.json();
        throw new Error(errorData.message || 'Failed to delete function');
      }
    },
    onSuccess: () => {
      queryClient.invalidateQueries({
        queryKey: ['listFunctions', catalog, schema],
      });
      onSuccessCallback?.();
    },
  });
}
