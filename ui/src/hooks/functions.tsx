import {
  useMutation,
  useQuery,
  useQueryClient,
  UseQueryOptions,
} from '@tanstack/react-query';
import apiClient from '../context/client';

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
  owner: string | null;
  created_by: string;
  updated_by: string | null;
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

      return apiClient
        .get(`/functions?${searchParams.toString()}`)
        .then((response) => response.data);
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

      return apiClient
        .get(`/functions/${fullFunctionName}`)
        .then((response) => response.data);
    },
  });
}

export interface DeleteFunctionMutationParams
  extends Pick<FunctionInterface, 'catalog_name' | 'schema_name' | 'name'> {}

interface DeleteFunctionParams {
  catalog: string;
  schema: string;
}

// Delete a function
export function useDeleteFunction({ catalog, schema }: DeleteFunctionParams) {
  const queryClient = useQueryClient();

  return useMutation<void, Error, DeleteFunctionMutationParams>({
    mutationFn: async ({
      catalog_name,
      schema_name,
      name,
    }: DeleteFunctionMutationParams) => {
      return apiClient
        .delete(`/functions/${catalog_name}.${schema_name}.${name}`)
        .then((response) => response.data)
        .catch((e) => {
          throw new Error(
            e.response?.data?.message || 'Failed to delete function',
          );
        });
    },
    onSuccess: () => {
      queryClient.invalidateQueries({
        queryKey: ['listFunctions', catalog, schema],
      });
    },
  });
}
