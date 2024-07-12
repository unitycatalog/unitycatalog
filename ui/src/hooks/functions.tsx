import { useQuery, UseQueryOptions } from '@tanstack/react-query';
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
        `${UC_API_PREFIX}/functions?${searchParams.toString()}`
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
        `${UC_API_PREFIX}/functions/${fullFunctionName}`
      );
      return response.json();
    },
  });
}
