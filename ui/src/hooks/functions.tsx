import { useQuery } from '@tanstack/react-query';
import { UC_API_PREFIX } from '../utils/constants';

export interface FunctionInterface {
  table_id: string;
  catalog_name: string;
  schema_name: string;
  name: string;
  comment: string;
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
}

export function useListFunctions({ catalog, schema }: ListFunctionsParams) {
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
  });
}
