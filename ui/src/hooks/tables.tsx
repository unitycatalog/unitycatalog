import { useQuery } from '@tanstack/react-query';
import { UC_API_PREFIX } from '../utils/constants';

export interface TableInterface {
  table_id: string;
  catalog_name: string;
  schema_name: string;
  name: string;
  comment: string;
  created_at: number;
  updated_at: number | null;
}
interface ListTablesResponse {
  tables: TableInterface[];
  next_page_token: string | null;
}

interface ListTablesParams {
  catalog: string;
  schema: string;
}

export function useListTables({ catalog, schema }: ListTablesParams) {
  return useQuery<ListTablesResponse>({
    queryKey: ['listTables', catalog, schema],
    queryFn: async () => {
      const searchParams = new URLSearchParams({
        catalog_name: catalog,
        schema_name: schema,
      });

      const response = await fetch(
        `${UC_API_PREFIX}/tables?${searchParams.toString()}`
      );
      return response.json();
    },
  });
}
