import { useQuery } from '@tanstack/react-query';
import { UC_API_PREFIX } from '../utils/constants';

interface SchemaInterface {
  schema_id: string;
  catalog_name: string;
  name: string;
  comment: string;
  created_at: string;
}
interface ListSchemasResponse {
  schemas: SchemaInterface[];
  next_page_token: string | null;
}

interface ListSchemasParams {
  catalog: string;
}

export function useListSchemas({ catalog }: ListSchemasParams) {
  return useQuery<ListSchemasResponse>({
    queryKey: ['listSchemas', catalog],
    queryFn: async () => {
      const searchParams = new URLSearchParams({ catalog_name: catalog });

      const response = await fetch(
        `${UC_API_PREFIX}/schemas?${searchParams.toString()}`
      );
      return response.json();
    },
  });
}
