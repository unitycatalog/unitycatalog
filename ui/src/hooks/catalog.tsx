import { useQuery } from '@tanstack/react-query';
import { UC_API_PREFIX } from '../utils/constants';

interface CatalogInterface {
  name: string;
  id: string;
  created_at: string;
}
interface ListCatalogsResponse {
  catalogs: CatalogInterface[];
  next_page_token: string | null;
}

export function useListCatalogs() {
  return useQuery<ListCatalogsResponse>({
    queryKey: ['listCatalogs'],
    queryFn: async () => {
      const response = await fetch(`${UC_API_PREFIX}/catalogs`);
      return response.json();
    },
  });
}
