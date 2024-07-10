import { useQuery } from '@tanstack/react-query';
import { UC_API_PREFIX } from '../utils/constants';

export function useListCatalogs() {
  return useQuery({
    queryKey: ['listCatalogs'],
    queryFn: async () => {
      const response = await fetch(`${UC_API_PREFIX}/catalogs`);
      return response.json();
    },
  });
}
