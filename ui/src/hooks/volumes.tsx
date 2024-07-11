import { useQuery, UseQueryOptions } from '@tanstack/react-query';
import { UC_API_PREFIX } from '../utils/constants';

export interface VolumeInterface {
  table_id: string;
  catalog_name: string;
  schema_name: string;
  name: string;
  comment: string;
  created_at: number;
  updated_at: number | null;
}
interface ListVolumesResponse {
  volumes: VolumeInterface[];
  next_page_token: string | null;
}

interface ListVolumesParams {
  catalog: string;
  schema: string;
  options?: Omit<UseQueryOptions<ListVolumesResponse>, 'queryKey' | 'queryFn'>;
}

export function useListVolumes({
  catalog,
  schema,
  options,
}: ListVolumesParams) {
  return useQuery<ListVolumesResponse>({
    queryKey: ['listVolumes', catalog, schema],
    queryFn: async () => {
      const searchParams = new URLSearchParams({
        catalog_name: catalog,
        schema_name: schema,
      });

      const response = await fetch(
        `${UC_API_PREFIX}/volumes?${searchParams.toString()}`
      );
      return response.json();
    },
    ...options,
  });
}
