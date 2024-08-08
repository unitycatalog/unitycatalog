import {
  useQuery,
  useMutation,
  useQueryClient,
  UseQueryOptions,
} from '@tanstack/react-query';
import { UC_API_PREFIX } from '../utils/constants';

export interface VolumeInterface {
  volume_id: string;
  volume_type: string;
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
        `${UC_API_PREFIX}/volumes?${searchParams.toString()}`,
      );
      return response.json();
    },
    ...options,
  });
}

interface GetVolumeParams {
  catalog: string;
  schema: string;
  volume: string;
}

export function useGetVolume({ catalog, schema, volume }: GetVolumeParams) {
  return useQuery<VolumeInterface>({
    queryKey: ['getVolume', catalog, schema, volume],
    queryFn: async () => {
      const fullVolumeName = [catalog, schema, volume].join('.');

      const response = await fetch(
        `${UC_API_PREFIX}/volumes/${fullVolumeName}`,
      );
      return response.json();
    },
  });
}

export interface DeleteVolumeMutationParams
  extends Pick<VolumeInterface, 'catalog_name' | 'schema_name' | 'name'> {}

interface DeleteVolumeParams {
  catalog: string;
  schema: string;
}

// Delete a volume
export function useDeleteVolume({ catalog, schema }: DeleteVolumeParams) {
  const queryClient = useQueryClient();

  return useMutation<void, Error, DeleteVolumeMutationParams>({
    mutationFn: async ({
      catalog_name,
      schema_name,
      name,
    }: DeleteVolumeMutationParams) => {
      const response = await fetch(
        `${UC_API_PREFIX}/volumes/${catalog_name}.${schema_name}.${name}`,
        {
          method: 'DELETE',
          headers: {
            'Content-Type': 'application/json',
          },
        },
      );
      if (!response.ok) {
        const errorData = await response.json();
        throw new Error(errorData.message || 'Failed to delete volume');
      }
    },
    onSuccess: () => {
      queryClient.invalidateQueries({
        queryKey: ['listVolumes', catalog, schema],
      });
    },
  });
}
