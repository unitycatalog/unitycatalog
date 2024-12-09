import {
  useQuery,
  useMutation,
  useQueryClient,
  UseQueryOptions,
} from '@tanstack/react-query';
import { CLIENT } from '../context/catalog';
import { route } from '../utils/openapi';
import type {
  paths as CatalogApi,
  components as CatalogComponent,
} from '../types/api/catalog.gen';
import type {
  ApiInterface,
  ApiSuccessResponse,
  ApiRequestPathParam,
  ApiRequestQueryParam,
  ApiRequestBody,
} from '../utils/openapi';

export type VolumeInterface = ApiInterface<CatalogComponent, 'VolumeInfo'>;

export type UseListVolumesArgs = ApiRequestQueryParam<
  CatalogApi,
  '/volumes',
  'get'
> & {
  options?: Omit<
    UseQueryOptions<ApiSuccessResponse<CatalogApi, '/volumes', 'get'>>,
    'queryKey' | 'queryFn'
  >;
};

export function useListVolumes({
  catalog_name,
  schema_name,
  max_results: _max_results,
  page_token: _page_token,
  options,
}: UseListVolumesArgs) {
  return useQuery<ApiSuccessResponse<CatalogApi, '/volumes', 'get'>>({
    queryKey: ['listVolumes', catalog_name, schema_name],
    queryFn: async () => {
      const api = route({
        client: CLIENT,
        request: {
          path: '/volumes',
          method: 'get',
          params: {
            query: {
              catalog_name,
              schema_name,
            },
          },
        },
        errorMessage: 'Failed to list volumes',
      });
      const response = await api.call();
      if (response.result !== 'success') {
        // NOTE:
        // When an expected error occurs, as defined in the OpenAPI specification, the following line will
        // be executed. This block serves as a placeholder for expected errors.
      }
      return response.data;
    },
    ...options,
  });
}

export type UseGetVolumeArgs = ApiRequestPathParam<
  CatalogApi,
  '/volumes/{name}',
  'get'
>;

export function useGetVolume({ name }: UseGetVolumeArgs) {
  const [catalog, schema, volume] = name.split('.');

  return useQuery<VolumeInterface>({
    queryKey: ['getVolume', catalog, schema, volume],
    queryFn: async () => {
      const api = route({
        client: CLIENT,
        request: {
          path: '/volumes/{name}',
          method: 'get',
          params: {
            paths: {
              name,
            },
          },
        },
        errorMessage: 'Failed to fetch volume',
      });
      const response = await api.call();
      if (response.result !== 'success') {
        // NOTE:
        // When an expected error occurs, as defined in the OpenAPI specification, the following line will
        // be executed. This block serves as a placeholder for expected errors.
      }
      return response.data;
    },
  });
}

export type UseUpdateVolumeArgs = ApiRequestPathParam<
  CatalogApi,
  '/volumes/{name}',
  'patch'
>;

export type UpdateVolumeMutationParams = ApiRequestBody<
  CatalogApi,
  '/volumes/{name}',
  'patch'
>;

export function useUpdateVolume({ name }: UseUpdateVolumeArgs) {
  const queryClient = useQueryClient();

  const [catalog, schema, volume] = name.split('.');

  return useMutation<
    ApiSuccessResponse<CatalogApi, '/volumes/{name}', 'patch'>,
    Error,
    UpdateVolumeMutationParams
  >({
    mutationFn: async ({ comment, new_name: _new_name }: UpdateVolumeMutationParams) => {
      const api = route({
        client: CLIENT,
        request: {
          path: '/volumes/{name}',
          method: 'patch',
          params: {
            paths: {
              name,
            },
            body: {
              comment,
            },
          },
        },
        errorMessage: 'Failed to update volume',
      });
      const response = await api.call();
      if (response.result !== 'success') {
        // NOTE:
        // When an expected error occurs, as defined in the OpenAPI specification, the following line will
        // be executed. This block serves as a placeholder for expected errors.
      }
      return response.data;
    },
    onSuccess: () => {
      queryClient.invalidateQueries({
        queryKey: ['getVolume', catalog, schema, volume],
      });
    },
  });
}

export type UseDeleteVolumeArgs = ApiRequestPathParam<
  CatalogApi,
  '/volumes/{name}',
  'delete'
>;

export type DeleteVolumeMutationParams = ApiRequestPathParam<
  CatalogApi,
  '/volumes/{name}',
  'delete'
>;

// Delete a volume
export function useDeleteVolume({ name }: UseDeleteVolumeArgs) {
  const queryClient = useQueryClient();

  const [catalog, schema] = name.split('.');

  return useMutation<
    ApiSuccessResponse<CatalogApi, '/volumes/{name}', 'delete'>,
    Error,
    DeleteVolumeMutationParams
  >({
    mutationFn: async ({ name }: DeleteVolumeMutationParams) => {
      const api = route({
        client: CLIENT,
        request: {
          path: '/volumes/{name}',
          method: 'delete',
          params: {
            paths: {
              name,
            },
          },
        },
        errorMessage: 'Failed to delete volume',
      });
      const response = await api.call();
      if (response.result !== 'success') {
        // NOTE:
        // When an expected error occurs, as defined in the OpenAPI specification, the following line will
        // be executed. This block serves as a placeholder for expected errors.
      }
      return response.data;
    },
    onSuccess: () => {
      queryClient.invalidateQueries({
        queryKey: ['listVolumes', catalog, schema],
      });
    },
  });
}
