import {
  useQuery,
  useMutation,
  useQueryClient,
  UseQueryOptions,
} from '@tanstack/react-query';
import { CLIENT } from '../context/client';
import { route, isError, assertNever } from '../utils/openapi';
import type {
  paths as CatalogApi,
  components as CatalogComponent,
} from '../types/api/catalog.gen';
import type {
  Model,
  PathParam,
  QueryParam,
  RequestBody,
  Route,
  SuccessResponseBody,
} from '../utils/openapi';

export interface VolumeInterface
  extends Model<CatalogComponent, 'VolumeInfo'> {}

export interface UseListVolumesArgs
  extends QueryParam<CatalogApi, '/volumes', 'get'> {
  options?: Omit<
    UseQueryOptions<SuccessResponseBody<CatalogApi, '/volumes', 'get'>>,
    'queryKey' | 'queryFn'
  >;
}

export function useListVolumes({
  catalog_name,
  schema_name,
  options,
}: UseListVolumesArgs) {
  return useQuery<SuccessResponseBody<CatalogApi, '/volumes', 'get'>>({
    queryKey: ['listVolumes', catalog_name, schema_name],
    queryFn: async () => {
      const response = await (route as Route<CatalogApi>)({
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
      }).call();
      if (isError(response)) {
        // NOTE:
        // When an expected error occurs, as defined in the OpenAPI specification, the following line will
        // be executed. This block serves as a placeholder for expected errors.
        return assertNever(response.data.status);
      } else {
        return response.data;
      }
    },
    ...options,
  });
}

export interface UseGetVolumeArgs
  extends PathParam<CatalogApi, '/volumes/{name}', 'get'> {}

export function useGetVolume({ name }: UseGetVolumeArgs) {
  const [catalog, schema, volume] = name.split('.');

  return useQuery<SuccessResponseBody<CatalogApi, '/volumes/{name}', 'get'>>({
    queryKey: ['getVolume', catalog, schema, volume],
    queryFn: async () => {
      const response = await (route as Route<CatalogApi>)({
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
      }).call();
      if (isError(response)) {
        // NOTE:
        // When an expected error occurs, as defined in the OpenAPI specification, the following line will
        // be executed. This block serves as a placeholder for expected errors.
        return assertNever(response.data.status);
      } else {
        return response.data;
      }
    },
  });
}

export interface UseUpdateVolumeArgs
  extends PathParam<CatalogApi, '/volumes/{name}', 'patch'> {}

export interface UpdateVolumeMutationParams
  extends RequestBody<CatalogApi, '/volumes/{name}', 'patch'> {}

export function useUpdateVolume({ name }: UseUpdateVolumeArgs) {
  const queryClient = useQueryClient();

  const [catalog, schema, volume] = name.split('.');

  return useMutation<
    SuccessResponseBody<CatalogApi, '/volumes/{name}', 'patch'>,
    Error,
    UpdateVolumeMutationParams
  >({
    mutationFn: async ({ comment }: UpdateVolumeMutationParams) => {
      const response = await (route as Route<CatalogApi>)({
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
      }).call();
      if (isError(response)) {
        // NOTE:
        // When an expected error occurs, as defined in the OpenAPI specification, the following line will
        // be executed. This block serves as a placeholder for expected errors.
        return assertNever(response.data.status);
      } else {
        return response.data;
      }
    },
    onSuccess: () => {
      queryClient.invalidateQueries({
        queryKey: ['getVolume', catalog, schema, volume],
      });
    },
  });
}

export interface UseDeleteVolumeArgs
  extends PathParam<CatalogApi, '/volumes/{name}', 'delete'> {}

export interface DeleteVolumeMutationParams
  extends PathParam<CatalogApi, '/volumes/{name}', 'delete'> {}

export function useDeleteVolume({ name }: UseDeleteVolumeArgs) {
  const queryClient = useQueryClient();

  const [catalog, schema] = name.split('.');

  return useMutation<
    SuccessResponseBody<CatalogApi, '/volumes/{name}', 'delete'>,
    Error,
    DeleteVolumeMutationParams
  >({
    mutationFn: async ({ name }: DeleteVolumeMutationParams) => {
      const response = await (route as Route<CatalogApi>)({
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
      }).call();
      if (isError(response)) {
        // NOTE:
        // When an expected error occurs, as defined in the OpenAPI specification, the following line will
        // be executed. This block serves as a placeholder for expected errors.
        return assertNever(response.data.status);
      } else {
        return response.data;
      }
    },
    onSuccess: () => {
      queryClient.invalidateQueries({
        queryKey: ['listVolumes', catalog, schema],
      });
    },
  });
}
