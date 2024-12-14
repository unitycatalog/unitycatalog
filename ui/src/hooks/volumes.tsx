import {
  useQuery,
  useMutation,
  useQueryClient,
  UseQueryOptions,
} from '@tanstack/react-query';
import { CLIENT } from '../context/catalog';
import { route, isError } from '../utils/openapi';
import type {
  paths as CatalogApi,
  components as CatalogComponent,
} from '../types/api/catalog.gen';
import type {
  Model,
  PathParam,
  QueryParam,
  RequestBody,
  SuccessResponseBody,
} from '../utils/openapi';

export type VolumeInterface = Model<CatalogComponent, 'VolumeInfo'>;

export type UseListVolumesArgs = QueryParam<CatalogApi, '/volumes', 'get'> & {
  options?: Omit<
    UseQueryOptions<SuccessResponseBody<CatalogApi, '/volumes', 'get'>>,
    'queryKey' | 'queryFn'
  >;
};

export function useListVolumes({
  catalog_name,
  schema_name,
  options,
}: UseListVolumesArgs) {
  return useQuery<SuccessResponseBody<CatalogApi, '/volumes', 'get'>>({
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
      if (isError(response)) {
        // NOTE:
        // When an expected error occurs, as defined in the OpenAPI specification, the following line will
        // be executed. This block serves as a placeholder for expected errors.
        //
        // NOTE:
        // As of 14/12/2024, all properties of the models defined in the OpenAPI specification are marked as
        // optional. Consequently, any `object` can match the type of the `SuccessResponseBody` for any API
        // (effectively disabling meaningful type checking). In the future, as the OpenAPI specification is
        // updated, additional changes may be required for this type guard clause, such as incorporating
        // `return response.data` below.
      }
      return response.data;
    },
    ...options,
  });
}

export type UseGetVolumeArgs = PathParam<CatalogApi, '/volumes/{name}', 'get'>;

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
      if (isError(response)) {
        // NOTE:
        // When an expected error occurs, as defined in the OpenAPI specification, the following line will
        // be executed. This block serves as a placeholder for expected errors.
        //
        // NOTE:
        // As of 14/12/2024, all properties of the models defined in the OpenAPI specification are marked as
        // optional. Consequently, any `object` can match the type of the `SuccessResponseBody` for any API
        // (effectively disabling meaningful type checking). In the future, as the OpenAPI specification is
        // updated, additional changes may be required for this type guard clause, such as incorporating
        // `return response.data` below.
      }
      return response.data;
    },
  });
}

export type UseUpdateVolumeArgs = PathParam<
  CatalogApi,
  '/volumes/{name}',
  'patch'
>;

export type UpdateVolumeMutationParams = RequestBody<
  CatalogApi,
  '/volumes/{name}',
  'patch'
>;

export function useUpdateVolume({ name }: UseUpdateVolumeArgs) {
  const queryClient = useQueryClient();

  const [catalog, schema, volume] = name.split('.');

  return useMutation<
    SuccessResponseBody<CatalogApi, '/volumes/{name}', 'patch'>,
    Error,
    UpdateVolumeMutationParams
  >({
    mutationFn: async ({ comment }: UpdateVolumeMutationParams) => {
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
      if (isError(response)) {
        // NOTE:
        // When an expected error occurs, as defined in the OpenAPI specification, the following line will
        // be executed. This block serves as a placeholder for expected errors.
        //
        // NOTE:
        // As of 14/12/2024, all properties of the models defined in the OpenAPI specification are marked as
        // optional. Consequently, any `object` can match the type of the `SuccessResponseBody` for any API
        // (effectively disabling meaningful type checking). In the future, as the OpenAPI specification is
        // updated, additional changes may be required for this type guard clause, such as incorporating
        // `return response.data` below.
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

export type UseDeleteVolumeArgs = PathParam<
  CatalogApi,
  '/volumes/{name}',
  'delete'
>;

export type DeleteVolumeMutationParams = PathParam<
  CatalogApi,
  '/volumes/{name}',
  'delete'
>;

export function useDeleteVolume({ name }: UseDeleteVolumeArgs) {
  const queryClient = useQueryClient();

  const [catalog, schema] = name.split('.');

  return useMutation<
    SuccessResponseBody<CatalogApi, '/volumes/{name}', 'delete'>,
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
      if (isError(response)) {
        // NOTE:
        // When an expected error occurs, as defined in the OpenAPI specification, the following line will
        // be executed. This block serves as a placeholder for expected errors.
        //
        // NOTE:
        // As of 14/12/2024, all properties of the models defined in the OpenAPI specification are marked as
        // optional. Consequently, any `object` can match the type of the `SuccessResponseBody` for any API
        // (effectively disabling meaningful type checking). In the future, as the OpenAPI specification is
        // updated, additional changes may be required for this type guard clause, such as incorporating
        // `return response.data` below.
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
