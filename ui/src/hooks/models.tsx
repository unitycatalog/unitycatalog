import {
  useMutation,
  useQuery,
  useQueryClient,
  UseQueryOptions,
} from '@tanstack/react-query';
import apiClient from '../context/client';

export interface ModelInterface {
  name: string;
  catalog_name: string;
  schema_name: string;
  storage_location: string;
  comment: string;
  full_name: string;
  created_at: number;
  created_by: string | null;
  updated_at: number | null;
  updated_by: string | null;
  registered_model_id: string;
  owner: string | null;
}

export enum ModelVersionStatus {
  PENDING_REGISTRATION = 'PENDING_REGISTRATION',
  FAILED_REGISTRATION = 'FAILED_REGISTRATION',
  READY = 'READY',
}

export interface ModelVersionInterface {
  model_name: string;
  catalog_name: string;
  schema_name: string;
  version: number | null;
  source: string;
  run_id: string;
  storage_location: string;
  status: ModelVersionStatus;
  comment: string;
  created_at: number;
  created_by: string;
  updated_at: number | null;
  updated_by: string | null;
  registered_model_id: string;
}

interface ListModelsResponse {
  registered_models: ModelInterface[];
  next_page_token: string | null;
}

interface ListModelVersionsResponse {
  model_versions: ModelVersionInterface[];
  next_page_token: string | null;
}

interface ListModelsParams {
  catalog: string;
  schema: string;
  options?: Omit<UseQueryOptions<ListModelsResponse>, 'queryKey' | 'queryFn'>;
}

export function useListModels({ catalog, schema, options }: ListModelsParams) {
  return useQuery<ListModelsResponse>({
    queryKey: ['listModels', catalog, schema],
    queryFn: async () => {
      const searchParams = new URLSearchParams({
        catalog_name: catalog,
        schema_name: schema,
      });

      return apiClient
        .get(`/models?${searchParams.toString()}`)
        .then((response) => response.data);
    },
    ...options,
  });
}

interface GetModelParams {
  catalog: string;
  schema: string;
  model: string;
}

export function useGetModel({ catalog, schema, model }: GetModelParams) {
  return useQuery<ModelInterface>({
    queryKey: ['getModel', catalog, schema, model],
    queryFn: async () => {
      const fullModelName = [catalog, schema, model].join('.');

      return apiClient
        .get(`/models/${fullModelName}`)
        .then((response) => response.data);
    },
  });
}

interface ListModelVersionsParams {
  catalog: string;
  schema: string;
  model: string;
}

export function useListModelVersions({
  catalog,
  schema,
  model,
}: ListModelVersionsParams) {
  return useQuery<ListModelVersionsResponse>({
    queryKey: ['listModelVersions', catalog, schema, model],
    queryFn: async () => {
      const fullModelName = [catalog, schema, model].join('.');

      return apiClient
        .get(`/models/${fullModelName}/versions`)
        .then((response) => response.data);
    },
  });
}

interface GetVersionParams {
  catalog: string;
  schema: string;
  model: string;
  version: string;
}

export function useGetModelVersion({
  catalog,
  schema,
  model,
  version,
}: GetVersionParams) {
  return useQuery<ModelVersionInterface>({
    queryKey: ['getVersion', catalog, schema, model, version],
    queryFn: async () => {
      const fullModelName = [catalog, schema, model].join('.');

      return apiClient
        .get(`/models/${fullModelName}/versions/${version}`)
        .then((response) => response.data);
    },
  });
}

export interface DeleteModelVersionMutationParams
  extends Pick<
    ModelVersionInterface,
    'catalog_name' | 'schema_name' | 'model_name' | 'version'
  > {}

interface DeleteModelVersionParams {
  catalog: string;
  schema: string;
  model: string;
  version: number;
}

// Delete a model version
export function useDeleteModelVersion({
  catalog,
  schema,
  model,
  version,
}: DeleteModelVersionParams) {
  const queryClient = useQueryClient();

  return useMutation<void, Error, DeleteModelVersionMutationParams>({
    mutationFn: async ({
      catalog_name,
      schema_name,
      model_name,
      version,
    }: DeleteModelVersionMutationParams) => {
      const fullName = [catalog_name, schema_name, model_name].join('.');
      return apiClient
        .delete(`/models/${fullName}/versions/${version}`)
        .then((response) => response.data)
        .catch((e) => {
          throw new Error(
            e.response?.data?.message || 'Failed to delete model version',
          );
        });
    },
    onSuccess: () => {
      queryClient.invalidateQueries({
        queryKey: ['listModelVersions', catalog, schema, model],
      });
    },
  });
}

export interface DeleteModelMutationParams
  extends Pick<ModelInterface, 'catalog_name' | 'schema_name' | 'name'> {}

interface DeleteModelParams {
  catalog: string;
  schema: string;
  model: string;
}

// Delete a model
export function useDeleteModel({ catalog, schema, model }: DeleteModelParams) {
  const queryClient = useQueryClient();

  return useMutation<void, Error, DeleteModelMutationParams>({
    mutationFn: async ({
      catalog_name,
      schema_name,
      name,
    }: DeleteModelMutationParams) => {
      const fullName = [catalog_name, schema_name, name].join('.');
      return apiClient
        .delete(`/models/${fullName}`)
        .then((response) => response.data)
        .catch((e) => {
          throw new Error(
            e.response?.data?.message || 'Failed to delete model',
          );
        });
    },
    onSuccess: () => {
      queryClient.invalidateQueries({
        queryKey: ['listModels', catalog, schema],
      });
    },
  });
}

// update model version
interface UpdateModelVersionParams {
  catalog: string;
  schema: string;
  model: string;
  version: number;
}
export interface UpdateModelVersionMutationParams
  extends Pick<ModelVersionInterface, 'comment'> {}

export function useUpdateModelVersion({
  catalog,
  schema,
  model,
  version,
}: UpdateModelVersionParams) {
  const queryClient = useQueryClient();

  return useMutation<
    ModelVersionInterface,
    Error,
    UpdateModelVersionMutationParams
  >({
    mutationFn: async (params: UpdateModelVersionMutationParams) => {
      const fullName = [catalog, schema, model].join('.');

      return apiClient
        .patch(
          `/models/${fullName}/versions/${version}`,
          JSON.stringify({ ...params, full_name: fullName, version: version }), // remove full_name and version from params when path variables are fixed
        )
        .then((response) => response.data)
        .catch((e) => {
          throw new Error(
            e.response?.data?.message || 'Failed to update model version',
          );
        });
    },
    onSuccess: () => {
      queryClient.invalidateQueries({
        queryKey: ['getVersion', catalog, schema, model, version],
      });
    },
  });
}
