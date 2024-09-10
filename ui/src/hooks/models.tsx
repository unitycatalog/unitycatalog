import { useQuery, UseQueryOptions } from '@tanstack/react-query';
import apiClient from '../context/client';

export interface ModelInterface {
  name: string;
  catalog_name: string;
  schema_name: string;
  storage_location: string;
  comment: string;
  full_name: string;
  created_at: number;
  created_by: string;
  updated_at: number | null;
  updated_by: string | null;
  registered_model_id: string;
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

export function useGetModelVersions({
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

export function useGetVersion({
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
