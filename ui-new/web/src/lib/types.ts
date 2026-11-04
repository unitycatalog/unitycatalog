// Lightweight Unity Catalog model shapes covering the fields the browsing UI
// renders. These mirror the server's *Info API models (io.unitycatalog.server.
// model.*) but intentionally type only what the UI reads, rather than pulling
// the full generated OpenAPI client. Extend as pages need more fields.

export interface Named {
  name?: string;
  full_name?: string;
}

export interface CatalogInfo {
  name: string;
  comment?: string;
  properties?: Record<string, string>;
  owner?: string;
  created_at?: number;
  updated_at?: number;
  id?: string;
}

export interface SchemaInfo {
  name: string;
  catalog_name: string;
  full_name?: string;
  comment?: string;
  properties?: Record<string, string>;
  owner?: string;
  created_at?: number;
  updated_at?: number;
  schema_id?: string;
}

export interface ColumnInfo {
  name: string;
  type_text?: string;
  type_name?: string;
  type_json?: string;
  comment?: string;
  nullable?: boolean;
  position?: number;
}

export type TableType = "MANAGED" | "EXTERNAL";
export type DataSourceFormat =
  | "DELTA"
  | "CSV"
  | "JSON"
  | "AVRO"
  | "PARQUET"
  | "ORC"
  | "TEXT"
  | string;

export interface TableInfo {
  name: string;
  catalog_name: string;
  schema_name: string;
  full_name?: string;
  table_type?: TableType;
  data_source_format?: DataSourceFormat;
  columns?: ColumnInfo[];
  storage_location?: string;
  comment?: string;
  properties?: Record<string, string>;
  owner?: string;
  created_at?: number;
  updated_at?: number;
  table_id?: string;
}

export interface VolumeInfo {
  name: string;
  catalog_name: string;
  schema_name: string;
  full_name?: string;
  volume_type?: "MANAGED" | "EXTERNAL";
  storage_location?: string;
  comment?: string;
  owner?: string;
  created_at?: number;
  updated_at?: number;
  volume_id?: string;
}

export interface FunctionParameterInfo {
  name: string;
  type_text?: string;
  type_name?: string;
  position?: number;
  comment?: string;
}

export interface FunctionInfo {
  name: string;
  catalog_name: string;
  schema_name: string;
  full_name?: string;
  comment?: string;
  data_type?: string;
  full_data_type?: string;
  input_params?: { parameters?: FunctionParameterInfo[] };
  routine_definition?: string;
  owner?: string;
  created_at?: number;
  updated_at?: number;
  function_id?: string;
}

export interface ModelInfo {
  name: string;
  catalog_name: string;
  schema_name: string;
  full_name?: string;
  comment?: string;
  owner?: string;
  created_at?: number;
  updated_at?: number;
  id?: string;
}

export interface ModelVersionInfo {
  model_name: string;
  catalog_name: string;
  schema_name: string;
  version: number;
  status?: string;
  source?: string;
  run_id?: string;
  comment?: string;
  storage_location?: string;
  created_at?: number;
  updated_at?: number;
  id?: string;
}

// List endpoint envelopes.
export interface ListCatalogsResponse {
  catalogs?: CatalogInfo[];
  next_page_token?: string;
}
export interface ListSchemasResponse {
  schemas?: SchemaInfo[];
  next_page_token?: string;
}
export interface ListTablesResponse {
  tables?: TableInfo[];
  next_page_token?: string;
}
export interface ListVolumesResponse {
  volumes?: VolumeInfo[];
  next_page_token?: string;
}
export interface ListFunctionsResponse {
  functions?: FunctionInfo[];
  next_page_token?: string;
}
export interface ListModelsResponse {
  registered_models?: ModelInfo[];
  next_page_token?: string;
}
export interface ListModelVersionsResponse {
  model_versions?: ModelVersionInfo[];
  next_page_token?: string;
}
