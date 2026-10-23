/*
 * Copyright 2026 The Unity Catalog Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.unitycatalog.server.auth;

/**
 * Operation identified by an endpoint's authorization annotation, shared across API protocols.
 *
 * <p>Request variants such as credential read/write scope or an Iceberg staged create are carried
 * by the authorization context alongside the action.
 */
public enum AuthorizationAction {
  /** No operation metadata; the existing authorization expression still applies. */
  UNSPECIFIED,
  COMMIT_TABLE,
  CREATE_CATALOG,
  CREATE_CREDENTIAL,
  CREATE_EXTERNAL_LOCATION,
  CREATE_FUNCTION,
  CREATE_MODEL_VERSION,
  CREATE_REGISTERED_MODEL,
  CREATE_SCHEMA,
  CREATE_STAGING_TABLE,
  CREATE_TABLE,
  CREATE_USER,
  CREATE_VOLUME,
  DELETE_CATALOG,
  DELETE_CREDENTIAL,
  DELETE_EXTERNAL_LOCATION,
  DELETE_FUNCTION,
  DELETE_MODEL_VERSION,
  DELETE_REGISTERED_MODEL,
  DELETE_SCHEMA,
  DELETE_TABLE,
  DELETE_USER,
  DELETE_VOLUME,
  FINALIZE_MODEL_VERSION,
  GENERATE_MODEL_VERSION_CREDENTIALS,
  GENERATE_PATH_CREDENTIALS,
  GENERATE_STAGING_TABLE_CREDENTIALS,
  GENERATE_TABLE_CREDENTIALS,
  GENERATE_VOLUME_CREDENTIALS,
  GET_CATALOG,
  GET_CATALOG_CONFIG,
  GET_CREDENTIAL,
  GET_EXTERNAL_LOCATION,
  GET_FUNCTION,
  GET_MODEL_VERSION,
  GET_REGISTERED_MODEL,
  GET_SCHEMA,
  GET_TABLE,
  GET_TABLE_COMMITS,
  GET_USER,
  GET_VIEW,
  GET_VOLUME,
  LIST_CATALOGS,
  LIST_CREDENTIALS,
  LIST_EXTERNAL_LOCATIONS,
  LIST_FUNCTIONS,
  LIST_MODEL_VERSIONS,
  LIST_REGISTERED_MODELS,
  LIST_SCHEMAS,
  LIST_TABLES,
  LIST_USERS,
  LIST_VOLUMES,
  RENAME_TABLE,
  REPORT_TABLE_COMMIT_METRICS,
  REPORT_TABLE_METRICS,
  SCHEMA_EXISTS,
  TABLE_EXISTS,
  UPDATE_CATALOG,
  UPDATE_CATALOG_PERMISSIONS,
  UPDATE_CREDENTIAL,
  UPDATE_CREDENTIAL_PERMISSIONS,
  UPDATE_EXTERNAL_LOCATION,
  UPDATE_EXTERNAL_LOCATION_PERMISSIONS,
  UPDATE_FUNCTION_PERMISSIONS,
  UPDATE_METASTORE_PERMISSIONS,
  UPDATE_MODEL_VERSION,
  UPDATE_REGISTERED_MODEL,
  UPDATE_REGISTERED_MODEL_PERMISSIONS,
  UPDATE_SCHEMA,
  UPDATE_SCHEMA_PERMISSIONS,
  UPDATE_TABLE_PERMISSIONS,
  UPDATE_USER,
  UPDATE_VOLUME,
  UPDATE_VOLUME_PERMISSIONS
}
