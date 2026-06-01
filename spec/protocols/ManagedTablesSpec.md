# Unity Catalog Managed Tables Specification

> **Status**: Proposed  
> **Version**: 0.2.0
> **Last Updated**: 2026-06-01

## Table of Contents

- [Introduction](#introduction)
- [Terminology](#terminology)
- [Changes from v0.1.0](#changes-from-v010)
- [APIs](#apis)
  - [Get Configuration](#get-configuration)
  - [Create a Staging Table](#create-a-staging-table)
  - [Get Staging Table Credentials](#get-staging-table-credentials)
  - [Create a Table](#create-a-table)
  - [Load a Table](#load-a-table)
  - [Get Table Credentials](#get-table-credentials)
  - [Write a Commit](#write-a-commit)
  - [Report Metrics](#report-metrics)
- [Error Response Body](#error-response-body)
---

## Introduction
This specification defines how Unity Catalog will serve as the managing catalog for [Catalog-Managed Tables](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#catalog-managed-tables).
Catalog-Managed Tables is a new Delta table feature which makes the catalog the source of truth for commits to a table.
It defines a managing catalog to perform commits according to the Catalog-Managed Tables specification.

This specification defines the APIs to enable the following operations between the server (Unity Catalog) and clients:
- Negotiating the API protocol version and discovering the supported endpoints.
- Creating a Catalog-Managed table with necessary table features and properties.
- Retrieving and reading from the table.
- Writing to the table and publishing commits.
- Obtaining temporary cloud storage credentials for reads and writes.
- Running maintenance operations.

### What is a Unity Catalog Managed Table?
A Unity Catalog managed table is a table whose data location and lifecycle (including deletion of files when dropped)
are fully controlled by Unity Catalog.
It is different from external tables, where the catalog only stores metadata pointing to data in an externally managed
location and does not delete the underlying files when the table is dropped.

Comparison with External Tables:

Feature | Managed Table | External Table
-|-|-
Storage ownership | Unity Catalog | User
Auto-cleanup on drop | Yes | No
Storage location | Assigned | User-specified


## Terminology
This section recaps key terminologies used in the Catalog-Managed Tables specification:
- **Commit**: A set of actions that moves a Delta table from version `v−1` to `v`, stored either as a Delta log file or inline in the catalog.
- **Staged commit**: A commit written to `_delta_log/_staged_commits/<v>.<uuid>.json`, not yet guaranteed to be accepted; the catalog decides whether to ratify it.
- **Ratified commit**: A proposed commit that the catalog has chosen as the winner for version `v`; it is part of the official history, whether or not it is already published.
- **Published commit**: A ratified commit that has been written as `_delta_log/<v>.json`; the presence of this file proves that version `v` is published.
- **Backfilled version**: The latest version for which all commits up to and including that version have been published to `_delta_log`.
- **Etag**: An opaque version token returned by the server for optimistic concurrency control. Clients echo the most recent etag in `assert-etag` requirements on update requests.

For further details, refer to the [Catalog-Managed Tables](https://github.com/delta-io/delta/blob/master/protocol_rfcs/catalog-managed.md).

## Changes from v0.1.0

v0.2.0 is a breaking revision over v0.1.0. The wire surface, endpoint set, and several semantics have changed:

- **Endpoint root**: prefix moved from `/api/2.1/unity-catalog` to `/api/2.1/unity-catalog/delta/v1`. Catalog and schema are now path parameters (`/catalogs/{catalog}/schemas/{schema}/...`) instead of being baked into a three-part `{full_name}`.
- **Field naming**: all wire field names are now hyphenated (e.g., `table-id`, `min-reader-version`, `last-commit-timestamp-ms`). The v0.1.0 snake_case names (`table_id`, `start_version`, etc.) are not accepted.
- **Schema representation**: table columns are expressed as a `DeltaStructType` (the same shape as `metaData.schemaString` in the Delta commit log), replacing the v0.1.0 `ColumnInfo` array.
- **`getCommits` endpoint removed**: ratified commits are now returned inline in the [`loadTable`](#load-a-table) response under the `commits` array. There is no separate `GET .../delta/commits`.
- **`commit` endpoint replaced**: the v0.1.0 `POST .../delta/commit` is replaced by [`updateTable`](#write-a-commit) (`POST .../catalogs/{catalog}/schemas/{schema}/tables/{table}`). Commits are now expressed as an `add-commit` action inside a `requirements` + `updates` envelope. Backfill notifications use the `set-latest-backfilled-version` action on the same endpoint.
- **Optimistic concurrency**: writes now require an `assert-table-uuid` precondition and may carry an `assert-etag` precondition built from the `etag` returned by [`loadTable`](#load-a-table).
- **Domain metadata**: clustering columns and row-tracking high-water-mark are no longer flat properties; they are carried in `domain-metadata` (on create) or via the `set-domain-metadata` action (on update).
- **Configuration discovery**: a new [`config`](#get-configuration) endpoint negotiates the protocol version and advertises supported endpoints. Clients should call it before issuing other requests.
- **Credential vending**: short-lived storage credentials are returned by [`createStagingTable`](#create-a-staging-table) for the initial commit and by [`getTableCredentials`](#get-table-credentials) / [`getStagingTableCredentials`](#get-staging-table-credentials) thereafter. Credentials are not embedded in `loadTable` responses.

## APIs
A Unity Catalog managed table needs to be registered and used according to the following specification:
- **Discover endpoints**: Before issuing any other request, the client sends a `GET` request to the [`config`](#get-configuration) API to negotiate the protocol version and learn which endpoints the server supports. The set of supported endpoints may evolve as the protocol version changes.
- **Create a table**: A Unity Catalog managed table needs to first be created.
  - The client sends a `POST` request to the [`createStagingTable`](#create-a-staging-table) API to initiate the creation of a new managed table in Unity Catalog.
  - The server, upon receiving the request to create a staging table, allocates a unique table ID and a storage location for the table, and returns the required / suggested protocol and properties the client must honor for the initial commit, along with short-lived storage credentials.
  - The client writes the first commit `0.json` under the `_delta_log` in the assigned staging location. The `0.json` file must include all table features and properties that the server advertises as required in the [`createStagingTable`](#create-a-staging-table) response. A reference list of features and properties that a client implementation is expected to be prepared to support is given in [Reference: Table Features and Properties for Unity Catalog Managed Tables](#reference-table-features-and-properties-for-unity-catalog-managed-tables); the authoritative set is always whatever the server returns at runtime. If the initial credentials expire before the client finishes writing, it can refresh them via the [`getStagingTableCredentials`](#get-staging-table-credentials) API. Then the client finishes the table creation by sending a `POST` request to the [`createTable`](#create-a-table) API.
    - For CTAS (Create Table As Select), the `0.json` file would also contain `AddFile` actions.
    - For REPLACE, do not drop and recreate the table. Instead, add a new commit that updates the table metadata and removes all existing data files. This approach ensures atomicity, preserves the table ID and history. The replaced table remains to be a Unity Catalog managed table.
  - When a create table request is received, the server first confirms the existence of a table with the specified table ID at the provided location. If the table has the Catalog-Managed feature enabled, the server then validates that all necessary table features and properties are correctly configured. Finally, the server registers the table as created at commit version `0`.
- **Get the table**: After the table is created successfully, the client can identify the table by its catalog, schema, and name to retrieve the table entry.
  - The client sends a `GET` request to the [`loadTable`](#load-a-table) API with the catalog, schema, and table name as URL path parameters.
  - The server responds with the table metadata (schema, properties, protocol, etag, table identity), all possibly-unbackfilled ratified commits in `commits`, and the `latest-table-version` tracked by Unity Catalog.
- **Read from the table**: To read from the table, the client needs to obtain all necessary commits to reconstruct the snapshot:
  - The client obtains all the published commits by listing all the files in `_delta_log`, and combines them with the unpublished ratified commits returned in the `commits` array from the [`loadTable`](#load-a-table) response.
    - For a newly created table, the server returns `latest-table-version` = `0` and an empty `commits` array when no commits have been made since version `0`.
  - The client combines the published commits, ratified commits and rebuilds the current snapshot. When building the snapshot, the client should not read any commits that go beyond `latest-table-version` returned by the server.
  - If the storage location requires vended credentials, the client requests them from the [`getTableCredentials`](#get-table-credentials) API with `operation=READ`.
- **Write to the table**:
  - The client should first write a staged commit file and then propose to the server to accept this commit by sending a `POST` request to the [`updateTable`](#write-a-commit) API. The request body specifies the `add-commit` action with the staged commit's metadata. If the commit contains table protocol, metadata, or domain-metadata changes, the client must include them as additional update actions in the same request. For UniForm / Iceberg-enabled tables, the request must include `uniform.iceberg` for every commit.
  - The server verifies that the version to commit equals the last ratified commit version + 1 and accepts the commit. If the request contains the field `uniform.iceberg`, the field is persisted as part of the table's latest state in the catalog.
  - After the commit is successful, the client must publish the ratified commit by copying the committed file to the `_delta_log` folder. After all commits up to this version have been published, the client should notify the server by sending the `set-latest-backfilled-version` action via the same [`updateTable`](#write-a-commit) API.
  - The server verifies that the version is equal or less than the last ratified version and optionally removes the entries for the ratified commits.
  - For write operations, the client requests credentials from the [`getTableCredentials`](#get-table-credentials) API with `operation=READ_WRITE`.
- **Metrics and running maintenance operations**: The client should send [metrics](#report-metrics) after each successful commit. These metrics may be used for Unity Catalog to schedule and manage maintenance operations. Unity Catalog enforces a maintenance policy for catalog-managed tables. By default, clients are only allowed to run checkpoints, log compaction, and version checksum. All other maintenance operations, such as metadata cleanup, OPTIMIZE, VACUUM, and REORG, are disallowed.

The rest of the specification outlines the APIs and detailed requirements. All the endpoints are prefixed with `/api/2.1/unity-catalog/delta/v1`.

### Get Configuration

```
GET .../config
```

Returns the catalog configuration and the set of endpoints supported by the server for the negotiated protocol version. The client declares the highest protocol versions it supports per major version, and the server selects the highest mutually supported version.

#### Request body
No request body. Parameters are passed as query string.

Field Name | Data Type | Description | Optional/Required
-|-|-|-
catalog | string | Catalog name. | required
protocol-versions | string | Comma-separated list of the highest protocol versions the client supports per major version (e.g., `"1.1,2.3"` means the client supports `1.0-1.1` and `2.0-2.3`). The server selects the highest mutually supported version and returns endpoints for that version. | required

#### Responses

**200: Request completed successfully.**

Field Name | Data Type | Description | Optional/Required
-|-|-|-
endpoints | array of string | List of supported endpoint signatures in the form `"METHOD /v1/path"`. Paths are relative to the catalog API root (`/api/2.1/unity-catalog/delta`); the prefix is not included in the returned strings. The server may return different endpoints depending on the negotiated protocol version. | required
protocol-version | string | The negotiated protocol version selected by the server (e.g., `"1.0"`). | required

#### Request-Response Samples

Request:
```
GET /api/2.1/unity-catalog/delta/v1/config?catalog=main&protocol-versions=1.0
```

Response: 200
```json
{
  "endpoints": [
    "POST /v1/catalogs/{catalog}/schemas/{schema}/staging-tables",
    "POST /v1/catalogs/{catalog}/schemas/{schema}/tables",
    "GET /v1/catalogs/{catalog}/schemas/{schema}/tables/{table}",
    "POST /v1/catalogs/{catalog}/schemas/{schema}/tables/{table}",
    "GET /v1/catalogs/{catalog}/schemas/{schema}/tables/{table}/credentials",
    "POST /v1/catalogs/{catalog}/schemas/{schema}/tables/{table}/metrics",
    "GET /v1/staging-tables/{table_id}/credentials"
  ],
  "protocol-version": "1.0"
}
```

#### Errors

Possible Errors on this API are listed below:

Error Type | HTTP Status | Description
-|-|-
BadRequestException | 400 | Missing or invalid query parameters.<br>The client should ensure both `catalog` and `protocol-versions` are provided and well-formed.
NoSuchCatalogException | 404 | The specified catalog does not exist.<br>The client should check the spelling of the catalog name or create the catalog first.

### Create a Staging Table

```
POST .../catalogs/{catalog}/schemas/{schema}/staging-tables
```

Proposes a staged table for Unity Catalog to allocate a table ID and storage location.
Once created, the table ID and location are not mutable.
A staged table is not yet a real table; as a result, it cannot be used with table APIs such as the `loadTable` API until it is promoted via `createTable`.

#### Path parameters

Field Name | Data Type | Description | Optional/Required
-|-|-|-
catalog | string | Name of the parent catalog where the staging table will be created. | required
schema | string | Name of the parent schema relative to its parent catalog. | required

#### Request body

Field Name | Data Type | Description | Optional/Required
-|-|-|-
name | string | Name of the staging table, relative to the parent schema. | required

#### Responses

**200: Request completed successfully.**

Field Name | Data Type | Description | Optional/Required
-|-|-|-
table-id | string | Unique identifier generated for the staging table. This can be used to reference the staging table in subsequent operations.<br>Type: UUID string. | required
table-type | string | Always `MANAGED` for staging tables. | required
location | string | URI generated for the staging table. This is the cloud storage location where data will be staged.<br>Format: URI string (`s3://`, `abfss://`, `gs://`, etc.). | required
storage-credentials | array of DeltaStorageCredential | Temporary credentials for writing the initial commit. See [Get Table Credentials](#get-table-credentials) for the `DeltaStorageCredential` schema. | required
required-protocol | DeltaProtocol | The minimum Delta protocol the client must write in the initial commit. | required
&nbsp;&nbsp;required-protocol.min-reader-version | int32 | Minimum reader version. | required
&nbsp;&nbsp;required-protocol.min-writer-version | int32 | Minimum writer version. | required
&nbsp;&nbsp;required-protocol.reader-features | array of string | Required reader features. | optional
&nbsp;&nbsp;required-protocol.writer-features | array of string | Required writer features. | optional
suggested-protocol | DeltaProtocol | Additional protocol features the client should enable if supported. Reader-writer features appear in both lists; writer-only features appear only in `writer-features`. Does not include version numbers. | optional
required-properties | object (string to string) | Non-protocol properties the client must set in `metaData.configuration`. Does not include protocol/feature properties (those are covered by `required-protocol`). | required
suggested-properties | object (string to string or null) | Non-protocol properties the client should set when enabling the corresponding suggested features. A `null` value means the client may generate any valid value (e.g., row-tracking materialized column names). | optional

#### Request-Response Samples
Request:
```
POST /api/2.1/unity-catalog/delta/v1/catalogs/main/schemas/default/staging-tables
Content-Type: application/json
```
```json
{
  "name": "my_table"
}
```
Response: 200
```json
{
  "table-id": "550e8400-e29b-41d4-a716-446655440000",
  "table-type": "MANAGED",
  "location": "s3://my_bucket/__unitystorage/tables/550e8400-e29b-41d4-a716-446655440000",
  "storage-credentials": [
    {
      "prefix": "s3://my_bucket/__unitystorage/tables/550e8400-e29b-41d4-a716-446655440000",
      "operation": "READ_WRITE",
      "expiration-time-ms": 1704153600000,
      "config": {
        "s3.access-key-id": "ASIA...",
        "s3.secret-access-key": "***",
        "s3.session-token": "***"
      }
    }
  ],
  "required-protocol": {
    "min-reader-version": 3,
    "min-writer-version": 7,
    "reader-features": ["catalogManaged", "deletionVectors", "v2Checkpoint", "vacuumProtocolCheck"],
    "writer-features": ["catalogManaged", "deletionVectors", "inCommitTimestamp", "v2Checkpoint", "vacuumProtocolCheck"]
  },
  "suggested-protocol": {
    "reader-features": ["columnMapping"],
    "writer-features": ["columnMapping", "domainMetadata", "rowTracking"]
  },
  "required-properties": {
    "delta.checkpointPolicy": "v2",
    "delta.enableDeletionVectors": "true",
    "delta.enableInCommitTimestamps": "true",
    "delta.checkpoint.writeStatsAsStruct": "true",
    "delta.checkpoint.writeStatsAsJson": "true",
    "io.unitycatalog.tableId": "550e8400-e29b-41d4-a716-446655440000"
  },
  "suggested-properties": {
    "delta.columnMapping.mode": "name",
    "delta.enableRowTracking": "true",
    "delta.randomizeFilePrefixes": "true",
    "delta.parquet.compression.codec": "zstd",
    "delta.rowTracking.materializedRowIdColumnName": null,
    "delta.rowTracking.materializedRowCommitVersionColumnName": null
  }
}
```
#### Errors
Possible Errors on this API are listed below:

Error Type | HTTP Status | Description
-|-|-
BadRequestException | 400 | Malformed request or invalid field values.<br>The client should check the request body format.
PermissionDeniedException | 403 | User lacks necessary permissions (CREATE on schema, USAGE on catalog/schema).<br>The client should check with their Unity Catalog Admin to grant them the necessary rights to create a staged table.
NoSuchSchemaException | 404 | The specified catalog or schema does not exist.<br>The client should check the spelling of the catalog and schema, or create them first.
AlreadyExistsException | 409 | A table with the same name already exists.<br>The client should provide a different table name.

### Get Staging Table Credentials

```
GET .../staging-tables/{table_id}/credentials
```

Returns temporary credentials for writing the initial commit at a staging table's location. The staging table is identified solely by its UUID; catalog and schema are not part of the path since the UUID is globally unique. Credentials are always `READ_WRITE` since the client needs to write the initial commit.

Typically used to refresh the credentials returned by [`createStagingTable`](#create-a-staging-table) when they expire before the client finishes writing `0.json`.

#### Path parameters

Field Name | Data Type | Description | Optional/Required
-|-|-|-
table_id | string | Staging table UUID (not name).<br>Type: UUID string. | required

#### Request body
No request body. No query parameters; credentials are always `READ_WRITE` for this endpoint.

#### Responses

**200: Request completed successfully.** Same response format as [Get Table Credentials](#get-table-credentials).

#### Request-Response Samples
Request:
```
GET /api/2.1/unity-catalog/delta/v1/staging-tables/550e8400-e29b-41d4-a716-446655440000/credentials
```
Response: 200
```json
{
  "storage-credentials": [
    {
      "prefix": "s3://my_bucket/__unitystorage/tables/550e8400-e29b-41d4-a716-446655440000",
      "operation": "READ_WRITE",
      "expiration-time-ms": 1704157200000,
      "config": {
        "s3.access-key-id": "ASIA...",
        "s3.secret-access-key": "***",
        "s3.session-token": "***"
      }
    }
  ]
}
```

#### Errors
Possible Errors on this API are listed below:

Error Type | HTTP Status | Description
-|-|-
PermissionDeniedException | 403 | User lacks necessary permissions.<br>Contact the admin for access.
NoSuchTableException | 404 | No staging table found with the specified UUID.

### Create a Table

```
POST .../catalogs/{catalog}/schemas/{schema}/tables
```

Creates a new Unity Catalog managed table in the specified catalog and schema, promoting a previously created staging table into a real catalog table. When creating the table, the `0.json` written by the client at the staging location must contain all the following table features and properties.

#### Reference: Table Features and Properties for Unity Catalog Managed Tables

*This section is a **reference** list of features and properties that a client implementation is expected to be prepared to support. It is **not** a normative part of this protocol specification. The authoritative set of required and suggested features and properties is determined by the server at runtime and advertised dynamically via the `required-protocol`, `required-properties`, `suggested-protocol`, and `suggested-properties` fields in the [`createStagingTable`](#create-a-staging-table) response. The server may grow or shrink the set across releases at its discretion. Clients must read those response fields rather than hard-coding the contents of this section.*

Table Feature | Table Property | Status
-|-|-
[`catalogManaged`](https://github.com/delta-io/delta/blob/master/protocol_rfcs/catalog-managed.md) | `io.unitycatalog.tableId = <tableID>` | Required
[`inCommitTimestamp`](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#in-commit-timestamps) | `delta.enableInCommitTimestamps = true` | Required (correctness of time travel)
[`vacuumProtocolCheck`](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#vacuum-protocol-check) | (none) | Required
[`v2Checkpoint`](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#v2-checkpoint-table-feature) | `delta.checkpointPolicy = v2` | Required
[`deletionVectors`](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#deletion-vectors) | `delta.enableDeletionVectors = true` | Required
[`columnMapping`](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#column-mapping) | `delta.columnMapping.mode = "name"` or `"id"`; companion `delta.columnMapping.maxColumnId` | Recommended
[`domainMetadata`](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#domain-metadata) | (none) | Recommended (required when `rowTracking` or `clustering` is enabled)
[`rowTracking`](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#row-tracking) | `delta.enableRowTracking = true`; companions `delta.rowTracking.materializedRowIdColumnName`, `delta.rowTracking.materializedRowCommitVersionColumnName` | Recommended

Other Protocol-Derived Properties:

Property Name | Description
-|-
`delta.feature.<feature-name> = "supported"` | One entry per supported table feature.
`delta.minReaderVersion = x` | `x` is the min reader version of the table, `x >= 3`.
`delta.minWriterVersion = y` | `y` is the min writer version of the table, `y >= 7`.

##### Required Property Values

Property Name | Required Value | Note
-|-|-
`delta.checkpoint.writeStatsAsStruct` | `"true"` | Struct-format stats in checkpoints for performant data skipping.
`delta.checkpoint.writeStatsAsJson` | `"true"` | JSON-format stats in checkpoints for engines without struct-format support.

##### Recommended Property Values

Property Name | Recommended Value | Note
-|-|-
`delta.randomizeFilePrefixes` | `"true"` | Improves write concurrency by randomizing object-store prefixes.
`delta.parquet.compression.codec` | `"zstd"` | Compression codec for new Parquet data and checkpoint files. Not yet in the OSS Delta spec but already honored by Delta Kernel; suggesting is harmless because clients that don't honor it can ignore it.

##### Restricted Properties

Some `delta.*` properties are not permitted at table creation. The server rejects them with an `InvalidParameterValueException` (HTTP 400) naming the offending key. The restricted set covers categories such as engine-internal write tuning, retention windows the catalog manages, cross-engine compatibility hazards, log-integrity / protocol invariants, and legacy or preview-only properties. The exact set evolves with the Delta protocol and UC policy; clients should rely on the server's rejection rather than maintaining their own list.

##### Clustering Columns

Clustering columns, if the [Clustered Table Feature](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#clustered-table) is supported, are carried in the `delta.clustering` domain-metadata entry on the `domain-metadata` request field instead of as a flat property. Column paths are serialized as a JSON array of string arrays. Each string array represents a path from the root schema to a column, with each element being a field name. For example, top-level column `id` and nested column `address.city` serialize to `[["id"], ["address", "city"]]`.

#### Path parameters

Field Name | Data Type | Description | Optional/Required
-|-|-|-
catalog | string | Name of the parent catalog where the table will be created. | required
schema | string | Name of the parent schema relative to its parent catalog. | required

#### Request body

Field Name | Data Type | Description | Optional/Required
-|-|-|-
name | string | Name of the table, relative to the parent schema. | required
table-type | string | The type of the table. For Unity Catalog managed tables, must be set to `MANAGED`. | required
location | string | For Unity Catalog managed tables, this must match the `location` returned by [`createStagingTable`](#create-a-staging-table).<br>Format: URI string (`s3://`, `abfss://`, `gs://`, etc.). | required
comment | string | Table comment. | optional
columns | DeltaStructType | The table's column definitions, expressed as a `DeltaStructType` (the same shape as `metaData.schemaString` in the Delta commit log). See [Column Schema](#column-schema) for the structure. | required
partition-columns | array of string | List of partition column names. | optional
properties | object (string to string) | Delta table properties as key-value pairs (from `metaData.configuration` in the Delta commit log). | required
protocol | DeltaProtocol | Delta protocol of the table. Same shape as `required-protocol` in the [`createStagingTable`](#create-a-staging-table) response. | required
domain-metadata | object (string to object) | Domain metadata keyed by domain name. Each value's structure follows the corresponding [Delta protocol domain metadata](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#domain-metadata) definition. | optional
last-commit-timestamp-ms | int64 | Timestamp of version 0 (the commit the client wrote at `location` before calling this endpoint), in epoch milliseconds. | required
uniform | DeltaUniformMetadata | Required if UniForm is enabled on the table after creation (i.e., the protocol / properties enable UniForm). See [Write a Commit](#write-a-commit) for the field shape. At create time the server validates: `iceberg.converted-delta-version` must be `0`; `iceberg.metadata-location` must be a subpath of `location`. | conditional

##### Column Schema

`columns` follows the `DeltaStructType` shape (with `fields` for nested fields). Top-level table columns live under the top-level `fields` array.

Field Name | Data Type | Description | Optional/Required
-|-|-|-
type | string | Must be `"struct"`. | required
fields | array of DeltaStructField | The table's columns (or nested fields, for nested structs). | required
&nbsp;&nbsp;fields[].name | string | Column name. Stored case-preserving; uniqueness within the parent `DeltaStructType` is enforced case-insensitively. | required
&nbsp;&nbsp;fields[].type | string or object | Column data type. For primitive types, a string (e.g., `"long"`, `"string"`, `"decimal(10,2)"`). For complex types, a nested object: `DeltaArrayType` (`{"type": "array", "element-type": ..., "contains-null": true}`), `DeltaMapType` (`{"type": "map", "key-type": ..., "value-type": ..., "value-contains-null": true}`), or `DeltaStructType` (`{"type": "struct", "fields": [...]}`). | required
&nbsp;&nbsp;fields[].nullable | boolean | Whether the column may be null. | required
&nbsp;&nbsp;fields[].metadata | object (string to any) | Spark/Delta field metadata as key-value pairs. Common keys include `"comment"` (string), `"delta.columnMapping.id"` (int), `"delta.columnMapping.physicalName"` (string). | required

#### Responses
**200: Request completed successfully.** Returns the same response as [Load a Table](#load-a-table).

#### Request-Response Samples
Request:
```
POST /api/2.1/unity-catalog/delta/v1/catalogs/main/schemas/default/tables
Content-Type: application/json
```
```json
{
  "name": "my_table",
  "table-type": "MANAGED",
  "location": "s3://my-bucket/__unitystorage/tables/550e8400-e29b-41d4-a716-446655440000",
  "columns": {
    "type": "struct",
    "fields": [
      {
        "name": "id",
        "type": "long",
        "nullable": true,
        "metadata": {}
      },
      {
        "name": "name",
        "type": "string",
        "nullable": true,
        "metadata": {}
      },
      {
        "name": "created_date",
        "type": "date",
        "nullable": true,
        "metadata": {}
      }
    ]
  },
  "partition-columns": ["created_date"],
  "properties": {
    "delta.enableDeletionVectors": "true",
    "delta.enableInCommitTimestamps": "true",
    "delta.checkpointPolicy": "v2",
    "delta.parquet.compression.codec": "zstd",
    "delta.checkpoint.writeStatsAsStruct": "true",
    "delta.checkpoint.writeStatsAsJson": "true",
    "io.unitycatalog.tableId": "550e8400-e29b-41d4-a716-446655440000"
  },
  "protocol": {
    "min-reader-version": 3,
    "min-writer-version": 7,
    "reader-features": ["catalogManaged", "deletionVectors", "v2Checkpoint", "vacuumProtocolCheck"],
    "writer-features": ["catalogManaged", "deletionVectors", "inCommitTimestamp", "v2Checkpoint", "vacuumProtocolCheck"]
  },
  "domain-metadata": {
    "delta.clustering": {
      "clusteringColumns": [["id"], ["created_date"]]
    }
  },
  "last-commit-timestamp-ms": 1704067400000
}
```

Response: 200 See [Load a Table](#load-a-table) response for the full response shape.

#### Errors

Possible Errors on this API are listed below:

Error Type | HTTP Status | Description
-|-|-
BadRequestException | 400 | Malformed request, missing required fields, or invalid field values.
InvalidParameterValueException | 400 | Server side check failed for the Unity Catalog Managed Delta Table, which could include but is not limited to (1) not a Delta Table, (2) missing required table features or properties, (3) location mismatch with the staging table.
PermissionDeniedException | 403 | User lacks necessary permissions (CREATE on schema, USAGE on catalog/schema).<br>The client should check with their Unity Catalog Admin to grant them the necessary rights to create a table.
NoSuchSchemaException | 404 | The specified catalog or schema doesn't exist.<br>The client should check the spelling of the catalog and schema, or create them first.
NoSuchTableException | 404 | The specified staging table doesn't exist.<br>The client should check the spelling of the table or create the staging table first.
AlreadyExistsException | 409 | A table with the same name already exists.<br>The client should provide a different table name.

### Load a Table

```
GET .../catalogs/{catalog}/schemas/{schema}/tables/{table}
```
Loads full table metadata, including columns, properties, protocol, all possibly-unbackfilled ratified commits, and the latest table version tracked by Unity Catalog. This is the primary endpoint for reading table state.

#### Path parameters

Field Name | Data Type | Description | Optional/Required
-|-|-|-
catalog | string | Catalog name. | required
schema | string | Schema name. | required
table | string | Table name. | required

#### Request body
No request body.

#### Responses

**200: Request completed successfully.**

Field Name | Data Type | Description | Optional/Required
-|-|-|-
metadata | object | Table metadata containing schema, protocol, properties, and table identity. Fields listed below. | required
&nbsp;&nbsp;metadata.etag | string | Opaque version token for optimistic concurrency. Echoed back in `assert-etag` requirements on update requests. | required
&nbsp;&nbsp;metadata.table-type | string | For Unity Catalog managed tables, `MANAGED`. | required
&nbsp;&nbsp;metadata.table-uuid | string | Unique table identifier.<br>Type: UUID string. | required
&nbsp;&nbsp;metadata.location | string | Storage location of the table. | required
&nbsp;&nbsp;metadata.created-time | int64 | Time the table was created, in epoch milliseconds. | required
&nbsp;&nbsp;metadata.updated-time | int64 | Time the table was last modified, in epoch milliseconds. | required
&nbsp;&nbsp;metadata.columns | DeltaStructType | The table's column definitions. See [Column Schema](#column-schema) for the structure.<br>Note: column comments are not included in the load response; clients read them directly from the Delta commit log. | required
&nbsp;&nbsp;metadata.partition-columns | array of string | List of partition column names. | optional
&nbsp;&nbsp;metadata.properties | object (string to string) | Table properties as key-value pairs. Includes protocol-derived properties (e.g., `delta.feature.catalogManaged`) and snapshot-derived properties (e.g., `delta.lastCommitTimestamp`, `delta.lastUpdateVersion`). | required
&nbsp;&nbsp;metadata.last-commit-version | int64 | The version of the last commit that changed table metadata (`delta.lastUpdateVersion`). Data-only commits do not update this value. Compare with `latest-table-version`, which tracks the latest commit overall. | required
&nbsp;&nbsp;metadata.last-commit-timestamp-ms | int64 | Timestamp of the last commit that updated table metadata in the catalog, in epoch milliseconds (`delta.lastCommitTimestamp`). | required
commits | array of DeltaCommit | All possibly-unbackfilled ratified commits, in descending version order (newest first). Empty array if no ratified commits exist beyond version 0. The server may bound the size of this list. Note: v0.2.0 does not define pagination for this field; the mechanism for fetching additional commits when the server bounds the response is to be specified in a future revision. | required
&nbsp;&nbsp;commits[].version | int64 | The commit's table version. Must be > 0. | required
&nbsp;&nbsp;commits[].timestamp | int64 | In-commit timestamp in milliseconds since epoch. | required
&nbsp;&nbsp;commits[].file-name | string | Commit filename (e.g., `"00000000000000000012.48ae7812-9cf2-4548-a859-fc64b89de294.json"`). | required
&nbsp;&nbsp;commits[].file-size | int64 | Size of the commit file in bytes. Must be > 0. | required
&nbsp;&nbsp;commits[].file-modification-timestamp | int64 | Filesystem modification time in milliseconds since epoch. | required
uniform | DeltaUniformMetadata | UniForm conversion metadata. Present only for UniForm-enabled tables. | optional
&nbsp;&nbsp;uniform.iceberg.metadata-location | string | Iceberg metadata location converted up to `iceberg.converted-delta-version`. | required
&nbsp;&nbsp;uniform.iceberg.converted-delta-version | int64 | The Delta version under conversion. | required
&nbsp;&nbsp;uniform.iceberg.converted-delta-timestamp | int64 | Timestamp of the conversion, in epoch milliseconds. | required
&nbsp;&nbsp;uniform.iceberg.base-converted-delta-version | int64 | Optional Delta version used to incrementally convert Delta changes to Iceberg changes to produce the latest Iceberg metadata at `metadata-location`. | optional
latest-table-version | int64 | The latest version tracked by Unity Catalog for this table, including data-only commits. Clients use this to know when they have all commits and cannot load a table version beyond this. `0` is returned for a newly created managed table that has only the initial commit (version `0`) and no subsequent ratified commits. | required

**Note:** Vended cloud storage credentials are not included in this response. Clients obtain credentials with the appropriate operation level (`READ` or `READ_WRITE`) via the dedicated [Get Table Credentials](#get-table-credentials) endpoint.

#### Request-Response Samples

Request:
```
GET /api/2.1/unity-catalog/delta/v1/catalogs/main/schemas/default/tables/my_table
```

Response: 200

```json
{
  "metadata": {
    "etag": "CAESCAAAAZuhmep0",
    "table-type": "MANAGED",
    "table-uuid": "550e8400-e29b-41d4-a716-446655440000",
    "location": "s3://my-bucket/__unitystorage/tables/550e8400-e29b-41d4-a716-446655440000",
    "created-time": 1705600000000,
    "updated-time": 1705600000000,
    "columns": {
      "type": "struct",
      "fields": [
        {"name": "id", "type": "long", "nullable": true, "metadata": {}},
        {"name": "name", "type": "string", "nullable": true, "metadata": {}},
        {"name": "created_date", "type": "date", "nullable": true, "metadata": {}}
      ]
    },
    "partition-columns": ["created_date"],
    "properties": {
      "delta.minReaderVersion": "3",
      "delta.minWriterVersion": "7",
      "delta.enableDeletionVectors": "true",
      "delta.enableInCommitTimestamps": "true",
      "delta.checkpointPolicy": "v2",
      "delta.parquet.compression.codec": "zstd",
      "delta.checkpoint.writeStatsAsStruct": "true",
      "delta.checkpoint.writeStatsAsJson": "true",
      "delta.feature.catalogManaged": "supported",
      "delta.feature.deletionVectors": "supported",
      "delta.feature.inCommitTimestamp": "supported",
      "delta.feature.v2Checkpoint": "supported",
      "delta.feature.vacuumProtocolCheck": "supported",
      "io.unitycatalog.tableId": "550e8400-e29b-41d4-a716-446655440000",
      "delta.lastUpdateVersion": "0",
      "delta.lastCommitTimestamp": "1704067400000"
    },
    "last-commit-version": 0,
    "last-commit-timestamp-ms": 1704067400000
  },
  "commits": [
    {
      "version": 44,
      "timestamp": 1704067400000,
      "file-name": "00000000000000000044.00000000-0000-0000-0000-00000000002c.json",
      "file-size": 1536,
      "file-modification-timestamp": 1704067400000
    },
    {
      "version": 43,
      "timestamp": 1704067300000,
      "file-name": "00000000000000000043.00000000-0000-0000-0000-00000000002b.json",
      "file-size": 3072,
      "file-modification-timestamp": 1704067300000
    }
  ],
  "latest-table-version": 44
}
```

#### Errors

Possible Errors on this API are listed below:

Error Type | HTTP Status | Description
-|-|-
UnsupportedTableFormatException | 400 | The table is not a Delta table, or a Delta table not yet supported by this endpoint.<br>The client should fall back to the existing UC table API.
PermissionDeniedException | 403 | User lacks necessary permissions (SELECT on the table, USAGE on catalog/schema).<br>The client should check with their Unity Catalog Admin to grant them the necessary rights to query the table.
NoSuchSchemaException | 404 | The specified catalog or schema doesn't exist.<br>The client should check the spelling of the catalog and schema, or create them first.
NoSuchTableException | 404 | The specified table doesn't exist.<br>The client should check the spelling of the table or create the table first.

### Get Table Credentials

```
GET .../catalogs/{catalog}/schemas/{schema}/tables/{table}/credentials
```

Gets temporary credentials for accessing table data (vended credentials). The `operation` query parameter controls the access level: `READ` requires `SELECT` privilege; `READ_WRITE` requires `MODIFY` privilege. The server enforces authorization based on this parameter.

#### Path parameters

Field Name | Data Type | Description | Optional/Required
-|-|-|-
catalog | string | Catalog name. | required
schema | string | Schema name. | required
table | string | Table name. | required

#### Request body
No request body. Parameters are passed as query string.

Field Name | Data Type | Description | Optional/Required
-|-|-|-
operation | string | The operation the credential is scoped to. One of `READ`, `READ_WRITE`. | required

#### Responses

**200: Request completed successfully.**

Field Name | Data Type | Description | Optional/Required
-|-|-|-
storage-credentials | array of DeltaStorageCredential | Temporary cloud storage credentials. | required
&nbsp;&nbsp;storage-credentials[].prefix | string | The storage path prefix this credential is valid for.<br>Format: URI string (`s3://`, `abfss://`, `gs://`, etc.). | required
&nbsp;&nbsp;storage-credentials[].operation | string | The permission level of this credential. One of `READ`, `READ_WRITE`. | required
&nbsp;&nbsp;storage-credentials[].expiration-time-ms | int64 | Credential expiration time, in epoch milliseconds. | required
&nbsp;&nbsp;storage-credentials[].config | object (string to string) | Cloud provider-specific credential configuration. Only keys for the relevant provider are present. Common keys: `s3.access-key-id`, `s3.secret-access-key`, `s3.session-token`, `azure.sas-token`, `gcs.oauth-token`. | required

#### Request-Response Samples

Request:
```
GET /api/2.1/unity-catalog/delta/v1/catalogs/main/schemas/default/tables/my_table/credentials?operation=READ
```

Response: 200
```json
{
  "storage-credentials": [
    {
      "prefix": "s3://my-bucket/__unitystorage/tables/550e8400-e29b-41d4-a716-446655440000",
      "operation": "READ",
      "expiration-time-ms": 1704153600000,
      "config": {
        "s3.access-key-id": "ASIA...",
        "s3.secret-access-key": "***",
        "s3.session-token": "***"
      }
    }
  ]
}
```

#### Errors

Possible Errors on this API are listed below:

Error Type | HTTP Status | Description
-|-|-
BadRequestException | 400 | Missing or invalid `operation` parameter. Must be one of `READ`, `READ_WRITE`.
PermissionDeniedException | 403 | User lacks necessary permissions (`SELECT` for `READ`, `MODIFY` for `READ_WRITE`).<br>Contact the admin for access.
NoSuchTableException | 404 | The specified table does not exist.

### Write a Commit

```
POST .../catalogs/{catalog}/schemas/{schema}/tables/{table}
```

Proposes a staged commit to the catalog and / or informs Unity Catalog that commits have been published. There is no standalone Commit API in this specification: the same endpoint serves both commit operations and other table metadata updates. For the managed-table commit flow, the client uses the `add-commit` action and, after publishing the ratified commit, the `set-latest-backfilled-version` action.

For UniForm / Iceberg-enabled tables, the request body must atomically include the `uniform.iceberg` field alongside the `add-commit` action for every commit. Writers cannot commit to a UniForm-enabled table without it.

#### Path parameters

Field Name | Data Type | Description | Optional/Required
-|-|-|-
catalog | string | Catalog name. | required
schema | string | Schema name. | required
table | string | Table name. | required

#### Request body

Field Name | Data Type | Description | Optional/Required
-|-|-|-
requirements | array of object | List of precondition assertions. All must hold for the update to succeed. An `assert-table-uuid` requirement is required for every request; `assert-etag` is optional. The server also applies an implicit assertion that the existing table version is `v-1` when committing version `v`. | required
&nbsp;&nbsp;requirements[].type | string | Assertion type. One of `assert-table-uuid`, `assert-etag`. | required
&nbsp;&nbsp;requirements[].uuid | string | For `assert-table-uuid`: the expected table UUID. | conditional
&nbsp;&nbsp;requirements[].etag | string | For `assert-etag`: the most recent etag returned by the server. | conditional
updates | array of object | List of update actions to apply. Each action has an `action` field that selects its type plus the action-specific fields listed below. Multiple actions may be combined in a single request and apply atomically. | required

##### Update actions

`add-commit` and `set-latest-backfilled-version` are the two actions specific to the catalog-managed commit lifecycle. The remaining actions in this table are used to express table-metadata changes that accompany a commit (schema evolution, property updates, protocol upgrades, clustering changes); they must be combined with `add-commit` in the same request when the resulting commit changes table metadata, so the catalog state and the Delta log stay in lockstep.

This specification covers only the actions used by managed tables. The same endpoint also accepts external-table-specific actions (e.g., `update-metadata-snapshot-version`); those are out of scope for this document.

Action | Description | Key Fields
-|-|-
`add-commit` | Add a new catalog-managed commit. Managed tables only. The commit `version` carries an implicit assertion that the existing version must be `v-1`. | `commit` (DeltaCommit, required): commit info. `uniform` (DeltaUniformMetadata, conditional): required only for UniForm-enabled tables.
`set-latest-backfilled-version` | Report the last backfilled / published commit version. Used after the client publishes a ratified commit to `_delta_log`. | `latest-published-version` (int64, required): the version that has been published.
`set-properties` | Set or update one or more table properties. | `updates` (object: string to string, required): key-value pairs to set.
`remove-properties` | Remove one or more table properties. | `removals` (array of string, required): property keys to remove.
`set-protocol` | Update the Delta protocol (e.g., to enable a new table feature). | `protocol` (DeltaProtocol, required): the new protocol. Same shape as `protocol` in [Create a Table](#create-a-table).
`set-columns` | Replace the table's column definitions. | `columns` (DeltaStructType, required): new column definitions. Same shape as `columns` in [Create a Table](#create-a-table).
`set-partition-columns` | Replace the partition column list. | `partition-columns` (array of string, required): partition column names.
`set-table-comment` | Update the table comment. | `comment` (string, required): new comment text.
`set-domain-metadata` | Set domain metadata. Only domains present in `updates` are modified; absent domains are untouched. Used to set or update clustering columns (`delta.clustering`) and row-tracking high-water-mark (`delta.rowTracking`). | `updates` (object: string to object, required): a map of domain name to domain-specific metadata object. Each value's structure follows the corresponding [Delta protocol domain metadata](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#domain-metadata) definition.
`remove-domain-metadata` | Remove domain metadata for specific domains. | `domains` (array of string, required): domain names to remove.

##### DeltaCommit (under `add-commit.commit`)

Field Name | Data Type | Description | Optional/Required
-|-|-|-
version | int64 | The table version to commit. Must be > 0. | required
timestamp | int64 | In-commit timestamp in milliseconds since epoch. | required
file-name | string | UUID-based commit filename (e.g., `"00000000000000000042.a1b2c3d4-5678-9abc-def0-123456789abc.json"`). | required
file-size | int64 | Size of the commit file in bytes. Must be > 0. | required
file-modification-timestamp | int64 | Filesystem modification time in milliseconds since epoch. | required

##### DeltaUniformMetadata (under `add-commit.uniform`)

Field Name | Data Type | Description | Optional/Required
-|-|-|-
iceberg.metadata-location | string | Iceberg metadata location converted up to the Delta version. Must be a subpath of the table's storage root. | required
iceberg.converted-delta-version | int64 | The Delta version under conversion. | required
iceberg.converted-delta-timestamp | int64 | Timestamp of the conversion, in epoch milliseconds. | required
iceberg.base-converted-delta-version | int64 | Optional Delta version used to incrementally convert Delta changes to Iceberg changes to produce the latest Iceberg metadata at `metadata-location`. Supplying this opts into sequential validation against the currently-stored `converted-delta-version`. | optional

#### Responses

**200: Request completed successfully.** Returns the same response as [Load a Table](#load-a-table).

#### Request-Response Samples

1. Propose a simple staged commit

    Request:
    ```
    POST /api/2.1/unity-catalog/delta/v1/catalogs/main/schemas/default/tables/my_table
    Content-Type: application/json
    ```
    ```json
    {
      "requirements": [
        {
          "type": "assert-table-uuid",
          "uuid": "550e8400-e29b-41d4-a716-446655440000"
        }
      ],
      "updates": [
        {
          "action": "add-commit",
          "commit": {
            "version": 42,
            "timestamp": 1704067200000,
            "file-name": "00000000000000000042.a1b2c3d4-5678-9abc-def0-123456789abc.json",
            "file-size": 2048,
            "file-modification-timestamp": 1704067200000
          }
        }
      ]
    }
    ```

    Response: 200 See [Load a Table](#load-a-table) response for the full response shape.

2. Propose a new staged commit with a metadata change

    Request:
    ```json
    {
      "requirements": [
        {
          "type": "assert-table-uuid",
          "uuid": "550e8400-e29b-41d4-a716-446655440000"
        },
        {
          "type": "assert-etag",
          "etag": "CAESCAAAAZuhmep0"
        }
      ],
      "updates": [
        {
          "action": "set-table-comment",
          "comment": "Updated table description"
        },
        {
          "action": "set-columns",
          "columns": {
            "type": "struct",
            "fields": [
              {"name": "id", "type": "long", "nullable": false, "metadata": {}},
              {"name": "name", "type": "string", "nullable": true, "metadata": {}},
              {"name": "created_at", "type": "timestamp", "nullable": false, "metadata": {}}
            ]
          }
        },
        {
          "action": "add-commit",
          "commit": {
            "version": 43,
            "timestamp": 1704067300000,
            "file-name": "00000000000000000043.a1b2c3d4-5678-9abc-def0-123456789abc.json",
            "file-size": 3072,
            "file-modification-timestamp": 1704067300000
          }
        }
      ]
    }
    ```
    Response: 200 See [Load a Table](#load-a-table) response for the full response shape.

3. Notify the catalog of the latest published version

    Request:
    ```json
    {
      "requirements": [
        {
          "type": "assert-table-uuid",
          "uuid": "550e8400-e29b-41d4-a716-446655440000"
        }
      ],
      "updates": [
        {
          "action": "set-latest-backfilled-version",
          "latest-published-version": 42
        }
      ]
    }
    ```
    Response: 200 See [Load a Table](#load-a-table) response for the full response shape.

4. Propose a new staged commit that updates clustering columns

    Request:
    ```json
    {
      "requirements": [
        {
          "type": "assert-table-uuid",
          "uuid": "550e8400-e29b-41d4-a716-446655440000"
        }
      ],
      "updates": [
        {
          "action": "set-domain-metadata",
          "updates": {
            "delta.clustering": {
              "clusteringColumns": [["id"], ["created_date"]]
            }
          }
        },
        {
          "action": "add-commit",
          "commit": {
            "version": 44,
            "timestamp": 1704067500000,
            "file-name": "00000000000000000044.7f834a99-c4b5-41e7-9c25-26a4d0c14e7e.json",
            "file-size": 1792,
            "file-modification-timestamp": 1704067500000
          }
        }
      ]
    }
    ```
    Response: 200 See [Load a Table](#load-a-table) response for the full response shape.

5. Propose a new staged commit with UniForm Iceberg changes

    Request:
    ```json
    {
      "requirements": [
        {
          "type": "assert-table-uuid",
          "uuid": "550e8400-e29b-41d4-a716-446655440000"
        }
      ],
      "updates": [
        {
          "action": "add-commit",
          "commit": {
            "version": 42,
            "timestamp": 1704067200000,
            "file-name": "00000000000000000042.a1b2c3d4-5678-9abc-def0-123456789abc.json",
            "file-size": 2048,
            "file-modification-timestamp": 1704067200000
          },
          "uniform": {
            "iceberg": {
              "metadata-location": "s3://my-bucket/__unitystorage/tables/550e8400-e29b-41d4-a716-446655440000/metadata/00042-xxxx.metadata.json",
              "converted-delta-version": 42,
              "converted-delta-timestamp": 1704067200000
            }
          }
        }
      ]
    }
    ```
    Response: 200 See [Load a Table](#load-a-table) response for the full response shape.

#### Errors

Possible Errors on this API are listed below:

Error Type | HTTP Status | Description
-|-|-
BadRequestException | 400 | Malformed request or invalid action.<br>The client should check the request body structure and action types.
InvalidParameterValueException | 400 | Server side check failed. Examples: invalid `uniform.iceberg`; missing `uniform.iceberg` when the post-commit snapshot has UniForm enabled; non-positive version / timestamp / file size; missing required fields.<br>The client should validate the request shape and field values before retrying.
NoSuchTableException | 404 | The specified table doesn't exist.<br>The client should check if the table ID is correct for this table.
CommitVersionConflictException | 409 | Commit rejected due to a concurrent conflicting commit.<br>The client should follow the Delta protocol to rebuild the snapshot and retry the commit with a new version.
UpdateRequirementConflictException | 409 | An `assert-etag` or `assert-table-uuid` requirement was not met, or `uniform.iceberg.base-converted-delta-version` was supplied and does not match the current stored `converted-delta-version` (sequential-validation violation).<br>The client should reload the table and retry with the current etag / `converted-delta-version`.
ResourceExhaustedException | 429 | The maximum number of unbackfilled commits has been reached.<br>The client should backfill pending commits before retrying.
CommitStateUnknownException | 500 | Commit outcome unknown (e.g., network failure after the server received the request).<br>The client should check the table state before retrying.

### Report Metrics

```
POST .../catalogs/{catalog}/schemas/{schema}/tables/{table}/metrics
```

Reports commit metrics (telemetry) for a table. The catalog uses these metrics to schedule maintenance operations. The path identifies the table by name; the body's `table-id` must match the same table's UUID. The server validates this and rejects mismatches.

#### Path parameters

Field Name | Data Type | Description | Optional/Required
-|-|-|-
catalog | string | Catalog name. | required
schema | string | Schema name. | required
table | string | Table name. | required

#### Request body

Field Name | Data Type | Description | Optional/Required
-|-|-|-
table-id | string | Table UUID. Must match the table identified by the path `{catalog}/{schema}/{table}`. Obtained from the [`loadTable`](#load-a-table) response (`metadata.table-uuid`).<br>Format: UUID string. | required
report | object | Container for the metrics being reported. | optional
&nbsp;&nbsp;report.commit-report | object | Commit report metrics. | optional
&nbsp;&nbsp;&nbsp;&nbsp;report.commit-report.num-files-added | int64 | Number of files added by this commit. | optional
&nbsp;&nbsp;&nbsp;&nbsp;report.commit-report.num-bytes-added | int64 | Number of bytes added by this commit. | optional
&nbsp;&nbsp;&nbsp;&nbsp;report.commit-report.num-files-removed | int64 | Number of files removed by this commit. | optional
&nbsp;&nbsp;&nbsp;&nbsp;report.commit-report.num-bytes-removed | int64 | Number of bytes removed by this commit. | optional
&nbsp;&nbsp;&nbsp;&nbsp;report.commit-report.num-rows-inserted | int64 | Number of rows inserted by this commit. | optional
&nbsp;&nbsp;&nbsp;&nbsp;report.commit-report.num-rows-removed | int64 | Number of rows removed by this commit. | optional
&nbsp;&nbsp;&nbsp;&nbsp;report.commit-report.num-rows-updated | int64 | Number of rows updated by this commit. | optional
&nbsp;&nbsp;&nbsp;&nbsp;report.commit-report.file-size-histogram | object | Histogram tracking file counts and total bytes across size ranges. | optional
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;report.commit-report.file-size-histogram.sorted-bin-boundaries | array of int64 | Sorted bin boundaries: each element is the start of a bin (inclusive) and the next element is the end (exclusive). The first element must be `0`. | required
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;report.commit-report.file-size-histogram.file-counts | array of int32 | Count of files in each bin. Length must match `sorted-bin-boundaries`. | required
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;report.commit-report.file-size-histogram.total-bytes | array of int64 | Total bytes in each bin. Length must match `sorted-bin-boundaries`. | required
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;report.commit-report.file-size-histogram.commit-version | int64 | The commit version this histogram is for. | optional

#### Responses

**204: Metrics received successfully.** No response body.

#### Request-Response Samples

Request:
```
POST /api/2.1/unity-catalog/delta/v1/catalogs/main/schemas/default/tables/my_table/metrics
Content-Type: application/json
```
```json
{
  "table-id": "550e8400-e29b-41d4-a716-446655440000",
  "report": {
    "commit-report": {
      "num-files-added": 10,
      "num-bytes-added": 104857600,
      "num-files-removed": 2,
      "num-bytes-removed": 8192,
      "num-rows-inserted": 1000000,
      "num-rows-removed": 50000,
      "num-rows-updated": 25000,
      "file-size-histogram": {
        "sorted-bin-boundaries": [0, 1024, 2048],
        "file-counts": [100, 40, 5],
        "total-bytes": [104857600, 167772160, 83886080],
        "commit-version": 6
      }
    }
  }
}
```
Response: 204 No Content

#### Errors

Possible Errors on this API are listed below:

Error Type | HTTP Status | Description
-|-|-
BadRequestException | 400 | Malformed request body.<br>The client should check the request body structure.
InvalidParameterValueException | 400 | Server-side check failed. Examples: `table-id` does not match the table at the path; missing required histogram fields; histogram array lengths do not match.<br>The client should validate the request shape and field values before retrying.
PermissionDeniedException | 403 | User lacks permission to report metrics on this table.<br>Contact the admin for access.
NoSuchTableException | 404 | The specified table does not exist.<br>The client should check that the path identifies an existing table.

## Error Response Body

All non-2xx responses return an `error` object with the fields below.

Field Name | Data Type | Description | Optional/Required
-|-|-|-
type | string | Error-type name. One of the values listed in the per-endpoint Errors tables above (e.g., `NoSuchTableException`, `BadRequestException`). | required
code | int32 | HTTP status code (400-599). | required
message | string | Human-readable description of the error. | required
stack | array of string | Stack trace, when the server elects to include one for debugging. | optional

Example:
```json
{
  "error": {
    "type": "NoSuchTableException",
    "code": 404,
    "message": "Table 'main.default.my_table' does not exist"
  }
}
```

## API Reference

Method | HTTP request | Description
-|-|-
**getConfig** | **GET** /config | [Get Configuration](#get-configuration)
**createStagingTable** | **POST** /catalogs/{catalog}/schemas/{schema}/staging-tables | [Create a Staging Table](#create-a-staging-table)
**getStagingTableCredentials** | **GET** /staging-tables/{table_id}/credentials | [Get Staging Table Credentials](#get-staging-table-credentials)
**createTable** | **POST** /catalogs/{catalog}/schemas/{schema}/tables | [Create a Table](#create-a-table)
**loadTable** | **GET** /catalogs/{catalog}/schemas/{schema}/tables/{table} | [Load a Table](#load-a-table)
**getTableCredentials** | **GET** /catalogs/{catalog}/schemas/{schema}/tables/{table}/credentials | [Get Table Credentials](#get-table-credentials)
**updateTable** | **POST** /catalogs/{catalog}/schemas/{schema}/tables/{table} | [Write a Commit](#write-a-commit)
**reportMetrics** | **POST** /catalogs/{catalog}/schemas/{schema}/tables/{table}/metrics | [Report Metrics](#report-metrics)

For full API documentation index, see [Delta APIs README](../../api/delta-docs/README.md).

---

[[Back to Main README]](../../README.md)

