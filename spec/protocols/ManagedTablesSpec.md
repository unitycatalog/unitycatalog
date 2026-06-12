# Unity Catalog Managed Tables Specification

> **Spec Version**: 1.0
> **Last Updated**: 2026-06-11

## Table of Contents

- [Introduction](#introduction)
- [Terminology](#terminology)
- [Changes from last version](#changes-from-last-version)
- [APIs](#apis)
  - [Get Configuration](#get-configuration)
  - [Create a Staging Table](#create-a-staging-table)
  - [Get Staging Table Credentials](#get-staging-table-credentials)
  - [Create a Table](#create-a-table)
  - [Load a Table](#load-a-table)
  - [Get Table Credentials](#get-table-credentials)
  - [Write a Commit](#write-a-commit)
  - [Report Metrics](#report-metrics)
  - [Table Lifecycle Operations](#table-lifecycle-operations)
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

For further details, refer to the [Catalog-Managed Tables](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#catalog-managed-tables).

## Changes from last version

v1.0 is a breaking revision over last version. The wire surface, endpoint set, and several semantics have changed:

- **Endpoint root**: prefix moved from `/api/2.1/unity-catalog` to `/api/2.1/unity-catalog/delta/v1`. Catalog and schema are now path parameters (`/catalogs/{catalog}/schemas/{schema}/...`) instead of being baked into a three-part `{full_name}`.
- **Field naming**: all wire field names are now hyphenated (e.g., `table-id`, `min-reader-version`, `last-commit-timestamp-ms`). The v0.1.0 snake_case names (`table_id`, `start_version`, etc.) are not accepted.
- **Schema representation**: table columns are expressed as a `DeltaStructType` (the same shape as `metaData.schemaString` in the Delta commit log), replacing the v0.1.0 `ColumnInfo` array.
- **`getCommits` endpoint removed**: ratified commits are now returned inline in the [`loadTable`](#load-a-table) response under the `commits` array. There is no separate `GET .../delta/commits` and no pagination: the server bounds the number of unbackfilled commits at write time (see [Write a Commit](#write-a-commit)), so the `commits` array is always complete.
- **`commit` endpoint replaced**: the v0.1.0 `POST .../delta/commit` is replaced by [`updateTable`](#write-a-commit) (`POST .../catalogs/{catalog}/schemas/{schema}/tables/{table}`). Commits are now expressed as an `add-commit` action inside a `requirements` + `updates` envelope. Backfill notifications use the `set-latest-backfilled-version` action on the same endpoint.
- **Optimistic concurrency**: writes now require an `assert-table-uuid` precondition and may carry an `assert-etag` precondition built from the `etag` returned by [`loadTable`](#load-a-table).
- **Domain metadata**: clustering columns and row-tracking high-water-mark are no longer flat properties; they are carried in `domain-metadata` (on create) or via the `set-domain-metadata` action (on update).
- **Configuration discovery**: a new [`config`](#get-configuration) endpoint negotiates the protocol version and advertises supported endpoints. Clients should call it before issuing other requests.
- **Credential vending**: short-lived storage credentials are returned by [`createStagingTable`](#create-a-staging-table) for the initial commit and by [`getTableCredentials`](#get-table-credentials) / [`getStagingTableCredentials`](#get-staging-table-credentials) thereafter. Credentials are not embedded in `loadTable` responses.

## APIs

Scope and conventions that apply to every endpoint in this document:

- **Authentication**: all endpoints use the same OAuth 2.0 bearer token authentication as the rest of the Unity Catalog API; clients send an `Authorization: Bearer <token>` header on every request. Authentication is not redefined by this specification.
- **Staged commits only**: this protocol version supports only staged (file-based) commits. Inline commits (commit content stored in the catalog) are not supported; this is why `file-name` and `file-size` are required on every `DeltaCommit`.
- **Creation-only enablement**: tables become catalog-managed through this API only via the [`createStagingTable`](#create-a-staging-table) / [`createTable`](#create-a-table) flow. This protocol version defines no operation for enabling `catalogManaged` on an existing table (onboarding) and no operation for removing the feature from a managed table.
- **Scope**: the same API family also serves external Delta tables (EXTERNAL `createTable`, the `update-metadata-snapshot-version` action, `GET .../temporary-path-credentials`) and generic table lifecycle operations. Servers may therefore advertise endpoints beyond the ones specified here; clients must ignore endpoint signatures they do not recognize. This document is normative for managed tables only.

A Unity Catalog managed table needs to be registered and used according to the following specification:
- **Discover endpoints**: Before issuing any other request, the client sends a `GET` request to the [`config`](#get-configuration) API to negotiate the protocol version and learn which endpoints the server supports. The set of supported endpoints may evolve as the protocol version changes.
- **Create a table**: A Unity Catalog managed table needs to first be created.
  - The client sends a `POST` request to the [`createStagingTable`](#create-a-staging-table) API to initiate the creation of a new managed table in Unity Catalog.
  - The server, upon receiving the request to create a staging table, allocates a unique table ID and a storage location for the table, and returns the required / suggested protocol and properties the client must honor for the initial commit, along with short-lived storage credentials.
  - The client writes the first commit `0.json` under the `_delta_log` in the assigned staging location. The `0.json` file must include all table features and properties that the server advertises as required in the [`createStagingTable`](#create-a-staging-table) response. A reference list of features and properties that a client implementation is expected to be prepared to support is given in [Reference: Table Features and Properties for Unity Catalog Managed Tables](#reference-table-features-and-properties-for-unity-catalog-managed-tables); the authoritative set is always whatever the server returns at runtime. If the initial credentials expire before the client finishes writing, it can refresh them via the [`getStagingTableCredentials`](#get-staging-table-credentials) API. Then the client finishes the table creation by sending a `POST` request to the [`createTable`](#create-a-table) API.
    - For CTAS (Create Table As Select), the `0.json` file would also contain `AddFile` actions.
    - For REPLACE, do not drop and recreate the table. Instead, add a new commit that updates the table metadata and removes all existing data files. This approach ensures atomicity, preserves the table ID and history. The replaced table remains to be a Unity Catalog managed table. The update actions that express this commit are specified in [REPLACE TABLE](#replace-table).
  - When a create table request is received, the server resolves the staging table allocated for the provided location, verifies that the caller is the same principal that created the staging table, and validates the request's declared protocol, properties, and domain metadata against the managed-table contract (including that the `io.unitycatalog.tableId` property matches the UC-allocated table ID). The server may additionally read the Delta log at the staging location to verify the declarations against the written `0.json`, but is not required to: the client must ensure the `0.json` it wrote matches its declarations regardless (see [Catalog and log consistency](#catalog-and-log-consistency)). Finally, the server registers the table as created at commit version `0`. A staging table can be finalized at most once; a second `createTable` against the same location is rejected.
- **Get the table**: After the table is created successfully, the client can identify the table by its catalog, schema, and name to retrieve the table entry.
  - The client sends a `GET` request to the [`loadTable`](#load-a-table) API with the catalog, schema, and table name as URL path parameters.
  - The server responds with the table metadata (schema, properties including protocol-derived entries, etag, table identity), all possibly-unbackfilled ratified commits in `commits`, and the `latest-table-version` tracked by Unity Catalog.
- **Read from the table**: To read from the table, the client needs to obtain all necessary commits to reconstruct the snapshot:
  - The client obtains all the published commits by listing all the files in `_delta_log`, and combines them with the unpublished ratified commits returned in the `commits` array from the [`loadTable`](#load-a-table) response. The `commits` array is complete and contiguous (see [Load a Table](#load-a-table)), so the union of published commits and the returned ratified commits always covers every version up to `latest-table-version`. Each entry's `file-name` names a staged commit file under `_delta_log/_staged_commits/`.
  - If a version appears both in the `commits` array and as a published `_delta_log/<v>.json` file (for example, the commit was published but the catalog was never notified), the catalog's entry is authoritative: per the Delta protocol's reader requirements, the client must read the staged commit file named by the catalog and ignore the published file for that version.
    - For a newly created table, the server returns `latest-table-version` = `0` and an empty `commits` array when no commits have been made since version `0`.
  - The client combines the published commits, ratified commits and rebuilds the current snapshot. When building the snapshot, the client must ignore any commits beyond `latest-table-version` returned by the server, even if later files are already visible in `_delta_log`.
  - Managed-table storage locations are owned by Unity Catalog and are not expected to be directly accessible with ambient credentials: clients should always obtain vended credentials from the [`getTableCredentials`](#get-table-credentials) API with `operation=READ` before reading data files or the Delta log.
- **Write to the table**:
  - The client should first write a staged commit file and then propose to the server to accept this commit by sending a `POST` request to the [`updateTable`](#write-a-commit) API. The request body specifies the `add-commit` action with the staged commit's metadata. If the commit contains table protocol, metadata, or domain-metadata changes, the client must include them as additional update actions in the same request (see [Catalog and log consistency](#catalog-and-log-consistency)). For UniForm / Iceberg-enabled tables, the `add-commit` action must carry the nested `uniform` field for every commit.
  - Staged commit files must satisfy the writer requirements of the Delta [Catalog-Managed Tables](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#catalog-managed-tables) specification. In particular, `commitInfo` must contain a unique `txnId` (commit provenance), and the in-commit timestamp must be strictly greater than the previous version's timestamp (clients typically use `max(wall clock, previous ICT + 1)`). The catalog validates version sequencing and may additionally validate timestamp monotonicity; clients must not rely on the latter check, since a client that violates it corrupts its own table.
  - The server verifies that the version to commit equals the last ratified commit version + 1 and accepts the commit. Since [`createTable`](#create-a-table) registers the table at commit version `0`, this sequencing applies from the start: the first `add-commit` must propose version `1`. If the `add-commit` action contains the `uniform` field, the field is persisted as part of the table's latest state in the catalog.
  - After the commit is successful, the client must publish the ratified commit by copying the committed file to the `_delta_log` folder. Commits must be published in version order. After all commits up to a version have been published, the client should notify the server by sending the `set-latest-backfilled-version` action via the same [`updateTable`](#write-a-commit) API. See [Publishing and backfill](#publishing-and-backfill) for the safety rules around this notification.
  - The server verifies that the reported version is equal or less than the last ratified version and may remove the catalog entries for the published commits (reporting a version that is already backfilled is a no-op).
  - For write operations, the client requests credentials from the [`getTableCredentials`](#get-table-credentials) API with `operation=READ_WRITE`.
- **Metrics and running maintenance operations**: The client should send [metrics](#report-metrics) after each successful commit. These metrics may be used for Unity Catalog to schedule and manage maintenance operations. The [Delta protocol](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#maintenance-operations-on-catalog-managed-tables) prohibits maintenance operations on catalog-managed tables unless the managing catalog explicitly permits them, with checkpoints, log compaction, and version checksum as the only default exceptions. Per the same protocol section, checkpoints and log compaction files may only target versions that are already published in `_delta_log`, while version checksum files may also be written for versions not yet published. Unity Catalog does not permit any additional operations in this protocol version: metadata cleanup, VACUUM, OPTIMIZE, REORG, and all other maintenance operations remain prohibited. Statistics collection that writes results into the catalog (e.g., ANALYZE) is likewise unsupported, as this protocol version defines no API for it. There is no per-table policy or API for the catalog to grant more; clients must assume this default.

The rest of the specification outlines the APIs and detailed requirements. All the endpoints are prefixed with `/api/2.1/unity-catalog/delta/v1`.

### Get Configuration

```
GET .../config
```

Returns the catalog configuration and the set of endpoints supported by the server for the negotiated protocol version. The client declares the highest protocol versions it supports per major version, and the server selects the highest mutually supported version.

Version negotiation and bootstrap:

- The returned endpoint signatures carry their own version segment (`/v1/...` for protocol version `1.0`); clients route by the returned strings rather than by constructing paths, so the path layout under any other protocol version is whatever the server advertises for it.
- The `config` endpoint at `GET /delta/v1/config` is the bootstrap. A `404` on `config` means the server does not support this protocol; the client should fall back to the pre-existing Unity Catalog table APIs.
- If the client and server share no protocol version, the server rejects the request with `InvalidParameterValueException` (400) and an error message naming the protocol versions it supports. The client should treat this like an unsupported server.
- Servers may advertise endpoints beyond the ones defined in this document (for example external-table operations); clients must ignore endpoint signatures they do not recognize.

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
    "DELETE /v1/catalogs/{catalog}/schemas/{schema}/tables/{table}",
    "HEAD /v1/catalogs/{catalog}/schemas/{schema}/tables/{table}",
    "POST /v1/catalogs/{catalog}/schemas/{schema}/tables/{table}/rename",
    "GET /v1/catalogs/{catalog}/schemas/{schema}/tables/{table}/credentials",
    "POST /v1/catalogs/{catalog}/schemas/{schema}/tables/{table}/metrics",
    "GET /v1/staging-tables/{table_id}/credentials",
    "GET /v1/temporary-path-credentials"
  ],
  "protocol-version": "1.0"
}
```

In the example, `GET .../temporary-path-credentials` serves external-table creation and is outside the scope of this document; `DELETE`, `HEAD`, and `rename` are described in [Table Lifecycle Operations](#table-lifecycle-operations).

#### Errors

Possible Errors on this API are listed below:

Error Type | HTTP Status | Description
-|-|-
InvalidParameterValueException | 400 | Missing or invalid query parameters, or no mutually supported protocol version (the error message names the versions the server supports).<br>The client should ensure both `catalog` and `protocol-versions` are provided and well-formed.
NoSuchCatalogException | 404 | The specified catalog does not exist.<br>The client should check the spelling of the catalog name or create the catalog first.

### Create a Staging Table

```
POST .../catalogs/{catalog}/schemas/{schema}/staging-tables
```

Proposes a staged table for Unity Catalog to allocate a table ID and storage location.
Once created, the table ID and location are not mutable.
A staged table is not yet a real table; as a result, it cannot be used with table APIs such as the `loadTable` API until it is promoted via `createTable`.

Staging table lifecycle:

- The request is rejected with `AlreadyExistsException` if a real table with the same name already exists, but the staging table name is otherwise not reserved: multiple staging tables with the same name may coexist (each with its own table ID and location), and final name uniqueness is enforced at [`createTable`](#create-a-table) time.
- The staging table is keyed by its allocated location: `createTable` resolves the staging table by the `location` field, and only the principal that created the staging table may finalize it.
- A staging table can be finalized at most once.
- This protocol version defines no staging-table expiry, no automatic cleanup, and no operation to delete or abandon a staging table: clients should not rely on an abandoned staging table (for example, after the writer crashed before `createTable`) or the files written at its location being reclaimed.
- A `createStagingTable` retried after an unknown outcome simply allocates a fresh staging table with a new ID and location; any allocation from the earlier attempt is abandoned and falls under the no-cleanup rule above.

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
required-properties | object (string to string or null) | Non-protocol properties the client must set in `metaData.configuration`. Does not include protocol/feature properties (those are covered by `required-protocol`). A `null` value means the key is required but its value is engine-generated: the client must substitute a valid value at commit time and echo the key with a non-null value in the [`createTable`](#create-a-table) request. | required
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
    "delta.columnMapping.maxColumnId": null,
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
PermissionDeniedException | 403 | User lacks necessary permissions (`USE CATALOG` on the catalog; `USE SCHEMA` and `CREATE TABLE` on the schema, or schema ownership).<br>The client should check with their Unity Catalog Admin to grant them the necessary rights to create a staged table.
NoSuchSchemaException | 404 | The specified catalog or schema does not exist.<br>The client should check the spelling of the catalog and schema, or create them first.
AlreadyExistsException | 409 | A table with the same name already exists.<br>The client should provide a different table name.

### Get Staging Table Credentials

```
GET .../staging-tables/{table_id}/credentials
```

Returns temporary credentials for writing the initial commit at a staging table's location. The staging table is identified solely by its UUID; catalog and schema are not part of the path since the UUID is globally unique. Credentials are always `READ_WRITE` since the client needs to write the initial commit. Only the principal that created the staging table may obtain credentials for it, matching the rule that only the creator can finalize it.

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
PermissionDeniedException | 403 | The caller is not the principal that created the staging table.
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
[`catalogManaged`](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#catalog-managed-tables) | `io.unitycatalog.tableId = <tableID>` | Required
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

Some `delta.*` properties may not be permitted at table creation. A server that restricts a property rejects it with an `InvalidParameterValueException` (HTTP 400) naming the offending key. The restricted set is implementation-defined and covers categories such as engine-internal write tuning, retention windows the catalog manages, cross-engine compatibility hazards, log-integrity / protocol invariants, and legacy or preview-only properties. The exact set evolves with the Delta protocol and catalog policy; clients should rely on the server's rejection rather than maintaining their own list.

##### Clustering Columns

Clustering columns, if the [Clustered Table Feature](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#clustered-table) is supported, are carried in the `delta.clustering` domain-metadata entry on the `domain-metadata` request field instead of as a flat property. Column paths are serialized as a JSON array of string arrays. Each string array represents a path from the root schema to a column, with each element being a field name. For example, top-level column `id` and nested column `address.city` serialize to `[["id"], ["address", "city"]]`. When column mapping is enabled, the path elements are physical column names, matching the Delta clustered-table specification; `partition-columns`, by contrast, always uses the logical (schema) column names from `metaData.partitionColumns`.

#### Path parameters

Field Name | Data Type | Description | Optional/Required
-|-|-|-
catalog | string | Name of the parent catalog where the table will be created. | required
schema | string | Name of the parent schema relative to its parent catalog. | required

#### Request body

Field Name | Data Type | Description | Optional/Required
-|-|-|-
name | string | Name of the table, relative to the parent schema. Should be the same name used at [`createStagingTable`](#create-a-staging-table). Name uniqueness is enforced here (not at staging time). | required
table-type | string | The type of the table. For Unity Catalog managed tables, must be set to `MANAGED`. | required
location | string | For Unity Catalog managed tables, this must match the `location` returned by [`createStagingTable`](#create-a-staging-table); it is how the server resolves the staging table being finalized.<br>Format: URI string (`s3://`, `abfss://`, `gs://`, etc.). | required
comment | string | Table comment. | optional
columns | DeltaStructType | The table's column definitions, expressed as a `DeltaStructType` (the same shape as `metaData.schemaString` in the Delta commit log). See [Column Schema](#column-schema) for the structure. | required
partition-columns | array of string | List of partition column names. | optional
properties | object (string to string) | Delta table properties as key-value pairs (from `metaData.configuration` in the Delta commit log). | required
protocol | DeltaProtocol | Delta protocol of the table. Same shape as `required-protocol` in the [`createStagingTable`](#create-a-staging-table) response. | required
domain-metadata | object (string to object) | Domain metadata keyed by domain name. Each value's structure follows the corresponding [Delta protocol domain metadata](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#domain-metadata) definition. | optional
last-commit-timestamp-ms | int64 | Timestamp of version 0 (the commit the client wrote at `location` before calling this endpoint), in epoch milliseconds. | required
uniform | DeltaUniformMetadata | Required if and only if the table's properties enable UniForm (`delta.universalFormat.enabledFormats` includes `iceberg`). See [Write a Commit](#write-a-commit) for the field shape. At create time the server validates: `iceberg.converted-delta-version` must be `0`; `iceberg.metadata-location` must be a subpath of `location`. | conditional

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

##### Create recovery

A staging table can be finalized at most once, so a `createTable` retried after an unknown outcome (timeout, network failure) can be rejected even though the original attempt succeeded. The client disambiguates as follows:

1. Call [`loadTable`](#load-a-table) with the target name.
2. If the table exists and its `metadata.table-uuid` equals the staging `table-id`, the original `createTable` succeeded: only the staging table's creator can finalize it, so no other principal could have produced this table. Treat the create as complete.
3. Otherwise the rejection is genuine; resolve the underlying error (for example, a name conflict with an unrelated table) before retrying.

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
    "writer-features": ["catalogManaged", "clustering", "deletionVectors", "domainMetadata", "inCommitTimestamp", "v2Checkpoint", "vacuumProtocolCheck"]
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
InvalidParameterValueException | 400 | Server side check failed for the Unity Catalog Managed Delta Table, which could include but is not limited to (1) missing required table features or properties, (2) `io.unitycatalog.tableId` missing or not matching the staging table's ID, (3) invalid or inconsistent UniForm metadata, (4) the staging table was already finalized (see [Create recovery](#create-recovery) before treating this as a failure).
PermissionDeniedException | 403 | User lacks necessary permissions (`USE CATALOG` on the catalog; `USE SCHEMA` and `CREATE TABLE` on the schema, or schema ownership).<br>The client should check with their Unity Catalog Admin to grant them the necessary rights to create a table.
NoSuchSchemaException | 404 | The specified catalog or schema doesn't exist.<br>The client should check the spelling of the catalog and schema, or create them first.
NoSuchTableException | 404 | No staging table exists at the provided `location`.<br>The client should call [`createStagingTable`](#create-a-staging-table) first and pass back the location it returned.
AlreadyExistsException | 409 | A table with the same name already exists.<br>The client should provide a different table name.

### Load a Table

```
GET .../catalogs/{catalog}/schemas/{schema}/tables/{table}
```
Loads full table metadata, including columns, properties, protocol, all possibly-unbackfilled ratified commits, and the latest table version tracked by Unity Catalog. This is the primary endpoint for reading table state.

A lightweight existence check is available at `HEAD` on the same path; it returns `204` (exists) or `404` without a body (see [Table Lifecycle Operations](#table-lifecycle-operations)).

This response also serves as the [table discovery](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#table-discovery) step of the Delta protocol: the table is catalog-managed, and the client must follow the catalog-managed read and write rules, if and only if `metadata.properties` carries `delta.feature.catalogManaged = "supported"`. A Delta table without that entry (for example, an external Delta table served by this same endpoint) follows ordinary filesystem-based Delta access, and its `commits` / `latest-table-version` fields are absent.

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
&nbsp;&nbsp;metadata.etag | string | Opaque version token for optimistic concurrency. Echoed back in `assert-etag` requirements on update requests. The etag changes only on metadata changes (e.g., `set-columns`, `set-properties`, `set-protocol`, `set-domain-metadata`, `set-table-comment`); data-only `add-commit` requests and backfill notifications do not change it, so an `assert-etag` requirement does not spuriously fail under concurrent data ingest. | required
&nbsp;&nbsp;metadata.table-type | string | For Unity Catalog managed tables, `MANAGED`. | required
&nbsp;&nbsp;metadata.table-uuid | string | Unique table identifier.<br>Type: UUID string. | required
&nbsp;&nbsp;metadata.location | string | Storage location of the table. | required
&nbsp;&nbsp;metadata.created-time | int64 | Time the table was created, in epoch milliseconds. | required
&nbsp;&nbsp;metadata.updated-time | int64 | Time the table was last modified, in epoch milliseconds. | required
&nbsp;&nbsp;metadata.columns | DeltaStructType | The table's column definitions. See [Column Schema](#column-schema) for the structure.<br>Note: a column comment supplied by the client in a field's `metadata` (`comment` key) is stored and returned as part of `columns`; the catalog does not synthesize column comments from any other source. | required
&nbsp;&nbsp;metadata.partition-columns | array of string | List of partition column names. Always the logical (schema) column names from `metaData.partitionColumns`, even when column mapping is enabled. | optional
&nbsp;&nbsp;metadata.properties | object (string to string) | Table properties as key-value pairs. In addition to the table's real `metaData.configuration` entries, the map carries server-derived keys (see the synthetic-key list below this table). | required
&nbsp;&nbsp;metadata.last-commit-version | int64 | The version of the last commit that changed table metadata (`delta.lastUpdateVersion`). Data-only commits do not update this value. Compare with `latest-table-version`, which tracks the latest commit overall. | required
&nbsp;&nbsp;metadata.last-commit-timestamp-ms | int64 | Timestamp of the last commit that updated table metadata in the catalog, in epoch milliseconds (`delta.lastCommitTimestamp`). | required
commits | array of DeltaCommit | All possibly-unbackfilled ratified commits, in descending version order (newest first). Empty array if no ratified commits exist beyond version 0. The list is complete and contiguous: it covers every ratified version after the latest backfilled version, up to and including `latest-table-version`, and is returned atomically with `metadata` and `latest-table-version` from a single consistent snapshot of catalog state. The server never truncates this list; its size is instead bounded at write time, because the server rejects new commits with `ResourceExhaustedException` once the unbackfilled-commit limit is reached (see [Write a Commit](#write-a-commit)). Since clients are expected to backfill proactively, the list stays short in practice, and there is no pagination or version filtering. | required
&nbsp;&nbsp;commits[].version | int64 | The commit's table version. Must be > 0. | required
&nbsp;&nbsp;commits[].timestamp | int64 | In-commit timestamp in milliseconds since epoch. | required
&nbsp;&nbsp;commits[].file-name | string | Name of the staged commit file under `_delta_log/_staged_commits/` (e.g., `"00000000000000000012.48ae7812-9cf2-4548-a859-fc64b89de294.json"`). When the same version is also published as `_delta_log/<v>.json`, this catalog-named file is the authoritative copy to read. | required
&nbsp;&nbsp;commits[].file-size | int64 | Size of the commit file in bytes. Must be > 0. | required
&nbsp;&nbsp;commits[].file-modification-timestamp | int64 | Filesystem modification time in milliseconds since epoch. | required
uniform | DeltaUniformMetadata | UniForm conversion metadata. Present only for UniForm-enabled tables. | optional
&nbsp;&nbsp;uniform.iceberg.metadata-location | string | Iceberg metadata location converted up to `iceberg.converted-delta-version`. | required
&nbsp;&nbsp;uniform.iceberg.converted-delta-version | int64 | The Delta version under conversion. | required
&nbsp;&nbsp;uniform.iceberg.converted-delta-timestamp | int64 | Timestamp of the conversion, in epoch milliseconds. | required
&nbsp;&nbsp;uniform.iceberg.base-converted-delta-version | int64 | Optional Delta version used to incrementally convert Delta changes to Iceberg changes to produce the latest Iceberg metadata at `metadata-location`. | optional
latest-table-version | int64 | The latest version tracked by Unity Catalog for this table, including data-only commits. Clients use this to know when they have all commits and cannot load a table version beyond this. `0` is returned for a newly created managed table that has only the initial commit (version `0`) and no subsequent ratified commits. | required

**Note:** Vended cloud storage credentials are not included in this response. Clients obtain credentials with the appropriate operation level (`READ` or `READ_WRITE`) via the dedicated [Get Table Credentials](#get-table-credentials) endpoint.

**Synthetic property keys.** `metadata.properties` mixes the table's real `metaData.configuration` entries with keys the server derives from other state. The derived keys are listed below; clients must never copy them back into a `metaData.configuration` they write to the Delta log, and must not send them in `set-properties` / `remove-properties` actions:

- `delta.minReaderVersion`, `delta.minWriterVersion`, `delta.feature.<name>`: projected from the table's protocol. In the Delta log these live in the `protocol` action, and `delta.feature.<name>` keys inside `configuration` are creation-time directives, not persisted properties.
- `delta.clusteringColumns`, `delta.rowTracking.rowIdHighWaterMark`: projected from the `delta.clustering` / `delta.rowTracking` domain metadata. In the Delta log these live in `domainMetadata` actions.
- `delta.lastUpdateVersion`, `delta.lastCommitTimestamp`: catalog bookkeeping for the last metadata-changing commit; they have no Delta log representation.

`io.unitycatalog.tableId` is not synthetic: it is a real `metaData.configuration` entry that the client itself is required to write in the initial commit.

**Protocol and domain metadata on the read side.** This protocol version does not return the structured `protocol` or domain metadata in the `loadTable` response, even though both are structured on the write side. The protocol is conveyed only through the protocol-derived property keys above; a client that needs to reconstruct the protocol action classifies each `delta.feature.<name>` as reader-writer or writer-only by its name per the Delta protocol, or reads the `protocol` action from the Delta log, which remains authoritative for snapshot construction.

**Field requiredness.** The required markers in the table above describe the server's obligations for managed Delta tables. The shared `delta.yaml` response schema marks `commits`, `latest-table-version`, and the `last-commit-*` fields optional because the same response shape also serves external Delta tables, which have no catalog-managed commit log.

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
      "timestamp": 1704067500000,
      "file-name": "00000000000000000044.7f834a99-c4b5-41e7-9c25-26a4d0c14e7e.json",
      "file-size": 1792,
      "file-modification-timestamp": 1704067500000
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
PermissionDeniedException | 403 | User lacks necessary permissions (`SELECT` on the table; `USE CATALOG` / `USE SCHEMA` on the parents).<br>The client should check with their Unity Catalog Admin to grant them the necessary rights to query the table.
NoSuchSchemaException | 404 | The specified catalog or schema doesn't exist.<br>The client should check the spelling of the catalog and schema, or create them first.
NoSuchTableException | 404 | The specified table doesn't exist.<br>The client should check the spelling of the table or create the table first.

### Get Table Credentials

```
GET .../catalogs/{catalog}/schemas/{schema}/tables/{table}/credentials
```

Gets temporary credentials for accessing table data (vended credentials). The `operation` query parameter controls the access level: `READ` requires the `SELECT` privilege; `READ_WRITE` requires both `SELECT` and `MODIFY`. Table ownership satisfies either level. The server enforces authorization based on this parameter; admin privileges above the table are not by themselves sufficient.

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
storage-credentials | array of DeltaStorageCredential | Temporary cloud storage credentials. When several credentials are returned, the client selects the one whose `prefix` is the longest (most specific) match for the path being accessed. | required
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
PermissionDeniedException | 403 | User lacks necessary permissions (`SELECT` for `READ`; `SELECT` and `MODIFY` for `READ_WRITE`; ownership satisfies either).<br>Contact the admin for access.
NoSuchTableException | 404 | The specified table does not exist.

### Write a Commit

```
POST .../catalogs/{catalog}/schemas/{schema}/tables/{table}
```

Proposes a staged commit to the catalog and / or informs Unity Catalog that commits have been published. There is no standalone Commit API in this specification: the same endpoint serves both commit operations and other table metadata updates. For the managed-table commit flow, the client uses the `add-commit` action and, after publishing the ratified commit, the `set-latest-backfilled-version` action.

For UniForm / Iceberg-enabled tables, every `add-commit` action must carry the `uniform` field (nested under the action, see [DeltaUniformMetadata](#deltauniformmetadata-under-add-commituniform)) so the Iceberg conversion advances atomically with the commit. Writers cannot commit to a UniForm-enabled table without it: a writer with no Iceberg-conversion capability must not write to such tables. Clients can detect the requirement before attempting a commit: a UniForm-enabled table carries the `uniform` field in its [`loadTable`](#load-a-table) response and the enabling property (`delta.universalFormat.enabledFormats` containing `iceberg`) in `metadata.properties`.

The exact rule the server enforces is an invariant on the post-commit state: the `uniform` field must be present on an `add-commit` if and only if the table's properties after applying the request enable UniForm (`delta.universalFormat.enabledFormats` includes `iceberg`). Enabling or disabling UniForm on an existing managed table therefore happens atomically in the commit that flips the property: the enabling request combines a `set-properties` action with an `add-commit` carrying the table's first `uniform` block, and the disabling request combines the property removal with an `add-commit` that omits `uniform`.

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

Request shape rules, violations of which are rejected with `InvalidParameterValueException` (400):

- `updates` must contain at least one action.
- A request may carry at most one action of each type, and at most one requirement of each type.
- `set-properties` and `remove-properties` in the same request must not touch the same property key, and `set-domain-metadata` and `remove-domain-metadata` must not touch the same domain: the intent is contradictory and the request is rejected rather than resolved by ordering.
- The order of entries in `updates` and `requirements` carries no meaning; the server applies actions in a fixed canonical order.

##### Update actions

`add-commit` and `set-latest-backfilled-version` are the two actions specific to the catalog-managed commit lifecycle. The remaining actions in this table are used to express table-metadata changes that accompany a commit (schema evolution, property updates, protocol upgrades, clustering changes); they must be combined with `add-commit` in the same request when the resulting commit changes table metadata, so the catalog state and the Delta log stay in lockstep.

This specification covers only the actions used by managed tables. The same endpoint also accepts external-table-specific actions (e.g., `update-metadata-snapshot-version`); those are out of scope for this document.

Action | Description | Key Fields
-|-|-
`add-commit` | Add a new catalog-managed commit. Managed tables only. The commit `version` carries an implicit assertion that the existing version must be `v-1`. | `commit` (DeltaCommit, required): commit info. `uniform` (DeltaUniformMetadata, conditional): required only for UniForm-enabled tables.
`set-latest-backfilled-version` | Report the last backfilled / published commit version. Used after the client publishes a ratified commit to `_delta_log`. See [Publishing and backfill](#publishing-and-backfill) for the safety rules; a premature report can destroy catalog state. | `latest-published-version` (int64, required): the version that has been published.
`set-properties` | Set or update one or more table properties. | `updates` (object: string to string, required): key-value pairs to set.
`remove-properties` | Remove one or more table properties. | `removals` (array of string, required): property keys to remove.
`set-protocol` | Update the Delta protocol (e.g., to enable a new table feature). | `protocol` (DeltaProtocol, required): the new protocol. Same shape as `protocol` in [Create a Table](#create-a-table).
`set-columns` | Replace the table's column definitions. | `columns` (DeltaStructType, required): new column definitions. Same shape as `columns` in [Create a Table](#create-a-table).
`set-partition-columns` | Replace the partition column list. Only legal in a commit that also replaces the table's data files; see [REPLACE TABLE](#replace-table). | `partition-columns` (array of string, required): partition column names.
`set-table-comment` | Update the table comment. | `comment` (string, required): new comment text.
`set-domain-metadata` | Set domain metadata. Only domains present in `updates` are modified; absent domains are untouched. Used to set or update clustering columns (`delta.clustering`) and row-tracking high-water-mark (`delta.rowTracking`). | `updates` (object: string to object, required): a map of domain name to domain-specific metadata object. Each value's structure follows the corresponding [Delta protocol domain metadata](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#domain-metadata) definition.
`remove-domain-metadata` | Remove domain metadata for specific domains. | `domains` (array of string, required): domain names to remove.

##### Catalog and log consistency

Servers are not required to read staged commit files (at `createTable` time or on any update) and may accept requests without inspecting them; the client must keep the catalog's view and the Delta log in lockstep regardless. The same division applies to the content of metadata changes: the catalog validates the managed-table contract (required protocol versions, features, and properties) against the post-apply state, but Delta-protocol legality of the change itself is the client's responsibility and is not necessarily validated. This includes schema-evolution rules (column-mapping field-id immutability, `delta.columnMapping.maxColumnId` monotonicity, type-change compatibility) on `set-columns`, and the Delta feature-addition and feature-removal procedures on `set-protocol`: `set-protocol` is a full replacement, so a protocol that omits a previously active feature performs a feature drop, and the client must have carried out the corresponding removal procedure in the Delta log. Servers may reject changes that violate these rules; clients must not rely on it.

The lockstep rules:

- Any change that has a Delta log representation (schema via `set-columns`, protocol via `set-protocol`, `metaData.configuration` entries via `set-properties` / `remove-properties`, partition columns, and domain metadata) must be sent in the same request as the `add-commit` whose staged commit file carries the matching `metaData` / `protocol` / `domainMetadata` actions. The request applies atomically, so the two views move together.
- A metadata-changing request without an `add-commit` is permitted only for attributes with no Delta log representation, such as `set-table-comment` or properties that exist only in the catalog. Sending log-represented changes without an accompanying commit makes the catalog diverge from the log by construction and is a client error.
- If the two views diverge (client bug, crash between log write and catalog update), the Delta log is authoritative for snapshot semantics: engines reconstruct table state from published and ratified commit files, and the catalog's `metadata` projection serves discovery and UC features. The catalog may reject requests or repair its projection when divergence is detected, but this protocol version defines no automated reconciliation.

##### REPLACE TABLE

REPLACE is expressed as a single ordinary commit, not as a drop-and-recreate (see the creation flow in [APIs](#apis)): the table ID, location, and version history are preserved, and the version advances by one. The staged commit file carries the rewritten `metaData` / `protocol` actions plus `RemoveFile` actions for all existing data files, and the [`updateTable`](#write-a-commit) request mirrors the full rewrite by combining, in one atomic request:

- `add-commit` with the REPLACE commit's metadata;
- `set-columns` with the new schema;
- `set-protocol` with the full new protocol when the protocol changes (the managed-table contract is re-validated against the post-apply state, so all required features must be carried);
- `set-properties` for properties that change, and `remove-properties` enumerating every previously-set key that does not carry forward: `set-properties` merges and cannot clear, so omitting stale keys leaves them on the table. `io.unitycatalog.tableId` must always carry forward;
- `set-partition-columns` when the partitioning changes (an empty list for an unpartitioned result), and `set-domain-metadata` / `remove-domain-metadata` for clustering or row-tracking state that changes.

Facets that the REPLACE leaves unchanged need no action: lockstep mirrors the changes the staged commit makes, not the full table state.

Partition columns can only change as part of a commit that also replaces the table's data files, since existing data files are laid out by the old partitioning. A `set-partition-columns` outside a REPLACE-style commit makes the catalog diverge from the data and is a client error under [Catalog and log consistency](#catalog-and-log-consistency).

##### DeltaCommit (under `add-commit.commit`)

Field Name | Data Type | Description | Optional/Required
-|-|-|-
version | int64 | The table version to commit. Must be > 0. | required
timestamp | int64 | In-commit timestamp in milliseconds since epoch. | required
file-name | string | Basename of the staged commit file the client wrote under `_delta_log/_staged_commits/` (UUID-based, e.g., `"00000000000000000042.a1b2c3d4-5678-9abc-def0-123456789abc.json"`). The catalog returns this name verbatim to readers, which read the file at that path. | required
file-size | int64 | Size of the commit file in bytes. Must be > 0. | required
file-modification-timestamp | int64 | Filesystem modification time of the staged commit file in milliseconds since epoch, as observed by the client after writing it. | required

##### DeltaUniformMetadata (under `add-commit.uniform`)

Field Name | Data Type | Description | Optional/Required
-|-|-|-
iceberg.metadata-location | string | Iceberg metadata location converted up to the Delta version. Must be a subpath of the table's storage root. | required
iceberg.converted-delta-version | int64 | The Delta version under conversion. | required
iceberg.converted-delta-timestamp | int64 | Timestamp of the conversion, in epoch milliseconds. | required
iceberg.base-converted-delta-version | int64 | Optional Delta version used to incrementally convert Delta changes to Iceberg changes to produce the latest Iceberg metadata at `metadata-location`. Supplying this opts into sequential validation against the currently-stored `converted-delta-version`. | optional

##### Writer obligations for staged commits

The staged commit file proposed through `add-commit` must satisfy the writer requirements of the Delta [Catalog-Managed Tables](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#catalog-managed-tables) specification. In particular:

- `commitInfo` must contain a unique `txnId` (commit provenance). Servers are not required to validate this; omitting it produces a protocol-violating commit regardless.
- The in-commit timestamp sent as `commit.timestamp` must equal the `inCommitTimestamp` written in the file and must be strictly greater than the previous version's timestamp (clients typically use `max(wall clock, previous ICT + 1)`). The server validates version sequencing and may additionally reject non-monotonic timestamps; clients must ensure monotonicity regardless.

##### Publishing and backfill

Publishing (copying a ratified commit file to `_delta_log/<v>.json`) and reporting backfill are client duties with the following rules:

- Commits must be published in version order, per the Delta protocol.
- `set-latest-backfilled-version` is a client assertion: the server checks that the reported version does not exceed the last ratified version, and may verify that `_delta_log/<v>.json` actually exists. Once a version is reported as backfilled, the server may delete its record of the covered ratified commits at any time. Reporting a version whose publication did not complete can therefore permanently destroy the only record of those commits and corrupt the table. Clients must report a version only after the copy for that version (and all prior versions) has durably completed.
- Reporting an already-backfilled version is a no-op, so the action is safe to retry.
- Publishing is an idempotent file copy, and any client with write access may publish ratified commits proposed by other writers and report backfill for them. Cooperative publishing is the mechanism for clearing a backlog left by crashed writers: once the unbackfilled-commit limit is reached, the server rejects new commits with `ResourceExhaustedException` until someone publishes and reports.
- Staged commit files that were never ratified (for example, the losers of commit races) are not part of the table history. This protocol version defines no cleanup for them; they may accumulate under `_delta_log/_staged_commits/` until removed out of band.

##### Commit recovery

`add-commit` carries no idempotency token in this protocol version; the UUID embedded in `file-name` is the deduplication handle. When a commit outcome is unknown (`CommitStateUnknownException`, a network failure, or a process restart mid-commit), the client recovers as follows:

1. Reload the table via [`loadTable`](#load-a-table).
2. If `latest-table-version` >= `v` (the proposed version), look up version `v`: in the `commits` array, or in `_delta_log` if already published. If its `file-name` matches the UUID filename the client generated, the commit was accepted; resume from publishing. If it differs, a concurrent writer won version `v`; rebuild the snapshot and retry at the new latest version + 1.
3. If `latest-table-version` is `v-1`, the proposal was not accepted; re-send the same `add-commit`.

Re-sending an `add-commit` that was already accepted fails with `CommitVersionConflictException`; the client distinguishes "my commit won" from "someone else won" by the `file-name` comparison above, never by the error code alone.

In this protocol version, conflict responses carry no commit information: the reload in step 1 is how the client learns the outcome of a contended commit.

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

4. Propose a new staged commit that enables clustering and sets clustering columns

    The `delta.clustering` domain-metadata entry requires the `clustering` and `domainMetadata` writer features. The table in the [Load a Table](#load-a-table) example does not have them yet, so this request enables them via `set-protocol` in the same atomic request (a `set-protocol` must always carry the full target protocol, including all required features).

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
          "action": "set-protocol",
          "protocol": {
            "min-reader-version": 3,
            "min-writer-version": 7,
            "reader-features": ["catalogManaged", "deletionVectors", "v2Checkpoint", "vacuumProtocolCheck"],
            "writer-features": ["catalogManaged", "clustering", "deletionVectors", "domainMetadata", "inCommitTimestamp", "v2Checkpoint", "vacuumProtocolCheck"]
          }
        },
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

6. REPLACE the table with a new schema and no partitioning

    Per [REPLACE TABLE](#replace-table): the staged commit file for version 45 carries the rewritten `metaData` action and `RemoveFile` actions for all existing data files; the request mirrors the changed facets. The protocol and properties carry forward unchanged here, so no `set-protocol` or property actions are needed. `assert-etag` guards the read-modify-write of the metadata being replaced.

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
          "action": "set-columns",
          "columns": {
            "type": "struct",
            "fields": [
              {"name": "id", "type": "long", "nullable": false, "metadata": {}},
              {"name": "data", "type": "string", "nullable": true, "metadata": {}}
            ]
          }
        },
        {
          "action": "set-partition-columns",
          "partition-columns": []
        },
        {
          "action": "remove-domain-metadata",
          "domains": ["delta.clustering"]
        },
        {
          "action": "add-commit",
          "commit": {
            "version": 45,
            "timestamp": 1704067600000,
            "file-name": "00000000000000000045.3c1f2b88-1a4e-4f0e-9f1b-8a51d2f0c9aa.json",
            "file-size": 2304,
            "file-modification-timestamp": 1704067600000
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
PermissionDeniedException | 403 | User lacks necessary permissions (`MODIFY` on the table; `USE CATALOG` / `USE SCHEMA` on the parents).<br>Contact the admin for access.
NoSuchTableException | 404 | The specified table doesn't exist.<br>The client should check if the table ID is correct for this table.
CommitVersionConflictException | 409 | Commit rejected due to a concurrent conflicting commit. The response carries only this error body, with no information about the winning commit; the client reloads the table and follows [Commit recovery](#commit-recovery) to rebuild the snapshot and retry at the new version.
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
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;report.commit-report.file-size-histogram.file-counts | array of int64 | Count of files in each bin. Length must match `sorted-bin-boundaries`. | required
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

### Table Lifecycle Operations

Besides the managed-table protocol flow, the API exposes generic lifecycle operations on the same table path. They are listed here for completeness; their request and response shapes are defined in the [Delta APIs reference](../../api/delta-docs/README.md).

Operation | Endpoint | Behavior
-|-|-
Exists | `HEAD .../catalogs/{catalog}/schemas/{schema}/tables/{table}` | Returns `204` if the table exists, `404` otherwise. No response body. The check is format-agnostic: a `204` does not imply the table is loadable through this API (a non-Delta table also returns `204` here while `GET` on the same path returns `UnsupportedTableFormatException`).
Drop | `DELETE .../catalogs/{catalog}/schemas/{schema}/tables/{table}` | Removes the table from the catalog and returns `204`. For managed tables, Unity Catalog owns the storage location: the catalog is responsible for deleting the underlying data, and the location must not be reused by clients after the drop. This protocol version does not specify the cleanup timing (it may be asynchronous), does not support undrop, and does not specify whether already-vended credentials remain valid until their expiry.
Rename | `POST .../catalogs/{catalog}/schemas/{schema}/tables/{table}/rename` | Renames the table within the same catalog and schema; cross-schema and cross-catalog moves are not supported. Renaming to a name that already exists fails with `AlreadyExistsException` (409).

## Error Response Body

All non-2xx responses return an `error` object with the fields below.

The per-endpoint Errors tables list only endpoint-specific errors. In addition, every endpoint may return:

- `NotAuthorizedException` (401) when the bearer token is missing or invalid.
- `PermissionDeniedException` (403) when the caller lacks the required privileges; endpoint tables name the specific privileges where they matter.
- `TooManyRequestsException` (429) when the server throttles the client.
- `InternalServerErrorException` (500) and other 5xx errors on server failures.

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
**tableExists** | **HEAD** /catalogs/{catalog}/schemas/{schema}/tables/{table} | [Table Lifecycle Operations](#table-lifecycle-operations)
**deleteTable** | **DELETE** /catalogs/{catalog}/schemas/{schema}/tables/{table} | [Table Lifecycle Operations](#table-lifecycle-operations)
**renameTable** | **POST** /catalogs/{catalog}/schemas/{schema}/tables/{table}/rename | [Table Lifecycle Operations](#table-lifecycle-operations)

For full API documentation index, see [Delta APIs README](../../api/delta-docs/README.md).

---

[[Back to Main README]](../../README.md)

