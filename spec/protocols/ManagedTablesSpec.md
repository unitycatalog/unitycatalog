# Unity Catalog Managed Tables Specification

> **Status**: Proposed  
> **Version**: 0.1.0  
> **Last Updated**: 2026-02-17

## Table of Contents

- [Introduction](#introduction)
- [Terminologies](#terminologies)
- [APIs](#apis)
  - [Create a Staging Table](#create-a-staging-table)
  - [Create a Table](#create-a-table)
  - [Get a Table](#get-a-table)
  - [Get Commits](#get-commits)
  - [Write a Commit](#write-a-commit)
  - [Write Metrics](#write-metrics)
---

## Introduction
This specification defines how Unity Catalog will serve as the managing catalog for [Catalog-Managed Tables](https://github.com/delta-io/delta/blob/master/protocol_rfcs/catalog-managed.md).
Catalog-Managed Tables is a new Delta table feature which makes the catalog the source of truth for commits to a table.
It defines a managing catalog to perform commits according to the Catalog-Managed Tables specification.

This specification defines the APIs to enable the following operations between the server (Unity Catalog) and clients:
- Creating a Catalog-Managed table with necessary table features and properties.
- Retrieving and reading from the table.
- Writing to the table and publishing commits.
- Running maintenance operations.

### What is a Unity Catalog Managed Table?
A Unity Catalog Managed table is a table whose data location and lifecycle (including deletion of files when dropped)
are fully controlled by the Unity Catalog.
It is different from external tables, where the catalog only stores metadata pointing to data in an externally managed
location and does not delete the underlying files when the table is dropped.

Comparison with External Tables:

Feature | Managed Table | External Table
-|-|-
Storage ownership | Unity Catalog | User
Auto-cleanup on drop | Yes | No
Storage location | Assigned | User-specified


## Terminology
This section recaps key terminologies used in the catalog-managed tables specification:
- **Commit**: A set of actions that moves a Delta table from version `v−1` to `v`, stored either as a Delta log file or inline in the catalog.
- **Staged commit**: A commit written to `_delta_log/_staged_commits/<v>.<uuid>.json`, not yet guaranteed to be accepted; the catalog decides whether to ratify it.
- **Ratified commit**: A proposed commit that the catalog has chosen as the winner for version `v`; it is part of the official history, whether or not it is already published.
- **Published commit**: A ratified commit that has been written as `_delta_log/<v>.json`; the presence of this file proves that version `v` is published.

For further details, refer to the [Catalog-Managed Tables](https://github.com/delta-io/delta/blob/master/protocol_rfcs/catalog-managed.md).

## APIs
A Unity Catalog Managed table needs to be registered and used according to the following specification:
- **Create a table**: A Unity Catalog Managed table needs to first be created.
  - The client sends a `POST` request to the [`createStagingTable`](#create-a-staging-table) API to initiate the creation of a new Managed table in Unity Catalog.
  - The server upon receiving the request to create a staging table, allocates a unique table ID and a path for the table.
  - The client writes the first commit `0.json` under the `_delta_log` in the assigned table path. The `0.json` file should include all the table features and properties required by this specification. See the full list in [Required Table Features and Properties on a Unity Catalog Managed Table](#required-table-features-and-properties-on-a-unity-catalog-managed-table). Then the client finishes the table creation by sending a `POST` request to the [`createTable`](#create-a-table) API.
    - For CTAS (Create Table As Select), the `0.json` file would also contain `AddFile` actions.
    - For REPLACE, do not drop and recreate the table. Instead, add a new commit that updates the table metadata and removes all existing data files. This approach ensures atomicity, preserves the table ID and history. The replaced table remains to be a Unity Catalog Managed Table.
  - When a create table request is received, the server first confirms the existence of a table with the specified table ID at the provided location. If the table has the Catalog Managed feature enabled, the server then validates that all necessary table features and properties are correctly configured. Finally, the server registers the table as created at commit version 0.
- **Get the table**: After the table is created successfully, the client can use the three-part name (`catalog_name.schema_name.table_name`) to retrieve the table entry.
  - The client sends a `GET` call to the [`getTable`](#get-a-table) API by providing the fully qualified three-part name.
  - The server then responds with detailed information of the table entry, including the table ID, path etc.
- **Read from the table**: To read from the table, the client needs to obtain all necessary commits to reconstruct the snapshot:
  - The client obtains all the published commits by listing all the files in `_delta_log`, as well as to fetch the ratified commits from the server by performing a `GET` call to the [`getCommits`](#get-commits) API.
  - The server returns all ratified commits for the table. The server can optionally decide to paginate the response.
    - For a newly created table, the server should return `0` when no commits has been made to this table.
  - The client combines the published commits, ratified commits and rebuilds the current snapshot. When building the snapshot, the client should not read any commits that go beyond max ratified version returned by the server.
- **Write to the table**:
  - The client should first write a staged commit file and then propose to the server to accept this commit by sending a `POST` to the [`commit`](#write-a-commit) API. If the commit contains table protocol or metadata changes, it must insert them as part of the `commit` request. This POST request could also contain an optional field `uniform/iceberg` for UniForm/Managed Iceberg tables.
  - The server verifies the version to commit equals to the last ratified commit version + 1 and accepts the commit. If the request contains the field `uniform/iceberg`, the field will be persisted as part of the table’s latest state in the catalog.
  - After the commit is successful, the client must publish the ratified commit by copying the committed file to the `_delta_log` folder. After all the commits till this version has been published, the client should inform the server the commit has been published by sending another backfill commit, as a `POST` request to the `commit` API.
  - The server verifies that the version is equal or less than the last ratified version and optionally removes the entries for the ratified commits.
- **Metrics and running maintenance operations**: The client should send [metrics](#write-metrics) after each successful commit. These metrics may be used for Unity Catalog to schedule and manage maintenance operations. Unity Catalog enforces a maintenance policy for catalog‑managed tables. By default, clients are only allowed to run checkpoints, log compaction, and version checksum. All other maintenance operations, such as metadata cleanup, OPTIMIZE, VACUUM, and REORG, are disallowed.

The rest of the specification outlines the APIs and detailed requirements. All the endpoints are prefixed with `/api/2.1/unity-catalog`.

### Create a Staging Table

```
POST .../staging-tables
```

Proposes a staged table for Unity Catalog to allocate a table ID and path for the table.
Once created, the table ID and path are not mutable.
A staged table is not yet a real table, as a result, it cannot be used with table APIs such as the `GET tables` API.

#### Request body

Field Name | Data Type | Description | Optional/Required
-|-|-|-
name | string | Name of the staging table, relative to parent schema. | required
catalog_name | string | Name of the parent catalog where the staging table will be created. | required
schema_name | string | Name of the parent schema relative to its parent catalog. | required

#### Responses

**200: Request completed successfully.** It echoes all the fields in the request body and adds the additional following fields:

Field Name | Data Type | Description | Optional/Required
-|-|-|-
id | string | Unique identifier generated for the staging table. This can be used to reference the staging table in subsequent operations.<br/>Type: UUID string. | required
staging_location | string | URI generated for the staging table. This is the actual cloud storage location where data will be staged.<br/>Format: URI string (`s3://`, `abfss://`, `gs://`, etc.). | required

#### Request-Response Samples
Request:
```json
{
  "name": "my_table",
  "catalog_name": "main",
  "schema_name": "default"
}
```
Response: 200
```json
{
  "name": "my_table",
  "catalog_name": "main",
  "schema_name": "default",
  "id": "abcdef12-3456-7890-abcd-ef1234567890",
  "staging_location": "s3://my_bucket/main/default/my_table/"
}
```
#### Errors
Possible Errors on this API are listed below:

Error Code | HTTP Status | Description 
-|-|-
CATALOG_DOES_NOT_EXIST | 404 | The specified catalog doesn't exist.<br>The client should check the spelling of the catalog name or create the catalog first. 
SCHEMA_DOES_NOT_EXIST | 404 | The specified schema doesn't exist. <br>The client should check the spelling of the schema or create the schema first.
TABLE_ALREADY_EXISTS | 400 | A table with the same name already exists.<br>The client should provide a different table name.
PERMISSION_DENIED | 403 | User lacks necessary permissions (CREATE on schema, USAGE on catalog/schema).<br>The client should check with their Unity Catalog Admin to grant them the necessary rights to create a staged table.

### Create a Table

```
POST .../tables
```

Creates a new Unity Catalog Managed table in the specified catalog and schema. When creating the table, the `0.json` should contain all the following table features and properties.

#### Required Table Features and Properties on a Unity Catalog Managed Table

*The list of features in this table are subject to change.*

Feature Name | Table Feature | Table Properties | Description
-|-|-|-
[Catalog-Managed Tables](https://github.com/delta-io/delta/blob/master/protocol_rfcs/catalog-managed.md) | catalogManaged | `io.unitycatalog.tableId = <tableID>` | Required.
[In-Commit Timestamps](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#in-commit-timestamps) | inCommitTimestamp | `delta.enableInCommitTimestamps = true` | Required for the correctness of time travel.
[VACUUM Protocol Check](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#vacuum-protocol-check) | vacuumProtocolCheck | | Required for proper vacuum operations.
[V2 Checkpoint](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#v2-checkpoint-table-feature) | v2Checkpoint | `delta.checkpointPolicy = v2` | Recommended
[Deletion Vectors](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#deletion-vectors) | deletionVectors | `delta.enableDeletionVectors = true` | Recommended
[Row Tracking](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#row-tracking) | rowTracking | `delta.enableRowTracking = true` | Recommended

Other Required Table Properties:

Property Name | Description
-|-
`delta.feature.xyz = ‘supported’` | For all supported table features `xyz`.
`delta.minReaderVersion = x` | `x` the min reader version of the table, `x >= 3`.
`delta.minWriterVersion = y` | `y` is the min writer version of the table, `y >=7`.
`delta.lastUpdateVersion = 0` | Indicates the last version at which the metadata was updated.
`delta.lastCommitTimestamp = t` | `t` is the timestamp of commit 0, in milliseconds since epoch.
`clusteringColumns` | If the [Clustered Table Feature](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#clustered-table) is supported. Column paths are serialized as a JSON array of string arrays. Each string array represents a path from the root schema to a column, with each element being a field name. For example, top-level column `id` and nested column `address.city` serialize to `[["id"], ["address", "city"]]`.

#### Request body

Field Name | Data Type | Description | Optional/Required
-|-|-|-
name | string | Name of the staging table, relative to parent schema. | required
catalog_name | string | Name of the parent catalog where the staging table will be created. | required
schema_name | string | Name of the parent schema relative to its parent catalog. | required
table_type | string | The type of the table. For Unity Catalog Managed Tables. Must be set to “MANAGED”. | required
data_source_format | string | Specify the format of the table.<br>For Unity Catalog Managed Tables, must be set to “DELTA” for delta tables. | required
columns | array of ColumnInfos | The array of ColumnInfo definitions of the table's columns. | optional
&nbsp;&nbsp;columnInfo.name | string | Name of Column. | optional
&nbsp;&nbsp;columnInfo.type_text | string | Full data type specification as SQL/catalogString text. | optional
&nbsp;&nbsp;columnInfo.type_json | string | Full data type specification, JSON-serialized. | optional
&nbsp;&nbsp;columnInfo.type_name | string | The type of the column. One of BOOLEAN, BYTE, SHORT, INT, LONG, FLOAT, DOUBLE, DATE, TIMESTAMP, TIMESTAMP_NTZ, CHAR, STRING, BINARY, INTERVAL, ARRAY, STRUCT, MAP, DECIMAL, etc. | optional
&nbsp;&nbsp;columnInfo.type_precision | int32 | Digits of precision; required for DecimalTypes | optional
&nbsp;&nbsp;columnInfo.type_scale | int32 | Digits to the right of decimal; Required for DecimalTypes. | optional
&nbsp;&nbsp;columnInfo.type_internal_type | string | Format of IntervalType. | optional
&nbsp;&nbsp;columnInfo.position | int32 | Ordinal position of column (starting at position 0). | optional
&nbsp;&nbsp;columnInfo.nullable | boolean | Whether the field may be Null. Default to true. | optional
&nbsp;&nbsp;columnInfo.partition_index | int32 | Partition index for column. | optional
storage_location | string | For Unity Catalog Managed Tables, this must be the staging_location of staging table. | required
properties | object | Table properties as key-value pairs. | optional

#### Responses
**200: Request completed successfully.** It echoes all the fields in the request body and adds the additional following fields:

Field Name | Data Type | Description | Optional/Required
-|-|-|-
owner | string | Username of current owner of table. | required
created_at | int64 | Time at which this table was created, in epoch milliseconds. | required
created_by | string | Username of table creator. | required
updated_at | int64 | Time at which this table was last modified, in epoch milliseconds. | required
updated_by | string | Username of user who last modified the table. | optional
table_id | string | Unique identifier for the table.<br>Type: UUID string. | required

#### Request-Response Samples
Request:
```json
{
  "name": "my_table",
  "catalog_name": "main",
  "schema_name": "default",
  "table_type": "MANAGED",
  "data_source_format": "DELTA",
  "storage_location": "s3://my-bucket/main/default/my_table/",
  "columns": [
    {
      "name": "id",
      "type_text": "bigint",
      "type_json": "{\"name\":\"id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}}",
      "type_name": "LONG",
      "position": 0,
      "nullable": true
    },
    {
      "name": "name",
      "type_text": "string",
      "type_json": "{\"name\":\"name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}",
      "type_name": "STRING",
      "position": 1,
      "nullable": true
    },
    {
      "name": "created_date",
      "type_text": "date",
      "type_json": "{\"name\":\"created_date\",\"type\":\"date\",\"nullable\":true,\"metadata\":{}}",
      "type_name": "DATE",
      "position": 2,
      "partition_index": 0,
      "nullable": true
    }
  ],
  "properties": {
    "delta.minReaderVersion": "3",
    "delta.minWriterVersion": "7",
    "delta.enableInCommitTimestamps": "true",
    "delta.feature.inCommitTimestamp": "supported",
    "delta.feature.vacuumProtocolCheck": "supported",
    "delta.feature.catalogManaged": "supported",
    "io.unitycatalog.tableId": "abcdef12-3456-7890-abcd-ef1234567890",
    "delta.lastUpdateVersion": "0",
    "delta.lastCommitTimestamp": "1704067400000"
  }
}
```

Response: 200
```json
{
  "name": "my_table",
  "catalog_name": "main",
  "schema_name": "default",
  "table_type": "MANAGED",
  "data_source_format": "DELTA",
  "storage_location": "s3://my-bucket/main/default/my_table/",
  "columns": [
    {
      "name": "id",
      "type_text": "bigint",
      "type_json": "{\"name\":\"id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}}",
      "type_name": "LONG",
      "position": 0,
      "nullable": true
    },
    {
      "name": "name",
      "type_text": "string",
      "type_json": "{\"name\":\"name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}",
      "type_name": "STRING",
      "position": 1,
      "nullable": true
    },
    {
      "name": "created_date",
      "type_text": "date",
      "type_json": "{\"name\":\"created_date\",\"type\":\"date\",\"nullable\":true,\"metadata\":{}}",
      "type_name": "DATE",
      "position": 2,
      "partition_index": 0,
      "nullable": true
    }
  ],
  "properties": {
    "delta.minReaderVersion": "3",
    "delta.minWriterVersion": "7",
    "delta.enableInCommitTimestamps": "true",
    "delta.feature.inCommitTimestamp": "supported",
    "delta.feature.vacuumProtocolCheck": "supported",
    "delta.feature.catalogManaged": "supported",
    "io.unitycatalog.tableId": "abcdef12-3456-7890-abcd-ef1234567890",
    "delta.lastUpdateVersion": "0",
    "delta.lastCommitTimestamp": "1704067400000"
  },
  "owner": "user@databricks.com",
  "created_at": 1705600000000,
  "created_by": "user@databricks.com",
  "updated_at": 1705600000000,
  "updated_by": "user@databricks.com",
  "table_id": "abcdef12-3456-7890-abcd-ef1234567890"
}
```
#### Errors

Possible Errors on this API are listed below:

Error Code | HTTP Status | Description
-|-|-
CATALOG_DOES_NOT_EXIST | 404 | The specified catalog doesn't exist.<br>The client should check the spelling of the catalog name or create the catalog first.
SCHEMA_DOES_NOT_EXIST | 404 | The specified schema doesn't exist.<br>The client should check the spelling of the schema or create the schema first.
TABLE_ALREADY_EXISTS | 400 | A table with the same name already exists.<br>The client should provide a different table name.
TABLE_DOES_NOT_EXIST | 404 | The specified staging table doesn't exist.<br>The client should check the spelling of the table or create the staged table first.
PERMISSION_DENIED | 403 | User lacks necessary permissions (CREATE on schema, USAGE on catalog/schema).<br>The client should check with their Unity Catalog Admin to grant them the necessary rights to create a table.
INVALID_PARAMETER_VALUE | 400 | Server side check failed for the Unity Catalog Managed Delta Table, which could include but not limited to (1) not a Delta Table, (2) missing required table features or properties.

### Get a Table

```
GET .../tables/{full_name}
```
Gets a table using the full_name of the table, in the form of `catalog_name.schema_name.table_name`.

#### Request body
No extra parameters.

#### Responses

**200: Request completed successfully.**

Field Name | Data Type | Description | Optional/Required
-|-|-|-
name | string | Name of the staging table, relative to parent schema. | required
catalog_name | string | Name of the parent catalog where the staging table will be created. | required
schema_name | string | Name of the parent schema relative to its parent catalog. | required
table_type | string | The type of the table.<br>For Unity Catalog Managed Tables, set the value to be “MANAGED”. | required
data_source_format | string | Specify the format of the table.<br>For Unity Catalog Managed Tables, set the value to be “DELTA” for delta tables. | required
columns | array of ColumnInfos | The array of ColumnInfo definitions of the table's columns. | optional
&nbsp;&nbsp;columnInfo.name | string | Name of the column. | optional
&nbsp;&nbsp;columnInfo.type_text | string | Full data type specification as SQL/catalogString text. | optional
&nbsp;&nbsp;columnInfo.type_json | string | Full data type specification, JSON-serialized. | optional
&nbsp;&nbsp;columnInfo.type_name | string | The type of the column. One of BOOLEAN, BYTE, SHORT, INT, LONG, FLOAT, DOUBLE, DATE, TIMESTAMP, TIMESTAMP_NTZ, CHAR, STRING, BINARY, INTERVAL, ARRAY, STRUCT, MAP, DECIMAL, etc. | optional
&nbsp;&nbsp;columnInfo.type_precision | int32 | Digits of precision; required for DecimalTypes | optional
&nbsp;&nbsp;columnInfo.type_scale | int32 | Digits to the right of decimal; Required for DecimalTypes. | optional
&nbsp;&nbsp;columnInfo.type_internal_type | string | Format of IntervalType. | optional
&nbsp;&nbsp;columnInfo.position | int32 | Ordinal position of column (starting at position 0). | optional
&nbsp;&nbsp;columnInfo.nullable | boolean | Whether the field may be Null. Default to true. | optional
&nbsp;&nbsp;columnInfo.partition_index | int32 | Partition index for column. | optional
storage_location | string | Storage root URL for table | required
properties | object | Table properties as key-value pairs. | optional
owner | string | Username of current owner of table. | required
created_at | int64 | Time at which this table was created, in epoch milliseconds. | required
created_by | string | Username of table creator. | required
updated_at | int64 | Time at which this table was last modified, in epoch milliseconds. | required
updated_by | string | Username of user who last modified the table. | optional
table_id | string | Unique identifier for the table.<br>Type: UUID string. | required

Request-Response Samples

Request: `GET .../tables/main.default.my_table`

Response: 200

```json
{
  "name": "my_table",
  "catalog_name": "main",
  "schema_name": "default",
  "table_type": "MANAGED",
  "data_source_format": "DELTA",
  "storage_location": "s3://my-bucket/main/default/my_table/",
  "columns": [
    {
      "name": "id",
      "type_text": "bigint",
      "type_json": "{\"name\":\"id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}}",
      "type_name": "LONG",
      "position": 0,
      "nullable": true
    },
    {
      "name": "name",
      "type_text": "string",
      "type_json": "{\"name\":\"name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}",
      "type_name": "STRING",
      "position": 1,
      "nullable": true
    },
    {
      "name": "created_date",
      "type_text": "date",
      "type_json": "{\"name\":\"created_date\",\"type\":\"date\",\"nullable\":true,\"metadata\":{}}",
      "type_name": "DATE",
      "position": 2,
      "partition_index": 0,
      "nullable": true
    }
  ],
  "properties": {
    "delta.minReaderVersion": "3",
    "delta.minWriterVersion": "7",
    "delta.enableInCommitTimestamps": "true",
    "delta.feature.inCommitTimestamp": "supported",
    "delta.feature.vacuumProtocolCheck": "supported",
    "delta.feature.catalogManaged": "supported",
    "io.unitycatalog.tableId": "abcdef12-3456-7890-abcd-ef1234567890",
    "delta.lastUpdateVersion": "0",
    "delta.lastCommitTimestamp": "1704067400000"
  },
  "owner": "user@databricks.com",
  "created_at": 1705600000000,
  "created_by": "user@databricks.com",
  "updated_at": 1705600000000,
  "updated_by": "user@databricks.com",
  "table_id": "abcdef12-3456-7890-abcd-ef1234567890"
}
```

#### Errors

Possible Errors on this API are listed below:

Error Code | HTTP Status | Description
-|-|-
CATALOG_DOES_NOT_EXIST | 404 | The specified catalog doesn't exist.<br>The client should check the spelling of the catalog name or create the catalog first.
SCHEMA_DOES_NOT_EXIST | 404 | The specified schema doesn't exist.<br>The client should check the spelling of the schema or create the schema first.
TABLE_DOES_NOT_EXIST | 404 | The specified table doesn't exist.<br>The client should check the spelling of the table or create the table first.
PERMISSION_DENIED | 403 | User lacks necessary permissions (SELECT on the table, USAGE on catalog/schema). The client should check with their Unity Catalog Admin to grant them the necessary rights to query the table.

### Get Commits

```
GET .../delta/commits
```
Retrieve all ratified commits from Unity Catalog.
Note: Server implementations may choose to paginate responses. A paginated response is indicated by the highest returned version in the commits array being less than `latest_table_version`. In this case, the client should send another GET request with an adjusted `start_version` to retrieve any remaining commits.

#### Request body

Field Name | Data Type | Description | Optional/Required
-|-|-|-
table_id | string | Uniquely identifies the Delta table in Unity Catalog.<br>Type: UUID string. | required
table_uri | string | The storage location of the Delta table.<br>Format: URI string (`s3://`, `abfss://`, `gs://`, etc.). | required
start_version | int64 | The start version from which to retrieve commits (inclusive). Must be >= 0. Default: 0. | optional
end_version | int64 | The end version to retrieve commits up to (inclusive). Must be >= 0. Default: latest version. | optional

#### Responses

**200: Request completed successfully.**

Field Name | Data Type | Description | Optional/Required
-|-|-|-
commits | array of CommitInfo | List of ratified commit files tracked in Unity Catalog. Guaranteed to be contiguous and within the specified [`start_version`, `end_version`] range.<br/>Type: Array of CommitInfo objects.<br/>Note: Can be in arbitrary order. Empty array is valid if no commits in the requested range. | required
&nbsp;&nbsp;commitInfo.version | int64 | The commit's table version. | required
&nbsp;&nbsp;commitInfo.timestamp | int64 | Commit timestamp in milliseconds since epoch. | required
&nbsp;&nbsp;commitInfo.file_name | string | Commit filename. | required
&nbsp;&nbsp;commitInfo.file_size | int64 | Size of the commit file in bytes. | required
&nbsp;&nbsp;commitInfo.file_modification_timestamp | int64 | Filesystem modification time in milliseconds since epoch. | required
latest_table_version | int64 | The latest version tracked by UC for this table. This is the absolute latest version, regardless of `end_version` parameter. Clients can use this to know if they have all commits and cannot load a table version beyond this. 0 is returned for a newly created managed table before any commit is posted. -1 is returned if Unity Catalog does not manage this table. | required

#### Request-Response Samples

1. Base Get Commits

    Request:
    ```json
    {
      "table_id": "abcdef12-3456-7890-abcd-ef1234567890",
      "table_uri": "s3://my-bucket/main/default/my_table",
      "start_version": 42
    }
    ```
    Response: 200
    ```json
    {
      "commits": [
        {
          "version": 42,
          "timestamp": 1704067200000,
          "file_name": "00000000-0000-0000-0000-00000000002a.json",
          "file_size": 2048,
          "file_modification_timestamp": 1704067200000
        },
        {
          "version": 43,
          "timestamp": 1704067300000,
          "file_name": "00000000-0000-0000-0000-00000000002b.json",
          "file_size": 3072,
          "file_modification_timestamp": 1704067300000
        },
        {
          "version": 44,
          "timestamp": 1704067400000,
          "file_name": "00000000-0000-0000-0000-00000000002c.json",
          "file_size": 1536,
          "file_modification_timestamp": 1704067400000
        }
      ],
      "latest_table_version": 44
    }
    ```

2. Get Commits with Version Range 

    Request:
    ```json
    {
      "table_id": "abcdef12-3456-7890-abcd-ef1234567890",
      "table_uri": "s3://my-bucket/main/default/my_table",
      "start_version": 42,
      "end_version": 43
    }
    ```
    Response: 200
    ```json
    {
      "commits": [
        {
          "version": 42,
          "timestamp": 1704067200000,
          "file_name": "00000000-0000-0000-0000-00000000002a.json",
          "file_size": 2048,
          "file_modification_timestamp": 1704067200000
        },
        {
          "version": 43,
          "timestamp": 1704067300000,
          "file_name": "00000000-0000-0000-0000-00000000002b.json",
          "file_size": 3072,
          "file_modification_timestamp": 1704067300000
        }
      ],
      "latest_table_version": 50
    }
    ```

#### Errors

Possible Errors on this API are listed below:

Error Code | HTTP Status | Description
-|-|-
TABLE_DOES_NOT_EXIST | 404 | The specified table doesn't exist.<br>The client should check the spelling of the table or create the table first.
PERMISSION_DENIED | 403 | User lacks necessary permissions (SELECT on table).<br>The client should check with their Unity Catalog Admin to grant them the necessary rights to query commits.
INVALID_PARAMETER_VALUE | 400 | Server side check failed to get commits for Unity Catalog Managed Tables.<br>The client has provided wrong inputs to the server. This could include but not limited to the following errors: (1) missing required fields, (2) provided fields are invalid such as the start version is negative, the start version is greater than the end version, etc.



### Write a Commit

```
POST .../delta/commit
```

Proposes a staged commit to the catalog and informs Unity Catalog that a commit has been published.

#### Request body

Field Name | Data Type | Description | Optional/Required
-|-|-|-
table_id | string | Uniquely identifies the Delta table in Unity Catalog. Format: UUID string. | required
table_uri | string | The storage location of the Delta table. Format: URI string (`s3://`, `abfss://`, `gs://`, etc.). | required
commit_info | CommitInfo | Contains information about the new commit being registered. Constraint: At least one of `commit_info` or `latest_published_version` must be present. | optional
&nbsp;&nbsp;commitInfo.version | int64 | The table version to commit. | required
&nbsp;&nbsp;commitInfo.timestamp | int64 | In-commit timestamp in milliseconds since epoch. | required
&nbsp;&nbsp;commitInfo.file_name | string | UUID-based commit filename. | required
&nbsp;&nbsp;commitInfo.file_size | int64 | Size of the commit file in bytes. | required
&nbsp;&nbsp;commitInfo.file_modification_timestamp | int64 | Filesystem modification time in milliseconds since epoch. | required
latest_published_version | int64 | Notifies UC that commits up to this version have been published. Constraint: At least one of `commit_info` or `latest_published_version` must be present. | optional
metadata | Metadata | Delta metadata action if the commit changes table metadata. The posted metadata object is the desired final state, so all existing data that needs to be preserved must be included as well as the fields that changed. Any properties returned from Unity Catalog should be included. When to include: Schema changes, property updates, comment changes. Requires `commit_info`. | optional
&nbsp;&nbsp;metadata.id | string | The internal table ID created by Delta. | required
&nbsp;&nbsp;metadata.name | string | The table name. | optional
&nbsp;&nbsp;metadata.description | string | The table comment. | optional
&nbsp;&nbsp;metadata.provider | string | The format of the files, e.g. "parquet". | optional
&nbsp;&nbsp;metadata.options | object | Format-specific options as key-value pairs. | optional
&nbsp;&nbsp;metadata.schema | array of ColumnInfos | The table schema defined by an array of `ColumnInfo` objects. | optional
&nbsp;&nbsp;metadata.partition_columns | array of strings | The partition columns. | optional
&nbsp;&nbsp;metadata.properties | object | Table properties as key-value pairs. When updating, always includes all the required [properties](#required-table-features-and-properties-on-a-unity-catalog-managed-table). | optional
&nbsp;&nbsp;metadata.created_time | int64 | Timestamp when metadata was created in milliseconds since epoch | optional
uniform | Uniform | UniForm conversion information related to this Delta commit. | optional
&nbsp;&nbsp;uniform.iceberg | Iceberg | Uniform Iceberg info. | optional
&nbsp;&nbsp;&nbsp;&nbsp;uniform.iceberg.metadata_location | string | Iceberg metadata location converted up to the Delta version. | required
&nbsp;&nbsp;&nbsp;&nbsp;uniform.iceberg.converted_delta_version | int64 | The Delta version under conversion. | required
&nbsp;&nbsp;&nbsp;&nbsp;uniform.iceberg.converted_delta_timestamp | string | Timestamp of the conversion in ISO 8601 timestamp which is 27 characters. | required
&nbsp;&nbsp;&nbsp;&nbsp;uniform.iceberg.base_converted_delta_version | int64 | Optional Delta version used to incrementally convert Delta changes to Iceberg changes to produce the latest Iceberg metadata at the metadata_location. | optional

#### Responses

**200: Request completed successfully.**

#### Request-Response Samples

1. Propose a simple staged commit

    Request:
    ```json
    {
      "table_id": "abcdef12-3456-7890-abcd-ef1234567890",
      "table_uri": "s3://my-bucket/main/default/my_table",
      "commit_info": {
        "version": 42,
        "timestamp": 1704067200000,
        "file_name": "v.uuid1.json",
        "file_size": 2048,
        "file_modification_timestamp": 1704067200000
      }
    }
    ```
    
    Response: 200
    ```json
    {}
    ```

2. Propose a new staged commit with a Metadata change

    Request:
    ```json
    {
      "table_id": "abcdef12-3456-7890-abcd-ef1234567890",
      "table_uri": "s3://my-bucket/main/default/my_table",
      "commit_info": {
        "version": 43,
        "timestamp": 1704067300000,
        "file_name": "v.uuid1.json",
        "file_size": 3072,
        "file_modification_timestamp": 1704067300000
      },
      "metadata": {
        "id": "12345678-1234-5678-1234-567812345678",
        "name": "my_table",
        "description": "Updated table description",
        "format": {
          "provider": "parquet",
          "options": {}
        },
        "schema": [
          {"name": "id", "type": "long", "nullable": false},
          {"name": "name", "type": "string", "nullable": true},
          {"name": "created_at", "type": "timestamp", "nullable": false}
        ],
       "properties": {
         "delta.minReaderVersion": "3",
         "delta.minWriterVersion": "7",
         "delta.enableInCommitTimestamps": "true",
         "delta.feature.inCommitTimestamp": "supported",
         "delta.feature.vacuumProtocolCheck": "supported",
         "delta.feature.catalogManaged": "supported",
         "io.unitycatalog.tableId": "abcdef12-3456-7890-abcd-ef1234567890",
         "delta.lastUpdateVersion": "0",
         "delta.lastCommitTimestamp": "1704067400000"
       },
       "created_time": 1804067300000
     }
    }
    ```
    Response: 200
    ```json
    {}
    ```

3. Update the latest published version

    Request:
    ```json
    {
      "table_id": "abcdef12-3456-7890-abcd-ef1234567890",
      "table_uri": "s3://my-bucket/main/default/my_table",
      "latest_published_version": 40
    }
    ```
    
    Response: 200
    ```json
    {}
    ```

4. Propose a new staged commit with Uniform Iceberg changes

    Request:
    ```json
    {
      "table_id": "abcdef12-3456-7890-abcd-ef1234567890",
      "table_uri": "s3://my-bucket/main/default/my_table",
      "commit_info": {
        "version": 42,
        "timestamp": 1704067200000,
        "file_name": "v.uuid1.json",
        "file_size": 2048,
        "file_modification_timestamp": 1704067200000
      },
      "uniform": {
        "iceberg": {
          "metadata_location": "s3://my-bucket/warehouse/main.db/my_table/metadata/00042-xxxx.metadata.json",
          "converted_delta_version": 42,
          "converted_delta_timestamp": "2026-02-09T17:00:00.000000Z"
        }
      }
    }
    ```
    
    Response: 200
    ```json
    {}
    ```
#### Errors

Possible Errors on this API are listed below:

Error Code | HTTP Status | Description
-|-|-
TABLE_DOES_NOT_EXIST | 404 | The specified table doesn't exist.<br>The client should check if the table ID is correct for this table.
PERMISSION_DENIED | 403 | Insufficient permissions (MODIFY on table).<br>The client should check with their Unity Catalog Admin to grant them the necessary rights to write commits.
INVALID_PARAMETER_VALUE | 400 | Server side check failed to write commits to Unity Catalog Managed Tables.<br>The client has provided wrong inputs to the server. This could include but not limited to the following errors: (1) missing required fields, (2) provided fields are invalid, such as version is not positive, timestamp is not positive, file name is missing, file size is not positive, file modification time is not positive, (3) commit version is not exactly `last_version + 1`, (4) last published version > current table version, etc.
ALREADY_EXISTS | 409 | Commit version already exists (concurrent write).<br>The client should follow the Delta protocol to rebuild the snapshot and retry the commit with a new version.



### Write Metrics

```
POST .../delta/metrics
```

Publish metrics information for maintenance operations provided by the Unity Catalog.

#### Request body

Field Name | Data Type | Description | Optional/Required
-|-|-|-
table_id | string | Uniquely identifies the Delta table in Unity Catalog.<br>Format: UUID string. | required
table_uri | string | The storage location of the Delta table.<br>Format: URI string (`s3://`, `abfss://`, `gs://`, etc.). | required
report | Report | Contains information about which metrics are being sent. | optional
&nbsp;&nbsp;report.commit_report | CommitReport | The commit report metrics. | optional
&nbsp;&nbsp;&nbsp;&nbsp;report.commit_report.num_files_added | int64 | Number of files added by this commit. | optional
&nbsp;&nbsp;&nbsp;&nbsp;report.commit_report.num_bytes_added | int64 | Number of bytes added by this commit. | optional
&nbsp;&nbsp;&nbsp;&nbsp;report.commit_report.num_clustered_bytes_added | int64 | Number of clustered bytes added by this commit. | optional
&nbsp;&nbsp;&nbsp;&nbsp;report.commit_report.num_files_removed | int64 | Number of files removed by this commit. | optional
&nbsp;&nbsp;&nbsp;&nbsp;report.commit_report.num_bytes_removed | int64 | Number of bytes removed by this commit. | optional
&nbsp;&nbsp;&nbsp;&nbsp;report.commit_report.num_clustered_bytes_removed | int64 | Number of clustered bytes removed by this commit. | optional
&nbsp;&nbsp;&nbsp;&nbsp;report.commit_report.file_size_histogram | Histogram | Object that represents a histogram tracking file counts and total bytes across different size ranges. | optional
&nbsp;&nbsp;&nbsp;&nbsp;report.commit_report.file_size_histogram.sorted_bin_boundaries | array of int64 | A sorted array of bin boundaries where each element represents the start of a bin (inclusive) and the next element represents the end of the bin (exclusive). The first element must be 0. | optional
&nbsp;&nbsp;&nbsp;&nbsp;report.commit_report.file_size_histogram.file_counts | array of int64 | Count of files in each bin. Length must match `sortedBinBoundaries`. | optional
&nbsp;&nbsp;&nbsp;&nbsp;report.commit_report.file_size_histogram.total_bytes | array of int64 | Total bytes of files in each bin. Length must match `sorted\_bin\_boundaries`. | optional
&nbsp;&nbsp;&nbsp;&nbsp;report.commit_report.num_rows_inserted | int64 | Number of rows added by this commit. | optional
&nbsp;&nbsp;&nbsp;&nbsp;report.commit_report.num_rows_removed | int64 | Number of rows removed by this commit. | optional
&nbsp;&nbsp;&nbsp;&nbsp;report.commit_report.num_rows_updated | int64 | Number of rows updated by this commit. | optional
&nbsp;&nbsp;&nbsp;&nbsp;report.commit_report.commit_version | int64 | The commit version, needed for a sanity check. | required

#### Responses

**200: Request completed successfully.**

#### Request-Response Samples

Request:

```json
{
  "table_id": "abcdef12-3456-7890-abcd-ef1234567890",
  "table_uri": "s3://my-bucket/main/default/my_table",
  "report": {
    "commit_report": {
      "num_files_added": 10,
      "num_bytes_added": 104857600,
      "num_clustered_bytes_added": 52428800,
      "num_files_removed": 2,
      "num_bytes_removed": 8192,
      "num_clustered_bytes_removed": 4096,
      "file_size_histogram": {
        "sorted_bin_boundaries": [0, 1024, 2048],
        "file_counts": [100, 40, 5],
        "total_bytes": [104857600, 167772160, 83886080]
      },
      "num_rows_inserted": 1000000,
      "num_rows_removed": 50000,
      "num_rows_updated": 25000,
      "commit_version": 6
    }
  }
}
```
Response: 200
```json
{}
```

#### Errors

Possible Errors on this API are listed below:

Error Code | HTTP Status | Description
-|-|-
TABLE_DOES_NOT_EXIST | 404 | The specified table doesn't exist.<br>The client should check if the table ID is correct for this table.
PERMISSION_DENIED | 403 | Insufficient permissions (SELECT on table).<br>The client should check with their Unity Catalog Admin to grant them the necessary rights to publish metrics.
INVALID_PARAMETER_VALUE | 400 | Server side check failed to post metrics for Unity Catalog Managed Tables.<br>The client has provided wrong inputs to the server. This could include but not limited to the following errors:(1) missing required fields, (2) provided fields are invalid such as non-positive numbers, etc.

## API Reference

Method | HTTP request | Description
-|-|-
[**createStagingTable**](../../api/Apis/TablesApi.md#createstagingtable) | **POST** /staging-tables | Create a Staging Table
[**createTable**](../../api/Apis/TablesApi.md#createtable) | **POST** /tables | Create a Table
[**getTable**](../../api/Apis/TablesApi.md#gettable) | **GET** /tables/{full_name} | Get a Table
[**getCommits**](../../api/Apis/DeltaCommitsApi.md#getcommits) | **GET** /delta/commits | Get Commits
[**writeCommit**](../../api/Apis/DeltaCommitsApi.md#writecommit) | **POST** /delta/commit | Write a Commit
[**writeMetrics**](../../api/Apis/DeltaCommitsApi.md#writemetrics) | **POST** /delta/metrics | Write Metrics

For full API documentation index, see [Catalog APIs README](../../api/README.md).

---

[[Back to Main README]](../../README.md)

