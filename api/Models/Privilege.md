# Privilege

The privilege to grant.

## Properties

| Name | Type | Description | Notes |
|------------ | ------------- | ------------- | -------------|


## Enum Values

| Value | Description |
|-------|-------------|
| `CREATE CATALOG` | Enables users to create new catalogs in a metastore. |
| `USE CATALOG` | Required to interact with any object within a catalog. |
| `CREATE SCHEMA` | Enables users to create new schemas in a catalog. |
| `USE SCHEMA` | Required to interact with any object within a schema. |
| `CREATE TABLE` | Allows creating tables or views in a schema. |
| `SELECT` | Enables querying tables, views, materialized views. |
| `MODIFY` | Enables adding, updating, and deleting data within tables (requires SELECT privilege also). |
| `CREATE FUNCTION` | Allows users to create functions or procedures within a schema. |
| `EXECUTE` | Permits invoking user-defined functions or loading models for inference. |
| `CREATE VOLUME` | Enables creating volumes in a schema. |
| `READ VOLUME` | Permits reading files and directories within volumes. |
| `CREATE MODEL` | Permits establishing MLflow registered models in a schema. |
| `CREATE EXTERNAL LOCATION` | Required privilege on both the metastore and referenced storage credential to create external locations. |
| `READ FILES` | Grants direct read access to files in cloud storage configured as external locations. |
| `WRITE FILES` | Grants direct write access to files in cloud storage configured as external locations. |
| `CREATE EXTERNAL TABLE` | Enables creating external tables through external locations. |
| `CREATE EXTERNAL VOLUME` | Enables creating external volumes through external locations. |
| `CREATE MANAGED STORAGE` | Allows designating managed storage locations at catalog or schema levels. |
| `CREATE STORAGE CREDENTIAL` | Required privilege on the metastore to create storage credentials. |

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)

