# UC Iceberg REST API

Unity Catalog serves an Apache Iceberg REST catalog at `/api/2.1/unity-catalog/iceberg`. Iceberg clients such as
Apache Spark, Trino and PyIceberg can connect to it and work with Unity Catalog tables through the standard Iceberg
REST protocol.

In this API a Unity Catalog catalog is the Iceberg `warehouse`, and a Unity Catalog schema is an Iceberg `namespace`.

## Read and write access

The read endpoints are always available. You can use them to read Iceberg tables and
[UniForm](../tables/uniform.md) tables.

The write endpoints (creating, updating, renaming and dropping namespaces and tables) are turned off by default. To
turn them on, set this in `etc/conf/server.properties` and restart the server:

```properties
server.iceberg-table.enabled=true
```

When writes are off, a write request fails with an error that says so. Tables that were already created stay readable.

`GET /v1/config` tells the client which endpoints the server supports, so the list it returns changes with this
setting.

## Endpoints

All paths below are relative to `/api/2.1/unity-catalog/iceberg`.

### Config

| Method | Path         | Description                                                                                       |
|--------|--------------|---------------------------------------------------------------------------------------------------|
| GET    | `/v1/config` | Returns the catalog prefix and the supported endpoints. Needs the `warehouse` parameter (catalog name). |

### Namespaces

| Method | Path                                                       | Description                         | Write |
|--------|------------------------------------------------------------|-------------------------------------|-------|
| GET    | `/v1/catalogs/{catalog}/namespaces`                        | List namespaces                     |       |
| POST   | `/v1/catalogs/{catalog}/namespaces`                        | Create a namespace                  | Yes   |
| HEAD   | `/v1/catalogs/{catalog}/namespaces/{namespace}`            | Check if a namespace exists         |       |
| GET    | `/v1/catalogs/{catalog}/namespaces/{namespace}`            | Load a namespace and its properties |       |
| DELETE | `/v1/catalogs/{catalog}/namespaces/{namespace}`            | Drop an empty namespace             | Yes   |
| POST   | `/v1/catalogs/{catalog}/namespaces/{namespace}/properties` | Set or remove namespace properties  | Yes   |

### Tables

| Method | Path                                                                   | Description                             | Write |
|--------|------------------------------------------------------------------------|-----------------------------------------|-------|
| GET    | `/v1/catalogs/{catalog}/namespaces/{namespace}/tables`                 | List tables                             |       |
| POST   | `/v1/catalogs/{catalog}/namespaces/{namespace}/tables`                 | Create a table (staged create supported) | Yes   |
| HEAD   | `/v1/catalogs/{catalog}/namespaces/{namespace}/tables/{table}`         | Check if a table exists                 |       |
| GET    | `/v1/catalogs/{catalog}/namespaces/{namespace}/tables/{table}`         | Load table metadata                     |       |
| POST   | `/v1/catalogs/{catalog}/namespaces/{namespace}/tables/{table}`         | Commit changes to a table               | Yes   |
| DELETE | `/v1/catalogs/{catalog}/namespaces/{namespace}/tables/{table}`         | Drop a table                            | Yes   |
| GET    | `/v1/catalogs/{catalog}/namespaces/{namespace}/tables/{table}/credentials` | Get temporary storage credentials   |       |
| POST   | `/v1/catalogs/{catalog}/namespaces/{namespace}/tables/{table}/metrics` | Report scan or commit metrics           |       |
| POST   | `/v1/catalogs/{catalog}/tables/rename`                                 | Rename a table                          | Yes   |

### Views

| Method | Path                                                         | Description                       |
|--------|--------------------------------------------------------------|-----------------------------------|
| GET    | `/v1/catalogs/{catalog}/namespaces/{namespace}/views/{view}` | Always returns 404 (see below)    |

## Limitations

- **Views are not supported.** The load view endpoint exists only because Iceberg clients look for a view when a
  table is not found. It always returns 404.
- **Nested namespaces are not supported.** A namespace maps to one Unity Catalog schema, so it can only have one level.
- **Renaming a table into another namespace is not supported.** The source and destination namespace must be the same.
- **Only tables created through this API can be dropped or renamed here.** UniForm tables are Delta tables, so drop or
  rename them through the [Unity Catalog REST API](index.md).
- **Metrics reports are accepted but not stored.**

## Quickstart

Start a local Unity Catalog server:

```sh
bin/start-uc-server
```

Set the base path and get the config for the `unity` catalog:

```sh
export UC_ICEBERG="http://localhost:8080/api/2.1/unity-catalog/iceberg"

curl -s "$UC_ICEBERG/v1/config?warehouse=unity" | jq .
```

List the namespaces and the tables in `default`:

```sh
curl -s "$UC_ICEBERG/v1/catalogs/unity/namespaces" | jq .
curl -s "$UC_ICEBERG/v1/catalogs/unity/namespaces/default/tables" | jq .
```

Load a sample UniForm table:

```sh
curl -s "$UC_ICEBERG/v1/catalogs/unity/namespaces/default/tables/marksheet_uniform" | jq .
```

## Engine integrations

- [Spark](../tables/uniform.md#read-delta-uniform-tables-via-iceberg-rest-catalog)
- [Trino](../../integrations/unity-catalog-trino.md)