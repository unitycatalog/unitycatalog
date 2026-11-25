# UC Iceberg REST API

Unity Catalog serves an Apache Iceberg REST catalog at `/api/2.1/unity-catalog/iceberg`. Iceberg clients such as
Apache Spark, Trino and PyIceberg can connect to it and work with Unity Catalog tables through the standard Iceberg
REST protocol.

In this API a Unity Catalog catalog is the Iceberg `warehouse`, and a Unity Catalog schema is an Iceberg `namespace`.

## Connecting a client

Point the client at `/api/2.1/unity-catalog/iceberg` and set `warehouse` to the name of a Unity Catalog catalog. Do
not put `catalogs/<catalog>` in the URI yourself.

On startup the client calls `GET /v1/config?warehouse=<catalog>`. The server answers with a `prefix` override of
`catalogs/<catalog>`, and the client adds it to every later request, so a call to list namespaces goes to
`/v1/catalogs/<catalog>/namespaces`. If the URI already ended in `catalogs/<catalog>`, the prefix would be added twice
and the requests would fail.

## Read and write access

The read endpoints are always available. You can use them to read native Iceberg tables and
[UniForm](../tables/uniform.md) tables.

The write endpoints (creating, updating, renaming and dropping namespaces and tables) are on by default. To turn them
off, set this in `etc/conf/server.properties` and restart the server:

```properties
server.iceberg-table.enabled=false
```

When writes are off, an authorized write request returns HTTP 400 with the message `Iceberg table writes are currently disabled.
To enable them, set 'server.iceberg-table.enabled=true' in server.properties`. Tables that were already created stay
readable. `GET /v1/config` only lists the endpoints the server supports, so the list it returns changes with this
setting.

## Endpoints

All paths below are relative to `/api/2.1/unity-catalog/iceberg`. The Operation column is the operation name in the
[Iceberg REST catalog spec](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml).

### Config

| Method | Path         | Operation   | Description                                                                                      |
|--------|--------------|-------------|--------------------------------------------------------------------------------------------------|
| GET    | `/v1/config` | `getConfig` | Returns the `prefix` override and the supported endpoints. Needs `warehouse` (the catalog name). |

### Namespaces

| Method | Path                                                       | Operation               | Description                         | Write |
|--------|------------------------------------------------------------|-------------------------|-------------------------------------|-------|
| GET    | `/v1/catalogs/{catalog}/namespaces`                        | `listNamespaces`        | List namespaces                     |       |
| POST   | `/v1/catalogs/{catalog}/namespaces`                        | `createNamespace`       | Create a namespace                  | Yes   |
| HEAD   | `/v1/catalogs/{catalog}/namespaces/{namespace}`            | `namespaceExists`       | Check if a namespace exists         |       |
| GET    | `/v1/catalogs/{catalog}/namespaces/{namespace}`            | `loadNamespaceMetadata` | Load a namespace and its properties |       |
| DELETE | `/v1/catalogs/{catalog}/namespaces/{namespace}`            | `dropNamespace`         | Drop an empty namespace             | Yes   |
| POST   | `/v1/catalogs/{catalog}/namespaces/{namespace}/properties` | `updateProperties`      | Set or remove namespace properties  | Yes   |

### Tables

| Method | Path                                                                       | Operation         | Description                                                                                                                     | Write |
|--------|----------------------------------------------------------------------------|-------------------|---------------------------------------------------------------------------------------------------------------------------------|-------|
| GET    | `/v1/catalogs/{catalog}/namespaces/{namespace}/tables`                     | `listTables`      | List tables                                                                                                                     |       |
| POST   | `/v1/catalogs/{catalog}/namespaces/{namespace}/tables`                     | `createTable`     | Create a table. Set `stage-create` to `true` to stage the create; the table is not registered until a commit with `assert-create` arrives. | Yes   |
| POST   | `/v1/catalogs/{catalog}/namespaces/{namespace}/register`                   | `registerTable`   | Register an existing metadata file as a managed Iceberg table, without moving or rewriting files. | Yes   |
| HEAD   | `/v1/catalogs/{catalog}/namespaces/{namespace}/tables/{table}`             | `tableExists`     | Check if a table exists                                                                                                         |       |
| GET    | `/v1/catalogs/{catalog}/namespaces/{namespace}/tables/{table}`             | `loadTable`       | Load table metadata                                                                                                             |       |
| POST   | `/v1/catalogs/{catalog}/namespaces/{namespace}/tables/{table}`             | `updateTable`     | Commit changes to a table                                                                                                       | Yes   |
| DELETE | `/v1/catalogs/{catalog}/namespaces/{namespace}/tables/{table}`             | `dropTable`       | Drop a table                                                                                                                    | Yes   |
| GET    | `/v1/catalogs/{catalog}/namespaces/{namespace}/tables/{table}/credentials` | `loadCredentials` | Get temporary storage credentials                                                                                               |       |
| POST   | `/v1/catalogs/{catalog}/namespaces/{namespace}/tables/{table}/metrics`     | `reportMetrics`   | Report scan or commit metrics                                                                                                   |       |
| POST   | `/v1/catalogs/{catalog}/tables/rename`                                     | `renameTable`     | Rename a table                                                                                                                  | Yes   |

### Views

| Method | Path                                                         | Operation  | Description                    |
|--------|--------------------------------------------------------------|------------|--------------------------------|
| GET    | `/v1/catalogs/{catalog}/namespaces/{namespace}/views/{view}` | `loadView` | Always returns 404 (see below) |

## Register an existing table

Send the table name and the path of its current Iceberg metadata file:

```json
{
  "name": "orders",
  "metadata-location": "s3://warehouse/existing/orders/metadata/v1.metadata.json"
}
```

Use `POST /v1/catalogs/{catalog}/namespaces/{namespace}/register`. Local absolute paths and
`file://` URLs are also accepted when they are inside configured storage.

The metadata file must contain `table-uuid`, including for format-v1 tables. A file without it
returns HTTP 400 before the table is registered. Registration does not add a UUID or rewrite the file.

The metadata file must be inside the table directory declared by the metadata. That directory must
be below an external location, the destination catalog or schema's assigned `storage_location`, or
the server's default managed table directory under `storage-root.tables/__unitystorage`. Paths under
a configured `storage_root` but outside its assigned `storage_location` use the covering external
location and require its privileges. A different catalog or schema's assigned managed directory
cannot be used through a covering external location.
The `tables`, `volumes`, `models`, `catalogs`, and `schemas` directories directly under a managed
storage boundary are reserved for UC allocation. Registration cannot adopt them or their children,
including directories of resources whose cleanup has completed.
These local directories are reserved by filesystem identity, including case aliases, even when no
table, volume, model, or cleanup task remains at the path.
Registration also rejects a table directory that contains the default managed table or model
allocation directory, even when a covering external location grants access to it.

All files referenced by retained snapshots must also be inside the table directory, including
manifest lists, manifests, data files, and delete files. Previous metadata files and statistics files
must also be strict descendants of the table directory. Configured data directories may equal the
table directory or be below it. Configured metadata directories must be below it. Registration reads
the retained snapshots' manifests to check these paths and rejects layouts that need access outside
the table directory.
Bucket-root and container-root external locations are supported; the metadata read uses a child
directory within that boundary.
Cloud locations, including the request's `metadata-location`, must preserve their object-key spelling:
dot segments, repeated path separators, queries, and fragments are rejected. Directory properties may
have a trailing slash.

The caller needs `USE_CATALOG` on the destination catalog and `USE_SCHEMA` plus `CREATE_TABLE` on the
destination schema. Ownership satisfies the corresponding catalog or schema tier. For an external
location, the caller must also own the location or hold all three privileges: `CREATE_MANAGED_STORAGE`,
`READ_FILES`, and `WRITE_FILES`. UC checks these privileges before reading the metadata file.

Registration rejects directory overlaps with tables, volumes, models, uncommitted staging tables,
pending storage cleanup, and retired registered directories. It also rejects shared storage roots
and directories that contain them. The discovered table root is checked before manifest traversal
and is checked again in the write transaction.
Table, volume, model, catalog, and schema storage allocation checks registered directories and
pending cleanup under a shared database lock, including when authorization is disabled. This is a
server-wide allocation lock on the singleton metastore row: storage allocations serialize across
namespaces and server instances that share the database, even when registration is disabled. This
keeps a later registration from racing an allocation; it can reduce allocation throughput.
Overlap checks treat `abfs` and `abfss` URLs for the same account, container, and path as one location,
including URLs stored before registration was supported.
Registration accepts these Azure scheme aliases in embedded file paths and directory properties too.
S3 and GCS file names and partition keys can contain literal percent sequences below a plain table
root. The table root and the initial metadata file's parent directory cannot contain percent escapes:
credential scopes decode URI paths and must name the same object prefix that registration checks.
Cloud storage allocation and path-credential requests also reject percent escapes, queries, and
fragments, including when registration or authorization is disabled. Azure registration rejects
percent escapes in all embedded paths too.
Local allocation URLs must use their canonical spelling. Registration and later allocation also
check filesystem aliases, including case aliases on case-insensitive filesystems and existing
ancestors of new directories. Registration rejects an overlap with a legacy local URL whose spelling
differs from the filesystem path. Recreate affected storage entries with canonical URLs where needed;
occupied directories still cannot be adopted. Missing paths below the same resolved ancestor are
reserved without distinguishing case, including after cleanup removes a registered directory.

Registration keeps the metadata file, table UUID, snapshots, schema, and properties. UC assigns a
separate catalog table ID. The table becomes `MANAGED`: dropping it schedules deletion of its original
directory after the normal storage cleanup delay. Stop other catalogs and writers from using this
directory before registering it. Registration does not copy files or coordinate with another catalog.
After cleanup completes, UC permanently reserves the registered directory. Do not reuse it for
another table, volume, model, or managed namespace: a storage delete from an expired cleanup attempt
may still be in flight. Completed reservations are not retried by the cleanup worker.
They do not keep the covering external location in use after cleanup completes. A new catalog or
schema can use a parent raw storage root if its generated storage directory is disjoint. Path
credentials remain blocked for the retired directory and its ancestors and descendants, because
credentials for an ancestor could allow writes into the retired directory.

External Iceberg creates check directory overlaps under the allocation lock before storage writes
or staged credential vending. External staged writes have no durable reservation. Stop these writers,
and any writers outside UC, before registration. Dropping a committed external table preserves its
files and permits later registration after other writers stop.

Malformed metadata or manifests return HTTP 400. Wrapped transport I/O failures return HTTP 503 so
clients can retry. Legacy format-v1 metadata with embedded manifest paths remains supported.

A taken table name returns HTTP 409. `overwrite=true` returns HTTP 400; use normal Iceberg commits to
update a registered table. Both `server.iceberg-table.enabled` and `server.managed-table.enabled` must
be enabled.
The config response advertises registration only when both flags are enabled.

## Limitations

- **Views are not supported.** The load view endpoint exists only because Iceberg clients look for a view when a
  table is not found. It always returns 404.
- **Nested namespaces are not supported.** A namespace maps to one Unity Catalog schema, so it can only have one level.
- **Renaming a table into another namespace is not supported.** The source and destination namespace must be the same.
- **UniForm tables are read-only here.** Commits, drops and renames work only on native Iceberg tables. Change UniForm
  tables through the [Unity Catalog REST API](index.md).
- **`purgeRequested` does not delete files of external tables.** Dropping an external table removes it from the
  catalog but leaves its data and metadata files in place, even when the client asks for a purge. Dropping a managed
  table schedules its storage directory for removal by the storage cleanup worker, after
  `server.storage-cleanup.initial-delay` (default `P7D`).
- **Local tables get no credentials.** `loadCredentials` returns an empty list for a local `file://` table. A caller
  without access to the table gets a permission error.
- **Metrics reports are accepted but not stored.**

## Quickstart

Start a local Unity Catalog server:

```sh
bin/start-uc-server
```

The sample UniForm table expects its files under `/tmp`, so copy it there first (see
[UniForm](../tables/uniform.md)):

```sh
cp -r etc/data/external/unity/default/tables/marksheet_uniform /tmp/marksheet_uniform
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

Load the sample table:

```sh
curl -s "$UC_ICEBERG/v1/catalogs/unity/namespaces/default/tables/marksheet_uniform" | jq .
```

## Engine and tool integrations

- [Spark](../tables/uniform.md#setting-up-rest-catalog-with-apache-spark)
- [Trino](../../integrations/unity-catalog-trino.md)
- [OLake Go](../../integrations/unity-catalog-olake.md) - replicate databases into Iceberg tables via this API
