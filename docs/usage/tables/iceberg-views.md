# Spark SQL views through the Iceberg REST catalog

The experimental view API supports create, load, exists, and drop for ordinary Spark SQL views
created through IRC. Load and exists also support ordinary UC views created through the native
`/tables` API. Each name addresses the same canonical UC `VIEW` object.

## Configuration

View support defaults to disabled. Set both properties in `etc/conf/server.properties`:

```properties
server.iceberg-view.enabled=true
server.iceberg-view.metadata-root=/absolute/path/to/private/view-metadata
```

Enable this option on every server that writes to the same catalog database after upgrading them all.
An enabled server requires a metadata root at startup.
Table and view creates and renames then take a lock on the parent schema before checking a name.
Existing duplicate names are left for the operator to resolve.

The metadata directory must be durable and owned exclusively by the server. Multiple servers need
the same shared filesystem and absolute path. Back it up with the catalog database: it holds SQL
and schema metadata that the database references. Clients receive the metadata inline.
Native view snapshots also require atomic renames within that directory.

## API

The base URI is `/api/2.1/unity-catalog/iceberg`. Obtain `{prefix}` from
`GET /v1/config?warehouse={catalog}`.

| Operation | Method and path relative to the base URI |
| --- | --- |
| Create | `POST /v1/{prefix}/namespaces/{namespace}/views` |
| Load | `GET /v1/{prefix}/namespaces/{namespace}/views/{view}` |
| Exists | `HEAD /v1/{prefix}/namespaces/{namespace}/views/{view}` |
| Drop | `DELETE /v1/{prefix}/namespaces/{namespace}/views/{view}` |

Use standard Iceberg REST models with exactly one SQL representation and dialect `spark`.
The engine supplies valid SQL and its output schema; UC does not execute or analyze SQL. Views
require at least one output column. The server assigns metadata locations and reserves `view.*`
properties for native Spark context. Resolution namespaces have at most one level.
When a view declares column names different from its SELECT outputs, Spark sends the query
names in `spark.query-column-names` so the UC connector can resolve the view. Clients that omit
this property must use the SELECT output names in the view schema. Use explicit SELECT aliases
for query output names that contain commas.

Create checks authority on the parent schema. Load and exists use native metadata visibility;
drop uses the native deletion policy. A view grants no credentials or access to its base tables.
Create writes the metadata file before locking the schema, then inserts the UC view and its
metadata location in one database transaction. A failed create can leave an unreferenced file.
Drop retains metadata files.

## Reading native UC views

Existing UC views are exposed with dialect `spark`, their canonical UUID and output schema, and
their saved catalog and namespace context. The querying engine must have those catalog names
configured. It evaluates the SQL and obtains each base table's credentials as the caller.

IRC selects the saved query output names in their original order. When no query names were
saved, it uses the declared column names. This keeps a view over `SELECT *` bound to the same
columns after its source table is reordered. Query output names must be distinct ignoring case
and contain only letters, digits, or underscores. Spark can interpret quoted punctuation as a
regular expression depending on the reader's settings. The SQL may include one trailing semicolon.
Other semicolons, including those in strings and comments, and Spark variable substitutions
(`${...}`) are rejected.

Reads leave the native view's SQL, columns, properties, owner, grants, timestamps, and stored
metadata pointer unchanged. The server materializes a reusable immutable metadata file named by
its contents. Missing native snapshots can be rebuilt on a later read. They represent the current
native definition and do not expose native version history. Files are retained after deletion;
failed writes can also leave temporary files.

Saved Spark session settings and other resolution properties that Iceberg cannot carry return a
bad-request error. Native types without an exact Iceberg representation, including `byte`, `short`,
and `char`, are rejected. Metric views are excluded. Native-created views remain outside IRC drop;
delete them through the native `/tables` API.

Listing, replacement, property updates, rename/register, cloud metadata storage, and secure or
definer-rights execution are not included.
