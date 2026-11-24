# Spark SQL views through the Iceberg REST catalog

The experimental view API supports create, load, exists, and drop for ordinary Spark SQL views
created through IRC. Load and exists also support ordinary UC views created through the native
`/tables` API. Each name addresses the same canonical UC `VIEW` object.

## Configuration

View support defaults to disabled. Set both properties in `etc/conf/server.properties`:

```properties
server.iceberg-view.enabled=true
server.iceberg-view.metadata-root=s3://private-view-metadata
```

Upgrade all servers that write to the same catalog database, then enable views on all of them.
With views enabled, table and view creates and renames lock the parent schema before checking
a name. Existing duplicate names still require manual resolution.

Use the same durable metadata root on every server and keep its location stable. For S3,
dedicate a bucket to view metadata. Register the bucket as an admin-only external location
with a [server storage credential](../../server/aws.md) that can read, write, and delete
objects under `<root>/__unitystorage/views/`. Keep client IAM roles and keys, user-accessible
external locations, and table or volume storage out of that bucket. The server returns
metadata inline to Iceberg clients.

For a local filesystem, set the root to an absolute path or an absolute `file:` URI without a
host. Restrict the directory to the server and mount it at the same path on every replica. The
filesystem must support atomic renames to publish snapshots of UC-created views.

Back up the metadata root with the catalog database. UC stores the view identity, native
definition, and grants in the database. For an Iceberg-created view, the row also points to
the file returned on an Iceberg load. It preserves the submitted SQL, resolution context,
version timestamp, and summary.

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

Iceberg REST can read ordinary UC views created through the tables API without saved Spark
session settings. Spark 4.2 SQL `CREATE VIEW` captures ANSI mode and session time zone, even
when they have their default values. Iceberg's Spark reader evaluates SQL in its own session
and cannot apply those saved settings. Loading such a view through Iceberg returns HTTP 400.
Query these views through the UC Spark connector.

Supported UC views are exposed with dialect `spark`, their canonical UUID and output schema, and
their saved catalog and namespace context. The querying engine must have any saved catalog name
configured. Without a saved context, unqualified names resolve in the view's UC schema under the
catalog used to load the view. The engine evaluates the SQL and obtains each base table's
credentials as the caller.

IRC selects the saved query output names in their original order. When no query names were
saved, it uses the declared column names. This keeps a view over `SELECT *` bound to the same
columns after its source table is reordered. Query output names must be distinct ignoring case
and contain only letters, digits, or underscores. Spark can interpret quoted punctuation as a
regular expression depending on the reader's settings. The SQL may include one trailing semicolon.
Other semicolons, including those in strings and comments, and Spark variable substitutions
(`${...}`) are rejected.

For views with the default `COMPENSATION` schema mode, the projected SQL converts `INT` and
`BIGINT` outputs to their declared types. A null stays null, and an out-of-range value raises
an error even when the reader disables ANSI mode. Other output types use Iceberg's safe
upcasts. Other saved schema modes and non-default string collations return HTTP 400.

Reads leave the native view's SQL, columns, properties, owner, grants, timestamps, and stored
metadata pointer unchanged. The server materializes a reusable immutable metadata file named by
its contents. On load, UC rebuilds a missing or damaged snapshot from the current UC
definition at the same location. It reuses a matching file without rewriting it. Snapshots
represent the current definition and do not expose native version history.

Operators can reclaim old native snapshots after clients have finished using their returned
locations. Limit cleanup to files named `native-<64 hex digits>.metadata.json` in view UUID
directories. These files can also be removed after a native view is deleted. Retain the
UUID-named metadata files for Iceberg-created views. Their catalog rows reference them directly.

Native types without an exact Iceberg representation, including `byte`, `short`, and `char`,
return HTTP 400. Metric views are excluded. An authorized Iceberg DROP of a UC-created view
returns HTTP 400. Delete it through the UC `/tables` API.

Listing, replacement, property updates, rename/register, and secure or definer-rights execution
are outside the scope of this release.
