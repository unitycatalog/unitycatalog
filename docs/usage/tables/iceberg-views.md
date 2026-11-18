# Spark SQL views through the Iceberg REST catalog

The experimental view API supports create, load, exists, and drop for ordinary Spark SQL views
created through IRC. Each view is a native UC `VIEW` object and can also be read or deleted through
the native `/tables` API. Native-created views are not exposed through IRC in this release.

## Configuration

View support defaults to disabled. Set both properties in `etc/conf/server.properties`:

```properties
server.iceberg-view.enabled=true
server.iceberg-view.metadata-root=/absolute/path/to/private/view-metadata
```

The metadata directory must be durable and owned exclusively by the server. Multiple servers need
the same shared filesystem and absolute path. Back it up with the catalog database: it holds SQL
and schema metadata that the database references. Clients receive the metadata inline.

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

Create checks authority on the parent schema. Load and exists use native metadata visibility;
drop uses the native deletion policy. A view grants no credentials or access to its base tables.
Metadata publication and canonical creation share a database transaction. Failed creation can
leave an unreferenced metadata file; drop retains files. Automatic cleanup is not implemented.

Listing, replacement, property updates, rename/register, cloud metadata storage, and secure or
definer-rights execution are not included.
