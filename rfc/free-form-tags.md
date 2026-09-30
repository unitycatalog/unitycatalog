# Free-form Tags

**Associated Github issue for discussions: TODO (open an issue and link it here, as #1833 accompanies #1832)**

## Overview

A **tag** is a catalog-managed key/value annotation on a Unity Catalog securable or column.
Tags are catalog metadata, not table state: a server **must not** write them into a table's
`metadata.json`, version them with table history, or persist them across a catalog boundary.
They classify objects (`sensitivity=restricted`, `domain=finance`, `owner=data-platform`),
power discovery and AI reasoning, and — through the Iceberg REST Catalog — let external
engines enforce governance the catalog itself does not.

This RFC defines **free-form** tags: an open `key -> value?` space with no predefined
vocabulary. It proposes a native tag surface as an addition to
[`api/all.yaml`](https://github.com/unitycatalog/unitycatalog/blob/main/api/all.yaml), and a
projection onto the Iceberg REST Catalog where tags surface as `labels` on `LoadTableResponse`
and are written through an `UpdateLabels` verb. It reuses existing Unity Catalog conventions:
the `/api/2.1/unity-catalog` endpoint root, dotted `full_name` addressing, bearer-token
authentication, `snake_case` wire fields, and the standard error body.

This is an **RFC**, not an implementation and not a spec rewrite. A **governed** tag tier
(allowed values, assignment control), tag **inheritance**, and **policy/ABAC** are explicit
non-goals here; each layers on later without changing this contract.

## Motivation

Every mature catalog exposes tags — Lakekeeper and Gravitino ship them, Polaris is adding
them — because three needs recur:

- **Governance exchange over IRC.** An admin tags `sensitivity=restricted` on a table and
  `pii-type=email` on a column; the catalog surfaces them as `labels` on `LoadTableResponse`;
  an external engine (ClickHouse, Snowflake, Spark) reads them and enforces natively. One
  classification, every engine enforces, no runtime coordination.
- **Discovery and AI.** `domain`, `tier`, `owner` let search, BI, and agents find the right
  table among thousands.
- **A standalone lightweight catalog.** Free-form tags are the portable core that makes
  Unity Catalog OSS usable as a governed catalog without a policy engine.

Unity Catalog already stores generic `properties` on securables. Tags are deliberately
separate: properties are securable configuration, authored by the object's owner; tags are
classification metadata, authored by a different persona (steward, classifier, discovery
tooling), read over IRC as `labels`, and constrained to never affect table behaviour.
Overloading `properties` with a reserved key prefix makes that prefix a semantic and
permission boundary, which is fragile and non-portable. Tags get their own resource.

--------

<!-- Proposed additions to api/all.yaml (Unity Catalog API) follow. -->

> ***New "Tags" resource, addressed as a sub-resource of each securable.***

## Tags

A tag is a `(tag_key, tag_value?)` pair attached to a securable or a column. `tag_value` is
optional; its absence denotes a marker (presence-only) tag. A given `tag_key` carries at most
one value per object: re-setting a key replaces its value.

Tags attach to **catalogs, schemas, tables, views, volumes, functions, registered models, and
table columns**. A securable is addressed by its existing dotted `full_name`; a column by its
name within a table. Column tags are stored against the column's stable catalog identity, not
its ordinal, so they survive schema evolution.

`tag_key` and `tag_value` are each 1 to 256 characters, `tag_key` is case-sensitive, and
neither may contain control characters or leading/trailing whitespace. A server **must**
reject a key or value outside these bounds with `BadRequestException` (400).

A server holds at most **50** tags per securable and at most **1000** column-tag assignments
per table. Exceeding either **must** fail with `BadRequestException` (400) without applying the
request. (These bounds match Unity Catalog's managed service so tags round-trip.)

Keys beginning `system.` or `uc.` are **reserved**: a server surfaces them on read but
**must** reject a client write to a reserved key with `TagKeyNotWritable` (403). This is the
catalog-managed-versus-client-writable boundary; it needs no governed tier.

### Authorization and preconditions

Writing a tag requires `APPLY TAG` on the target securable (or its table, for a column tag),
plus `USE CATALOG` and `USE SCHEMA` on the parents; ownership satisfies `APPLY TAG`. Reading
tags requires the privilege that already lets the caller see the securable. Reverse lookup is
privilege-aware: a caller sees only assignments on objects it may see.

Per Unity Catalog convention the Errors tables below list only endpoint-specific errors. Every
endpoint may also return `NotAuthorized` (401), `PermissionDenied` (403), and
`InternalServerError` (500).

### Set Tag

```text
PUT /api/2.1/unity-catalog/{securable}/{full_name}/tags/{tag_key}
```

`{securable}` is one of `catalogs|schemas|tables|volumes|functions|models`; `{full_name}` is
that securable's identifier (`name` for a catalog). Creates or replaces one tag. Idempotent:
re-setting the same key and value changes nothing.

Field Name | Data Type | Description | Optional/Required
-|-|-|-
*body* | object | | required
&nbsp;&nbsp;tag_value | string | Value to set. Omit for a marker (presence-only) tag. | optional

**200: Tag set.** The response is the resulting tag.

```json
{ "tag_key": "sensitivity", "tag_value": "restricted", "source": "manual" }
```

Error Type | HTTP Status | Description
-|-|-
BadRequestException | 400 | Key/value out of bounds, or per-object tag limit exceeded.
TagKeyNotWritable | 403 | The key is reserved (`system.` / `uc.`).
NotFoundException | 404 | The securable does not exist.

### List Tags

```text
GET /api/2.1/unity-catalog/{securable}/{full_name}/tags
```

**200:** `{ "tags": [ { "tag_key", "tag_value", "source" } ] }`. Returns tags set directly on
the securable. (Inherited/effective reads are a non-goal of this RFC.)

### Delete Tag

```text
DELETE /api/2.1/unity-catalog/{securable}/{full_name}/tags/{tag_key}
```

Idempotent: deleting an absent key succeeds. **204** on success.

### Column Tags

```text
GET    /api/2.1/unity-catalog/tables/{full_name}/columns/{column_name}/tags
PUT    /api/2.1/unity-catalog/tables/{full_name}/columns/{column_name}/tags/{tag_key}
DELETE /api/2.1/unity-catalog/tables/{full_name}/columns/{column_name}/tags/{tag_key}
GET    /api/2.1/unity-catalog/tables/{full_name}/columns/tags
```

Bodies, responses, and errors match the securable endpoints. The last form returns every
tagged column of a table at once:

```json
{ "columns": [ { "column_name": "email", "tags": [ { "tag_key": "pii-type", "tag_value": "email", "source": "manual" } ] } ] }
```

A `{column_name}` not present in the table's current schema **must** fail with
`NotFoundException` (404).

### Find Objects by Tag

```text
GET /api/2.1/unity-catalog/tags?tag_key=&tag_value=&securable_type=&catalog_name=&schema_name=&max_results=&page_token=
```

The load-bearing governance query ("find every restricted column"). At least one narrowing
filter (`tag_key`, or a `catalog_name`/`schema_name` scope) is **required**; a server **must**
reject an unnarrowed request with `BadRequestException` (400). Results are paginated with the
existing `max_results` / `page_token` convention; a server **must not** silently truncate.

**200:**

```json
{
  "assignments": [
    { "securable_type": "table",  "full_name": "prod.sales.orders",       "tag_key": "sensitivity", "tag_value": "restricted", "source": "manual" },
    { "securable_type": "column", "full_name": "prod.sales.orders.email", "tag_key": "pii-type",    "tag_value": "email",      "source": "manual" }
  ],
  "next_page_token": "..."
}
```

The result shape mirrors the managed service's `INFORMATION_SCHEMA.*_TAGS` views so the query
ports across Unity Catalog deployments.

--------

<!-- Proposed additions to the Iceberg REST Catalog surface follow. -->

> ***Additions to `LoadTableResponse` and a new `UpdateLabels` verb. On the Iceberg wire, tags
> are named `labels`, the Iceberg-spec term; the data is the same.***

## Tags over the Iceberg REST Catalog

Unity Catalog serves an Iceberg REST Catalog. Tags project onto it as `labels`, so any Iceberg
client reads and writes them through one standard surface. Values are `Map<string,string>`,
opaque to Iceberg; the catalog defines their meaning, engines interpret them.

### Read: labels on LoadTableResponse

`LoadTableResponse` gains an optional `labels` object. A server populates it from the table's
tags and its columns' tags, resolving each column's catalog identity to its Iceberg
**field-id** from the loaded schema.

```jsonc
{
  "metadata-location": "...", "metadata": { ... },
  "labels": {
    "table":   { "sensitivity": "restricted", "domain": "finance" },
    "columns": [ { "field-id": 3, "labels": { "pii-type": "email" } } ]
  }
}
```

Schema tags project as namespace-level labels on the namespace load. `labels` is optional; a
client that does not understand it ignores it. Column labels use `field-id` so they stay
stable across schema evolution.

> The `labels` field on `LoadTableResponse` tracks the upstream Iceberg proposal
> ([apache/iceberg#15750](https://github.com/apache/iceberg/pull/15750)); until it merges,
> Unity Catalog serves it as an additive extension of the response.

### Write: UpdateLabels

```text
POST /v1/catalogs/{catalog}/namespaces/{namespace}/tables/{table}/labels
If-Match: "<etag>"
```

Writes table and column labels in one request, mapping to the native tag store. Entries
without `field-id` target the table; entries with `field-id` target that column.

Field Name | Data Type | Description | Optional/Required
-|-|-|-
*body* | object | | required
&nbsp;&nbsp;updates | array of object | `{ key, value, field-id? }` to set. | optional
&nbsp;&nbsp;removals | array of object | `{ key, field-id? }` to remove. | optional

The write is **atomic** (all updates and removals apply, or none) and **optimistically
concurrent**: the `If-Match` ETag comes from the most recent label read, and a stale token
fails with `412`. A write **must not** touch `metadata.json` or create a snapshot. A write to a
reserved (`system.`/`uc.`) key fails the whole request with `403`.

**200:** the full post-update label set (`labels` + `column-labels`) and a fresh ETag.

Error Type | HTTP Status | Description
-|-|-
LabelKeyNotWritable | 403 | A targeted key is reserved / catalog-managed.
TableNotFound | 404 | The table does not exist.
ETagMismatch | 412 | Optimistic-concurrency check failed.

> ***Add to the endpoint list returned by `/v1/config`.***

```text
POST /v1/catalogs/{catalog}/namespaces/{namespace}/tables/{table}/labels
```

A server that serves `labels` on read but does not accept writes omits this endpoint; a client
discovers write support by its presence, not by a failed request. Engine SQL DDL
(`ALTER TABLE ... SET LABEL`, or the managed service's `SET TAGS`) translates to this verb and
is engine-side work, out of scope here.

## Non-Goals

- **Governed tags.** A named tag definition with allowed values and assignment control is a
  separate, larger tier. Free-form tags are the substrate it will build on; keeping values
  open here leaves that room without pre-committing its shape.
- **Inheritance / effective reads.** This RFC returns tags set directly on an object. Resolving
  a catalog -> schema -> table effective set (most-specific-wins, computed at read time) is a
  follow-up.
- **Policy / ABAC.** Tags classify; they do not grant or deny access. Enforcement built on
  tags is a separate proposal.
- **Multi-value keys, nested-field tags, and cross-catalog tag portability.**

## Follow-up PRs

If the shape looks right: the `api/all.yaml` additions (`TagInfo`, list/search responses,
`UpdateLabels` request/response) with regenerated models; the server implementation (`TagDAO`
mirroring `PropertyDAO`, `TagRepository`, `TagService`, and the `IcebergRestCatalogService`
projection) with tests; then the `UpdateLabels` write path.
