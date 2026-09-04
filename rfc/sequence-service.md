# Sequences for Concurrent Identity Columns

**Associated Github issue for discussions: https://github.com/unitycatalog/unitycatalog/issues/XXXX**
<!-- Replace XXXX with the actual issue number once the change request is filed. -->

**Companion Delta protocol RFC:
[Concurrent Identity Columns](https://github.com/delta-io/delta/pull/7272)**

## Overview

The Delta [Concurrent Identity Columns](https://github.com/delta-io/delta/pull/7272)
writer feature (`concurrentIdentityColumns`) removes the identity-column high-water mark
from the write path. Instead of deriving values from `delta.identity.highWaterMark` and
rewriting that key in the commit that writes the data, a writer binds each identity column
to a **sequence** hosted by the table's catalog, reserves disjoint ranges of values from it,
and assigns those values locally. Because the ranges are disjoint by construction,
uniqueness no longer depends on winning the commit.

The Delta RFC leaves the allocation wire protocol to the catalog. This RFC defines it for
Unity Catalog proposing three endpoints on a new sequences sub-resource of a catalog-managed
Delta table, and the arithmetic and guarantees they adhere to.

The feature depends on `catalogManaged`, so the surface belongs to the Delta v1 API and is
specified as an addition to the
[Unity Catalog Managed Tables Specification](https://github.com/unitycatalog/unitycatalog/blob/main/spec/protocols/ManagedTablesSpec.md).
It reuses that specification's conventions unchanged: the `/delta/v1` endpoint root,
hyphenated wire field names, bearer-token authentication, the `DeltaErrorResponse` error
body, and endpoint advertisement through `config`. It adds no new `DeltaErrorType` value.

## Motivation

An identity column today admits one concurrent writer of generated values. Two writers that
read the same high-water mark generate overlapping values, so the loser must abort and retry.
The high-water mark is table metadata, so every write that generates values contends on it.
The contention grows with write concurrency rather than with data volume.

Moving allocation into the catalog turns a metadata conflict into a short, independent
request. A writer reserves a range before it needs values, buffers what it reserved, and
commits without touching any shared counter. The costs are a network dependency in the write
path and gaps in the generated values, both of which this RFC makes explicit.

--------

<!-- Proposed additions to spec/protocols/ManagedTablesSpec.md follow. -->

> ***New section after [Report Metrics](https://github.com/unitycatalog/unitycatalog/blob/main/spec/protocols/ManagedTablesSpec.md#report-metrics) in APIs.***

## Sequences

A **sequence** is a monotonic counter owned by one catalog-managed table, identified by an
opaque `sequence-id`, from which clients reserve ranges of identity-column values. A
concurrent identity column carries its sequence's identifier in the
`delta.identity.concurrent.sequenceId` column-metadata key. The Delta schema is source of
truth for the binding, the catalog for what the identifier resolves to and which values it
has issued.

`sequence-id` is client-generated. A server **must not** parse, interpret, or generate one; it
is stored verbatim. Clients **must** use a UUID. It **must** be 1 to 128 characters.

Each identity column has its own sequence; sequences are never shared between columns or
tables. A server holds per sequence an immutable `start` and non-zero `step`, and the last
issued value (`liv`), which is absent until the first reservation. Sequences follow their
table, rename does not affect them, deletion removes them.

### Value calculation

A sequence issues values of the form `start + k * step` for distinct non-negative integers
`k`, in increasing `k`. The requirement on a reservation of `count` values is that it returns
a contiguous run of them continuing strictly after every value the sequence has already
issued, which fixes the result:

```text
range-start = start        on the first reservation
range-start = liv + step   afterwards
range-end   = range-start + step * (count - 1)
```

The reserved range is the inclusive set of `count` values
`range-start, range-start + step, ..., range-end`, and `liv` becomes `range-end`. Ordering is
by `k`, not by numeric value, so a negative `step` needs no special case. `range-end` is then
numerically less than `range-start`.

**`count` must be at least 1.** A `count` of 0 or negative is invalid and **must** be
rejected with `BadRequestException` (400) without advancing any sequence. An empty range
cannot satisfy the formula, and every successful reservation consumes values.

`count` of 1 is valid, and is how the Delta feature reads back a single value as a classic
high-water mark when removing the binding. Then `range-end` equals `range-start`, and that
value is consumed like any other and never assigned to a row.

All arithmetic **must** be checked. If any intermediate or final value would fall outside the
signed 64-bit range, the request **must** fail with `BadRequestException` (400) and **must
not** advance any sequence. Values **must not** wrap.

### Guarantees

For every sequence a server **must** guarantee:

1. **Disjointness.** No value appears in the response of more than one successful reservation.
2. **Durability before acknowledgement.** The advance of `liv` is durable before the response
   is sent, including across a server restart.
3. **Linearizability.** Reservations on one sequence behave as if executed in some serial
   order consistent with real time.
4. **Request atomicity.** A request either applies to every sequence it names or to none.

Not guaranteed: gaplessness (an abandoned or under-filled range leaves values permanently
unused, and a server **must not** reclaim them), ordering against commit version, and
cross-table atomicity.

**Reservation is not idempotent.** A retry allocates a new range and permanently abandons the
previous one. A server **must not** return 4xx for a request whose durable effect it cannot
rule out; it **must** return `CommitStateUnknownException` (500), and the client **must** then
treat the values as consumed and reserve again.

### Lifecycle

A sequence exists from [Create Sequences](#create-sequences) until
[Drop Sequences](#drop-sequences), which deletes it together with its allocation state.
Reservations against a dropped sequence fail with `NotFoundException` (404).

A client **must never** reuse a dropped `sequence-id`. Re-creating one restarts allocation at
`start` and re-issues values already committed to data files. A client that needs values again
mints a fresh identifier, as Delta's repair operation already does.

### Authorization and preconditions

Every endpoint requires `MODIFY` on the table, plus `USE CATALOG` and `USE SCHEMA` on the
parents; table ownership satisfies `MODIFY`. The `sequence-id` currently is not used in the
authentication, a server **must** authorize against the table on every request.

A server **must** reject requests for a table that is not a catalog-managed Delta table with
`BadRequestException` (400). It **must not** additionally require `concurrentIdentityColumns`
in `writerFeatures`: Delta enables the feature and persists its bindings in one version, so
sequences are created *before* that commit and dropped *after* the commit that removes it.
Gating on the feature would reject both.

A request carries at most 64 entries. Exceeding that **must** fail the whole request with
`BadRequestException` (400) without applying it partially.

Per the convention of this specification, the Errors tables below list only endpoint-specific
errors. Every endpoint may also return `NotAuthorizedException` (401),
`PermissionDeniedException` (403), `TooManyRequestsException` (429), and
`InternalServerErrorException` (500).

### Create Sequences

```text
POST .../v1/catalogs/{catalog}/schemas/{schema}/tables/{table}/sequences
```

Creates sequences on a table, atomically. **Create-or-get**: repeating the request with the
same `start` and `step` succeeds and changes nothing, in particular it does not reset the
frontier, so a retry after the client has committed the binding is safe. Creation reserves no
value; the first reservation begins at `start`.

Field Name | Data Type | Description | Optional/Required
-|-|-|-
table-id | string (uuid) | Table UUID. Must identify the table named by the path; the server rejects a mismatch. From `loadTable` response `metadata.table-uuid`. | required
sequences | array of object | Must be non-empty, with `sequence-id` unique within the array. | required
&nbsp;&nbsp;sequences[].sequence-id | string | Client-generated opaque identifier, 1 to 128 characters. | required
&nbsp;&nbsp;sequences[].start | int64 | First value issued. The column's `delta.identity.start`. | required
&nbsp;&nbsp;sequences[].step | int64 | Increment. The column's `delta.identity.step`. Must not be zero. | required
&nbsp;&nbsp;sequences[].column-information | string | Audit only, at most 256 characters: which column this sequence was minted for, for debugging. Never a key. A server **must not** store it or behave differently on it. For Delta, the column's physical (rename-stable) name. | optional

**204: All sequences exist with the requested parameters.** No response body.

```json
{
  "table-id": "550e8400-e29b-41d4-a716-446655440000",
  "sequences": [
    { "sequence-id": "5f0a1c9e-1f4d-4a2b-9f1e-6d3a7c2b8e04", "start": 1, "step": 1 },
    { "sequence-id": "b71c4f2a-88de-4c6f-9a03-2e5b1d7f6c11", "start": 0, "step": -10 }
  ]
}
```

Error Type | HTTP Status | Description
-|-|-
BadRequestException | 400 | Empty `sequences`, more than 64 entries, duplicate `sequence-id`, `step` of zero, identifier longer than 128 characters, or the table is not a catalog-managed Delta table.
InvalidParameterValueException | 400 | `table-id` does not identify the table named by the path, or a required field is missing.
NoSuchTableException | 404 | The table does not exist.
AlreadyExistsException | 409 | An identifier already names a sequence with a different `start` or `step`.

### Reserve Ranges

```text
POST .../v1/catalogs/{catalog}/schemas/{schema}/tables/{table}/sequences/reserve
```

Reserves one range from each named sequence and advances each frontier past the range
returned, atomically. **Not idempotent**, see [Guarantees](#guarantees) before writing retry
logic. Clients **should** reserve more than one row's worth of values, buffer the remainder
locally, and batch all of a table's columns into one request.

Field Name | Data Type | Description | Optional/Required
-|-|-|-
table-id | string (uuid) | Table UUID. Must identify the table named by the path. | required
reservations | array of object | Must be non-empty, with `sequence-id` unique within the array. | required
&nbsp;&nbsp;reservations[].sequence-id | string | Sequence to advance. | required
&nbsp;&nbsp;reservations[].count | int64 | Number of values to reserve. Must be at least 1; 0 is invalid. | required
&nbsp;&nbsp;reservations[].step | int64 | Advisory: the client's schema-declared `delta.identity.step`. A server does not validate it; the field exists for future server-side validation. | optional

**200: All ranges reserved.**

Field Name | Data Type | Description | Optional/Required
-|-|-|-
ranges | array of object | One entry per reservation, positional with the request array. Clients **must** match by position, not by searching on `sequence-id`. | required
&nbsp;&nbsp;ranges[].sequence-id | string | The sequence the range came from. | required
&nbsp;&nbsp;ranges[].range-start | int64 | First reserved value, inclusive. | required
&nbsp;&nbsp;ranges[].range-end | int64 | Last reserved value, inclusive. Equals `range-start` when `count` is 1; numerically below it when `step` is negative. | required
&nbsp;&nbsp;ranges[].step | int64 | The stored `step` the range was reserved against. | required

Because a server does not validate the advisory request `step`, a client **must** compare the
returned `step` against its schema-declared `delta.identity.step` before using a range, and
**must** fail the write on a mismatch rather than generate values from the wrong stride.

Request, then a response in which both sequences had already issued values (frontiers 4096
and -70), so each range starts one `step` past its frontier:

```json
{
  "table-id": "550e8400-e29b-41d4-a716-446655440000",
  "reservations": [
    { "sequence-id": "5f0a1c9e-1f4d-4a2b-9f1e-6d3a7c2b8e04", "count": 1024, "step": 1 },
    { "sequence-id": "b71c4f2a-88de-4c6f-9a03-2e5b1d7f6c11", "count": 8, "step": -10 }
  ]
}
```

```json
{
  "ranges": [
    { "sequence-id": "5f0a1c9e-1f4d-4a2b-9f1e-6d3a7c2b8e04", "range-start": 4097, "range-end": 5120, "step": 1 },
    { "sequence-id": "b71c4f2a-88de-4c6f-9a03-2e5b1d7f6c11", "range-start": -80, "range-end": -150, "step": -10 }
  ]
}
```

Error Type | HTTP Status | Description
-|-|-
BadRequestException | 400 | Empty `reservations`, more than 64 entries, duplicate `sequence-id`, `count` less than 1, the range would fall outside the signed 64-bit domain, or the table is not a catalog-managed Delta table.
InvalidParameterValueException | 400 | `table-id` does not identify the table named by the path, or a required field is missing.
NoSuchTableException | 404 | The table does not exist.
NotFoundException | 404 | A named sequence does not exist on this table, or has been dropped.
CommitStateUnknownException | 500 | Outcome unknown. The values may or may not have been consumed; the client must treat them as consumed.

### Drop Sequences

```text
DELETE .../v1/catalogs/{catalog}/schemas/{schema}/tables/{table}/sequences
```

Deletes sequences together with their allocation state. Idempotent and de-duplicated: a
repeated identifier yields one result, and an unknown or already-dropped identifier succeeds
rather than failing. Clients call this *after* the Delta transaction that stops referencing the
sequence has committed, so a failure can strand a sequence nothing points at but can never
leave a live column pointing at a dropped one. Deleting the table drops its sequences, so no
per-sequence call is needed then.

Field Name | Data Type | Description | Optional/Required
-|-|-|-
table-id | string (uuid) | Table UUID. Must identify the table named by the path. | required
sequence-ids | array of string | Identifiers to drop. Must be non-empty, at most 64 entries, each at most 128 characters. | required

**200: None of the named sequences exists any more.**

Field Name | Data Type | Description | Optional/Required
-|-|-|-
results | array of object | One entry per unique requested identifier. | required
&nbsp;&nbsp;results[].sequence-id | string | The requested identifier, echoed. | required
&nbsp;&nbsp;results[].existed | boolean | `true` if this call deleted a live sequence, `false` if the identifier was unknown or already dropped. Informational: a client's behaviour does not depend on it. | required

Error Type | HTTP Status | Description
-|-|-
BadRequestException | 400 | Empty `sequence-ids`, more than 64 entries, or the table is not a catalog-managed Delta table.
InvalidParameterValueException | 400 | `table-id` does not identify the table named by the path.
NoSuchTableException | 404 | The table does not exist.

> ***Add the following to the endpoint list returned by [Get Configuration](https://github.com/unitycatalog/unitycatalog/blob/main/spec/protocols/ManagedTablesSpec.md#get-configuration).***

```text
POST   /v1/catalogs/{catalog}/schemas/{schema}/tables/{table}/sequences
POST   /v1/catalogs/{catalog}/schemas/{schema}/tables/{table}/sequences/reserve
DELETE /v1/catalogs/{catalog}/schemas/{schema}/tables/{table}/sequences
```

A server advertises all three or none. A client **must not** write to a table with the
`concurrentIdentityColumns` writer feature against a server that does not advertise them:
absence of the endpoints, not a failed request, is the signal that the feature is unsupported.

## Non-Goals

- **Standalone `SEQUENCE` objects.** A sequence has no name and no existence apart from its
  table. Keeping the identifier opaque and the resource nested leaves room for a named
  sequence resource later without changing this contract.
- **A read endpoint.** Nothing in the write path needs one, and values **must never** be
  derived from an observed frontier. A catalog may add one for diagnosis.
- **Gapless or commit-ordered values; shared or cross-table sequences.**
- **Garbage collection, storage engine, and service topology.** Policy for dropped records is
  a server concern, bounded only by [Lifecycle](#lifecycle). The contract is stated in
  observable request behaviour only.
- **Explicitly inserted values.** With `delta.identity.allowExplicitInsert`, a user-supplied
  value is not drawn from the sequence and the catalog does not account for it, exactly as it
  does not advance the high-water mark today.
- **Reader support.** Identity values are materialized in the data files.
