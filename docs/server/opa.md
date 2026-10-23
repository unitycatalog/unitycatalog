# Authorization with Open Policy Agent

Unity Catalog can delegate permission decisions to [Open Policy Agent (OPA)](https://www.openpolicyagent.org/).
UC still authenticates each caller, resolves resource names and IDs, and enforces its storage checks.
OPA decides whether that caller may perform the requested operation.

## Configure the server

Keep your existing [authentication configuration](auth.md) and add:

```properties
server.authorization=enable
server.authorization.provider=opa
server.authorization.opa.policy-url=http://localhost:8181/v1/data/unitycatalog/allow
server.authorization.opa.response-timeout=PT2S
```

The policy URL is the full OPA Data API endpoint for a Boolean decision. Use HTTPS when the policy
engine runs across an untrusted network. If OPA requires a bearer token, set
`server.authorization.opa.bearer-token`. Do not put credentials in the URL. Restart UC after changing
this configuration.

| Property | Default | Meaning |
| --- | --- | --- |
| `server.authorization.provider` | `native` | Use UC grants (`native`) or an external OPA policy (`opa`). |
| `server.authorization.opa.policy-url` | Unset | Required in OPA mode; must be an HTTP or HTTPS URL. |
| `server.authorization.opa.response-timeout` | `PT2S` | Armeria response timeout, starting when the policy request begins. Must be at least one millisecond. |
| `server.authorization.opa.bearer-token` | Unset | Optional token sent in the `Authorization: Bearer` header. |

UC allows access only when OPA returns HTTP 200 with `{"result": true}`. A Boolean `false` denies
access. A timeout, transport error, oversized response, or invalid decision fails the UC request.
UC does not retry the decision or fall back to native grants. Policy responses are limited to 64 KiB.

## Decision input

UC sends a POST with an `input` object. For example, creating an external table can send:

```json
{
  "input": {
    "principal": {
      "id": "10000000-0000-0000-0000-000000000001",
      "email": "creator@example.com"
    },
    "action": "CREATE_TABLE",
    "resources": {
      "catalog": "20000000-0000-0000-0000-000000000002",
      "schema": "30000000-0000-0000-0000-000000000003",
      "external_location": "40000000-0000-0000-0000-000000000004"
    },
    "resource_keys": {
      "catalog": "analytics",
      "schema": "sales",
      "external_location": "s3://example-data/sales/orders"
    },
    "context": {
      "name": "orders",
      "table_type": "EXTERNAL",
      "no_overlap_with_data_securable": true
    }
  }
}
```

- `principal` comes from the authenticated UC user. OPA does not replace token verification or user
  provisioning. Groups and native grants are not included.
- `action` identifies the operation, such as `CREATE_TABLE`, `GET_TABLE`, or
  `GENERATE_PATH_CREDENTIALS`. UC, Delta, and Iceberg APIs share actions where the operation is the
  same. Request variants such as read/write scope appear in `context`.
- `resources` contains server-resolved UC UUIDs, including applicable parents and related external
  locations or storage credentials. A new object has no ID yet; its parents are supplied instead.
- `resource_keys` contains the names, IDs, or paths extracted from the request. The available keys
  depend on the endpoint. Prefer resolved UUIDs for rules that must work across API protocols and
  equivalent names or paths.
- `context` contains selected request values, such as a new object's name, table type, model version,
  credential operation, or an Iceberg staged-create flag. Optional fields can be `null`. UC does not
  send entire request bodies, storage secrets, or authentication tokens to the policy endpoint.

For an operation using an external location, the same decision input contains both its catalog or
schema and the related external-location ID. Your policy must check both when both matter. UC does
not require OPA to reproduce `USE_CATALOG`, `USE_SCHEMA`, or any other native grant rule.

## Example policy

This example permits one reader to read tables in a particular catalog. It also permits a creator
to create external tables in one schema using one external location. Replace the example IDs with
your UC IDs and extend the policy for the other operations your users need, including administration.

```rego
package unitycatalog

import rego.v1

default allow := false

allow if {
    input.principal.email == "reader@example.com"
    input.action in {"GET_TABLE", "LIST_TABLES"}
    input.resources.catalog == "20000000-0000-0000-0000-000000000002"
}

allow if {
    input.principal.email == "reader@example.com"
    input.action == "GENERATE_TABLE_CREDENTIALS"
    input.context.operation == "READ"
    input.resources.catalog == "20000000-0000-0000-0000-000000000002"
}

allow if {
    input.principal.email == "creator@example.com"
    input.action == "CREATE_TABLE"
    input.context.table_type == "EXTERNAL"
    input.resources.schema == "30000000-0000-0000-0000-000000000003"
    input.resources.external_location == "40000000-0000-0000-0000-000000000004"
}
```

UC checks storage preconditions before calling OPA. These include rejecting overlapping data paths,
invalid credential operations, and invalid managed-storage roots. An OPA allow cannot override them.
The repositories also continue to enforce their normal validation and data consistency rules.

## Lists and storage credentials

List APIs that filter visible resources ask OPA once per candidate item, using the `LIST_*` action
and that item's UUID, name, and parent IDs. A false decision removes the item. A policy failure fails
the request. This can require several policy calls for one page; no batching or decision cache is
used. An empty page has no candidate decisions. Other list APIs, such as listing users or model
versions, make one decision for the whole operation.

Temporary credential requests include their requested operation. Table credentials use `READ` or
`READ_WRITE`; path credentials use `PATH_READ`, `PATH_READ_WRITE`, or `PATH_CREATE_TABLE`. A path is
resolved locally to its owning table, volume, model, or external location before OPA is called.

Iceberg table loading and credential renewal first require `GET_TABLE`. UC then asks for
`GENERATE_TABLE_CREDENTIALS` with `operation=READ`. For native Iceberg tables, UC also asks whether
`READ_WRITE` is allowed. A false write decision keeps the credentials read-only. Read must be allowed
before either scope can be returned. UniForm tables always receive read-only credentials.

## Native permissions and switching providers

OPA mode replaces native permission decisions, including decisions for administrators and owners.
Write and test the required administrative policy before enabling it. Authentication-only endpoints,
such as the current-user endpoint, continue to use local authentication.

UC permission read and update APIs return an unsupported-operation error in OPA mode. Manage access
in OPA. Existing grants remain stored, and UC continues its ownership and hierarchy bookkeeping, but
those grants do not affect OPA decisions. UC does not copy grants or owners into OPA automatically.

To return to native authorization, set `server.authorization.provider=native` and restart UC. Review
the stored grants first: they become effective again, including ownership recorded while OPA was
enabled.
