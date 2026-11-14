# Sharing data with OpenSharing

Unity Catalog can embed the [OpenSharing](https://github.com/OpenSharing-IO/OpenSharing) server
library, so one Unity Catalog server can share the Delta tables it governs. Unity Catalog owns
authentication, authorization, table metadata and storage credentials. OpenSharing owns shares and
what is in them.

Embedded OpenSharing is **off by default**. To turn it on, set the following in
`etc/conf/server.properties`:

```properties
server.authorization=enable
server.opensharing.enabled=true
server.opensharing.public-url=https://sharing.example.com
```

It needs authorization, because each share is owned by the Unity Catalog user who creates it. The
required `server.opensharing.public-url` value is the address recipients reach this server at;
OpenSharing uses it in activation links. The code is part of the server: no extra module or classpath
is needed.

## Provider API

The provider API manages shares. It is OpenSharing's API, hosted by Unity Catalog under the Unity
Catalog base path:

```text
/api/2.1/unity-catalog/opensharing/provider
```

Unity Catalog authenticates the caller like it does for its own API, with the same access tokens,
and decides what the caller may do with Unity Catalog privileges and ownership (see
[Privileges](#privileges)). The requests and responses, and the error bodies, are OpenSharing's: the
wire format follows the version of OpenSharing that the server embeds, and
[OpenSharing's specification](https://github.com/OpenSharing-IO/OpenSharing/tree/main/spec)
describes it. Like the Iceberg REST catalog, it is not part of the Unity Catalog API in
`api/all.yaml`.

| Method and path | What it does | Needs |
| --- | --- | --- |
| `POST /shares` | Creates a share, owned by the caller | `CREATE SHARE` on the metastore |
| `GET /shares` | Lists the shares the caller may read | `USE SHARE` on the metastore, or owning the share |
| `GET /shares/{share}` | Gets a share, and the tables in it with `include_shared_data=true` | `USE SHARE` on the metastore, or owning the share |
| `PATCH /shares/{share}` | Adds tables to the share, removes them, and changes its other fields | Owning the share |
| `DELETE /shares/{share}` | Deletes the share | Owning the share |

Share names are not case sensitive.

## Privileges

| Privilege | On | Allows |
| --- | --- | --- |
| `CREATE SHARE` | Metastore | Creating shares |
| `USE SHARE` | Metastore | Listing and reading every share, not only the caller's own |

The metastore owner holds both. Grant them like any other privilege, with the permissions API or
`bin/uc permission create`.

Putting a table in a share takes the access Unity Catalog asks for before it vends the table's read
credentials: `USE CATALOG` on its catalog, `USE SCHEMA` on its schema and `SELECT` on the table, or
owning them. Only Delta tables with a storage location can be shared.

Ownership can't be transferred. When the owner of a share is deleted, nobody can change or delete
the share.

## Storage credentials

OpenSharing reads shared tables through Unity Catalog's credential vendor, so the cloud storage a
table lives on is configured as it is for the rest of the server. The connector asks the vendor for
read-only credentials for the table's own storage location, and refuses a request for any other
operation. OpenSharing may suggest how long credentials should last, but Unity Catalog doesn't vary
it: they last the vendor's default, and carry their expiration.

## Database

OpenSharing's tables are part of Unity Catalog's schema, in the database
`etc/conf/hibernate.properties` configures. Their names start with `os_`: `os_shares`,
`os_share_properties`, `os_shared_data_objects`, `os_share_permissions`, `os_recipients` and
`os_recipient_tokens`. The schema steps create them whether or not OpenSharing is enabled, and a
release that changes them ships a step like any other (see
[Upgrading the database schema](upgrades.md)).
