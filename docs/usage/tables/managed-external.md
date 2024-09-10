# Managed vs External Tables

Unity Catalog supports both managed and external tables.

A **managed table** is a table for which Unity Catalog manages both the metadata and the data lifecycle itself (creation, storage, deletion). Data is managed at a pre-configured default storage location.

An **external table** is a table for which Unity Catalog manages _only_ the metadata and not the lifecycle of the underlying data itself. The user is responsible for data creation, storage and deletion.

When you drop a managed table from Unity Catalog, the underlying data is also removed.

<< image >>

## How to Create a Managed Table

<< code >>

## Specifying the Managed Storage Location

By default, managed tables are stored in Unity Catalog's root directory.

You can also specify locations for managed tables at the catalog or schema level. You might want to do this for stronger isolation between schemas or catalogs, e.g. between "prod" and "dev".

<< how / code >>

Unity Catalog will search for a user-defined storage location in the following order:

- at schema level
- at catalog level
- at metastore root directory, configured during setup

## How to Create an External Table

Use the `uc/bin table create` command with a `storage_location` to create an external table.

For example:

```
bin/uc table create --full_name unity.default.test_ext \
  --storage_location ~/tmp \
  --columns "id INT, name STRING" \
```

You will see `TABLE_TYPE EXTERNAL` in the returned metadata:

```
┌────────────────────┬──────────────────────────────────────────────────────────────────────────────────────────┐
│        KEY         │                                          VALUE                                           │
├────────────────────┼──────────────────────────────────────────────────────────────────────────────────────────┤
│NAME                │test_ext                                                                                  │
├────────────────────┼──────────────────────────────────────────────────────────────────────────────────────────┤
│CATALOG_NAME        │unity                                                                                     │
├────────────────────┼──────────────────────────────────────────────────────────────────────────────────────────┤
│SCHEMA_NAME         │default                                                                                   │
├────────────────────┼──────────────────────────────────────────────────────────────────────────────────────────┤
│TABLE_TYPE          │EXTERNAL                                                                                  │
├────────────────────┼──────────────────────────────────────────────────────────────────────────────────────────┤
│DATA_SOURCE_FORMAT  │DELTA                                                                                     │
├────────────────────┼──────────────────────────────────────────────────────────────────────────────────────────┤
│COLUMNS             │{"name":"id","type_text":"int","type_json":"{\"name\":\"id\",\"type\":\"integer\",\"nullab│
│                    │le\":true,\"metadata\":{}}","type_name":"INT","type_precision":0,"type_scale":0,"type_inte│
│                    │rval_type":null,"position":0,"comment":null,"nullable":true,"partition_index":null}       │
│                    │{"name":"name","type_text":"string","type_json":"{\"name\":\"name\",\"type\":\"string\",\"│
│                    │nullable\":true,\"metadata\":{}}","type_name":"STRING","type_precision":0,"type_scale":0,"│
│                    │type_interval_type":null,"position":1,"comment":null,"nullable":true,"partition_index":nul│
│                    │l}                                                                                        │
├────────────────────┼──────────────────────────────────────────────────────────────────────────────────────────┤
│STORAGE_LOCATION    │file:///Users/rpelgrim/tmp/                                                               │
├────────────────────┼──────────────────────────────────────────────────────────────────────────────────────────┤
│COMMENT             │null                                                                                      │
├────────────────────┼──────────────────────────────────────────────────────────────────────────────────────────┤
│PROPERTIES          │{}                                                                                        │
├────────────────────┼──────────────────────────────────────────────────────────────────────────────────────────┤
│CREATED_AT          │1725876982747                                                                             │
├────────────────────┼──────────────────────────────────────────────────────────────────────────────────────────┤
│UPDATED_AT          │null                                                                                      │
├────────────────────┼──────────────────────────────────────────────────────────────────────────────────────────┤
│TABLE_ID            │ad67416b-b3e1-412f-9ab3-bf6926a8ce49                                                      │
└────────────────────┴──────────────────────────────────────────────────────────────────────────────────────────┘
```

## When to use which table
