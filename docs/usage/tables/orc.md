# ORC

This page explains how you can work with ORC tables in your Unity Catalog.

Let's take a look at how we can create ORC tables in Unity Catalog.

## Create ORC table

Use `bin/uc table create` command with the `--format ORC` option to create a new ORC table in Unity Catalog.

Use the following command to create an external ORC tables (at `path/to/storage` location) with 2 colummns: `some_numbers` and `some_letters`.

```sh
bin/uc table create \
  --full_name unity.default.test \
  --columns "some_numbers INT, some_letters STRING" \
  --storage_location $DIRECTORY$ \
  --format ORC
```

After you run the `table create` command, your output should look something like this:

```
┌────────────────────┬──────────────────────────────────────────────────────────────────────────────────────────┐
│        KEY         │                                          VALUE                                           │
├────────────────────┼──────────────────────────────────────────────────────────────────────────────────────────┤
│NAME                │test                                                                                      │
├────────────────────┼──────────────────────────────────────────────────────────────────────────────────────────┤
│CATALOG_NAME        │unity                                                                                     │
├────────────────────┼──────────────────────────────────────────────────────────────────────────────────────────┤
│SCHEMA_NAME         │default                                                                                   │
├────────────────────┼──────────────────────────────────────────────────────────────────────────────────────────┤
│TABLE_TYPE          │EXTERNAL                                                                                  │
├────────────────────┼──────────────────────────────────────────────────────────────────────────────────────────┤
│DATA_SOURCE_FORMAT  │ORC                                                                                       │
├────────────────────┼──────────────────────────────────────────────────────────────────────────────────────────┤
│COLUMNS             │{"name":"some_numbers","type_text":"int","type_json":"{\"name\":\"some_numbers\",\"type\":│
│                    │\"integer\",\"nullable\":true,\"metadata\":{}}","type_name":"INT","type_precision":0,"type│
│                    │_scale":0,"type_interval_type":null,"position":0,"comment":null,"nullable":true,"partition│
│                    │_index":null}                                                                             │
│                    │{"name":"some_letters","type_text":"string","type_json":"{\"name\":\"some_letters\",\"type│
│                    │\":\"string\",\"nullable\":true,\"metadata\":{}}","type_name":"STRING","type_precision":0,│
│                    │"type_scale":0,"type_interval_type":null,"position":1,"comment":null,"nullable":true,"part│
│                    │ition_index":null}                                                                        │
├────────────────────┼──────────────────────────────────────────────────────────────────────────────────────────┤
│STORAGE_LOCATION    │file:///Users/avriiil/Documents/git/my-forks/unitycatalog/etc/data/external/unity/default│
│                    │/tables/                                                                                  │
├────────────────────┼──────────────────────────────────────────────────────────────────────────────────────────┤
│COMMENT             │null                                                                                      │
├────────────────────┼──────────────────────────────────────────────────────────────────────────────────────────┤
│PROPERTIES          │{}                                                                                        │
├────────────────────┼──────────────────────────────────────────────────────────────────────────────────────────┤
│CREATED_AT          │1725270803001                                                                             │
├────────────────────┼──────────────────────────────────────────────────────────────────────────────────────────┤
│UPDATED_AT          │null                                                                                      │
├────────────────────┼──────────────────────────────────────────────────────────────────────────────────────────┤
│TABLE_ID            │f4e88e98-1289-4573-999d-c724b010b601                                                      │
└────────────────────┴──────────────────────────────────────────────────────────────────────────────────────────┘
```
