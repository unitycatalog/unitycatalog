# Avro

This page explains how to work with Avro tables in Unity Catalog.

## Create Avro table

Use the `bin/uc table create` command with the `--format AVRO` option to create a new Avro table in Unity Catalog.

Run the following command with the correct `storage_location` to create a new Avro table with 2 colummns: `some_numbers` and `some_letters`:

```sh
bin/uc table create \
  --format AVRO \
  --full_name unity.default.test \
  --columns "some_numbers INT, some_letters STRING" \
  --storage_location $DIRECTORY$ \

```

After you run the `table create` command, your output should look something like this:

```
Table created successfully at: file:///Users/avriiil/tmp/tables
┌────────────────────┬──────────────────────────────────────────────────────────────────────────────────────────┐
│        KEY         │                                          VALUE                                           │
├────────────────────┼──────────────────────────────────────────────────────────────────────────────────────────┤
│NAME                │test-avro                                                                                 │
├────────────────────┼──────────────────────────────────────────────────────────────────────────────────────────┤
│CATALOG_NAME        │unity                                                                                     │
├────────────────────┼──────────────────────────────────────────────────────────────────────────────────────────┤
│SCHEMA_NAME         │default                                                                                   │
├────────────────────┼──────────────────────────────────────────────────────────────────────────────────────────┤
│TABLE_TYPE          │EXTERNAL                                                                                  │
├────────────────────┼──────────────────────────────────────────────────────────────────────────────────────────┤
│DATA_SOURCE_FORMAT  │AVRO                                                                                      │
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
│STORAGE_LOCATION    │file:///Users/avriiil/tmp/tables/                                                        │
├────────────────────┼──────────────────────────────────────────────────────────────────────────────────────────┤
│COMMENT             │null                                                                                      │
├────────────────────┼──────────────────────────────────────────────────────────────────────────────────────────┤
│PROPERTIES          │{}                                                                                        │
├────────────────────┼──────────────────────────────────────────────────────────────────────────────────────────┤
│CREATED_AT          │1725527979595                                                                             │
├────────────────────┼──────────────────────────────────────────────────────────────────────────────────────────┤
│UPDATED_AT          │null                                                                                      │
├────────────────────┼──────────────────────────────────────────────────────────────────────────────────────────┤
│TABLE_ID            │c0014881-7a10-4ed9-b025-016fdac4fda9                                                      │
└────────────────────┴──────────────────────────────────────────────────────────────────────────────────────────┘

```
