# Parquet

This page explains how you can work with Parquet tables in your Unity Catalog. It will also explain the advantages and drawbacks of working with Parquet.

## Set up Unity Catalog

To follow along, make sure you have a local instance of Unity Catalog running by launching the following command from a terminal window:

```sh
bin/start-uc-server
```

This local UC server will come with some sample data pre-loaded.

You can list all of the tables in your Unity Catalog using:

```sh
bin/uc table list --catalog unity --schema default
```

Your output should look something like this:

```
┌─────────────────┬───────┬───────┬───────┬───────┬───────┬───────┬───────┬───────┬───────┬───────┬────────────────────────────────────┐
│      NAME       │CATALOG│SCHEMA_│TABLE_T│DATA_SO│COLUMNS│STORAGE│COMMENT│PROPERT│CREATED│UPDATED│              TABLE_ID              │
│                 │ _NAME │ NAME  │  YPE  │URCE_FO│       │_LOCATI│       │  IES  │  _AT  │  _AT  │                                    │
│                 │       │       │       │ RMAT  │       │  ON   │       │       │       │       │                                    │
├─────────────────┼───────┼───────┼───────┼───────┼───────┼───────┼───────┼───────┼───────┼───────┼────────────────────────────────────┤
│marksheet        │unity  │default│MANAGED│DELTA  │[{"n...│file...│Mana...│{"ke...│1721...│1721...│c389adfa-5c8f-497b-8f70-26c2cca4976d│
├─────────────────┼───────┼───────┼───────┼───────┼───────┼───────┼───────┼───────┼───────┼───────┼────────────────────────────────────┤
│marksheet_uniform│unity  │default│EXTE...│DELTA  │[{"n...│file...│Unif...│{"ke...│1721...│1721...│9a73eb46-adf0-4457-9bd8-9ab491865e0d│
├─────────────────┼───────┼───────┼───────┼───────┼───────┼───────┼───────┼───────┼───────┼───────┼────────────────────────────────────┤
│numbers          │unity  │default│EXTE...│DELTA  │[{"n...│file...│Exte...│{"ke...│1721...│1721...│32025924-be53-4d67-ac39-501a86046c01│
├─────────────────┼───────┼───────┼───────┼───────┼───────┼───────┼───────┼───────┼───────┼───────┼────────────────────────────────────┤
│user_countries   │unity  │default│EXTE...│DELTA  │[{"n...│file...│Part...│{}     │1721...│1721...│26ed93b5-9a18-4726-8ae8-c89dfcfea069│
└─────────────────┴───────┴───────┴───────┴───────┴───────┴───────┴───────┴───────┴───────┴───────┴────────────────────────────────────┘
```

As you can see, there are currently four (4) Delta tables pre-loaded in this catalog.

Let's take a look at how we can create Parquet tables in Unity Catalog.

## Create a Parquet table

Use the `bin/uc table create ...` command with the `--format PARQUET` flag to create a new Parquet table in your Unity Catalog.

Run the command below with the correct `path/to/storage` to create a new PARQUET table with 2 colummns: `some_numbers` and `some_letters`:

```sh
bin/uc table create --full_name unity.default.test --columns "some_numbers INT, some_letters STRING" --storage_location $DIRECTORY$ --format PARQUET
```

Note that you will need to manually set the $DIRECTORY$ variable to the correct storage location.

This should output something like:

```
┌────────────────────┬────────────────────────────────────────────────────────────────────────────────────────────────────┐
│        KEY         │                                               VALUE                                                │
├────────────────────┼────────────────────────────────────────────────────────────────────────────────────────────────────┤
│NAME                │test                                                                                                │
├────────────────────┼────────────────────────────────────────────────────────────────────────────────────────────────────┤
│CATALOG_NAME        │unity                                                                                               │
├────────────────────┼────────────────────────────────────────────────────────────────────────────────────────────────────┤
│SCHEMA_NAME         │default                                                                                             │
├────────────────────┼────────────────────────────────────────────────────────────────────────────────────────────────────┤
│TABLE_TYPE          │EXTERNAL                                                                                            │
├────────────────────┼────────────────────────────────────────────────────────────────────────────────────────────────────┤
│DATA_SOURCE_FORMAT  │PARQUET                                                                                             │
├────────────────────┼────────────────────────────────────────────────────────────────────────────────────────────────────┤
│COLUMNS             │{"name":"some_numbers","type_text":"int","type_json":"{\"name\":\"some_numbers\",\"type\":\"integer\│
│                    │",\"nullable\":true,\"metadata\":{}}","type_name":"INT","type_precision":0,"type_scale":0,"type_inte│
│                    │rval_type":null,"position":0,"comment":null,"nullable":true,"partition_index":null}                 │
│                    │{"name":"some_letters","type_text":"string","type_json":"{\"name\":\"some_letters\",\"type\":\"strin│
│                    │g\",\"nullable\":true,\"metadata\":{}}","type_name":"STRING","type_precision":0,"type_scale":0,"type│
│                    │_interval_type":null,"position":1,"comment":null,"nullable":true,"partition_index":null}            │
│                    │{"name":"some_times","type_text":"timestamp","type_json":"{\"name\":\"some_times\",\"type\":\"timest│
│                    │amp\",\"nullable\":true,\"metadata\":{}}","type_name":"TIMESTAMP","type_precision":0,"type_scale":0,│
│                    │"type_interval_type":null,"position":2,"comment":null,"nullable":true,"partition_index":null}       │
├────────────────────┼────────────────────────────────────────────────────────────────────────────────────────────────────┤
│STORAGE_LOCATION    │file:///Users/avriiil/Documents/git/my-forks/unitycatalog/etc/data/external/unity/default/tables/te│
│                    │st                                                                                                  │
├────────────────────┼────────────────────────────────────────────────────────────────────────────────────────────────────┤
│COMMENT             │null                                                                                                │
├────────────────────┼────────────────────────────────────────────────────────────────────────────────────────────────────┤
│PROPERTIES          │{}                                                                                                  │
├────────────────────┼────────────────────────────────────────────────────────────────────────────────────────────────────┤
│CREATED_AT          │1721727161471                                                                                       │
├────────────────────┼────────────────────────────────────────────────────────────────────────────────────────────────────┤
│UPDATED_AT          │1721727161471                                                                                       │
├────────────────────┼────────────────────────────────────────────────────────────────────────────────────────────────────┤
│TABLE_ID            │e95d4f58-6e6e-4c4c-b45f-d47befb07cde                                                                │
└────────────────────┴────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

This command has multiple parameters:

- `full_name`: The full name of the table, which is a concatenation of the catalog name, schema name, and table name separated by dots (e.g., catalog_name.schema_name.table_name).
- `columns`: The columns of the table in SQL-like format "column_name column_data_type". Supported data types include BOOLEAN, BYTE, SHORT, INT, LONG, FLOAT, DOUBLE, DATE, TIMESTAMP, TIMESTAMP_NTZ, STRING, BINARY, DECIMAL. Separate multiple columns with a comma (e.g., "id INT, name STRING").
- `format`: [Optional] The format of the data source. Supported values are DELTA, PARQUET, ORC, JSON, CSV, AVRO, and TEXT. If not specified the default format is DELTA.
- `storage_location`: The storage location associated with the table. It is a mandatory field for EXTERNAL tables.
- `properties`: [Optional] The properties of the entity in JSON format (e.g., '{"key1": "value1", "key2": "value2"}'). Make sure to either escape the double quotes(\") inside the properties string or just use single quotes('') around the same.

## Pros and Cons of Using Parquet

[Apache Parquet](https://parquet.apache.org/) is a popular file format for tabular data. It is an efficient data storage format and offers great features like predicate pushdown, column pruning and partition skipping.

Parquet also has some important drawbacks you should be aware of compared to open table formats like Delta Lake and Apache Iceberg.

Here are some of the challenges of working with Parquet tables:

- No ACID transactions for Parquet data lakes means it's easier to accidentally corrupt your data
- It is not easy to delete rows from Parquet tables
- Parquet does not offer DML transactions
- There is no change data feed
- Slow file listing overhead
- Expensive footer reads to gather statistics for file skipping

Open table formats like Apache Iceberg and Delta Lake are specifically designed to overcome these limitations. Storing your data in one of those formats is almost always more advantageous than storing it in Parquet files.
