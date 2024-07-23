# Parquet

This page explains how you can work with Parquet tables in your Unity Catalog. It will also explain the advantages and drawbacks of working with Parquet.

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
┌────────────┬────────────────┬────────────────┬────────────────┬────────────────┬────────────────┬────────────────┬────────────────┬────────────────┬────────────────┬────────────────┬────────────────┐
│    NAME    │  CATALOG_NAME  │  SCHEMA_NAME   │   TABLE_TYPE   │DATA_SOURCE_FORM│    COLUMNS     │STORAGE_LOCATION│    COMMENT     │   PROPERTIES   │   CREATED_AT   │   UPDATED_AT   │    TABLE_ID    │
│            │                │                │                │       AT       │                │                │                │                │                │                │                │
├────────────┼────────────────┼────────────────┼────────────────┼────────────────┼────────────────┼────────────────┼────────────────┼────────────────┼────────────────┼────────────────┼────────────────┤
│user_coun...│unity           │default         │EXTERNAL        │DELTA           │[{"name":"fir...│file:///Users...│Partitioned t...│{}              │1721238005622   │1721238005622   │26ed93b5-9a18...│
├────────────┼────────────────┼────────────────┼────────────────┼────────────────┼────────────────┼────────────────┼────────────────┼────────────────┼────────────────┼────────────────┼────────────────┤
│numbers     │unity           │default         │EXTERNAL        │DELTA           │[{"name":"as_...│file:///Users...│External table  │{"key1":"valu...│1721238005617   │1721238005617   │32025924-be53...│
├────────────┼────────────────┼────────────────┼────────────────┼────────────────┼────────────────┼────────────────┼────────────────┼────────────────┼────────────────┼────────────────┼────────────────┤
│marksheet...│unity           │default         │EXTERNAL        │DELTA           │[{"name":"id"...│file:///tmp/m...│Uniform table   │{"key1":"valu...│1721238005611   │1721238005611   │9a73eb46-adf0...│
├────────────┼────────────────┼────────────────┼────────────────┼────────────────┼────────────────┼────────────────┼────────────────┼────────────────┼────────────────┼────────────────┼────────────────┤
│marksheet   │unity           │default         │MANAGED         │DELTA           │[{"name":"id"...│file:///Users...│Managed table   │{"key1":"valu...│1721238005595   │1721238005595   │c389adfa-5c8f...│
└────────────┴────────────────┴────────────────┴────────────────┴────────────────┴────────────────┴────────────────┴────────────────┴────────────────┴────────────────┴────────────────┴────────────────┘
```

As you can see, there are 4 tables in this catalog. All 4 pre-loaded tables are in the DELTA format.

Let's take a look at how we can create Parquet tables in Unity Catalog.

## Create a Parquet table

Use the `bin/uc table create ...` command with the `--format PARQUET` flag to create a new Parquet table in your Unity Catalog.

This command has multiple parameters:

- `full_name`: The full name of the table, which is a concatenation of the catalog name, schema name, and table name separated by dots (e.g., catalog_name.schema_name.table_name).
- `columns`: The columns of the table in SQL-like format "column_name column_data_type". Supported data types include BOOLEAN, BYTE, SHORT, INT, LONG, FLOAT, DOUBLE, DATE, TIMESTAMP, TIMESTAMP_NTZ, STRING, BINARY, DECIMAL. Separate multiple columns with a comma (e.g., "id INT, name STRING").
- `format`: [Optional] The format of the data source. Supported values are DELTA, PARQUET, ORC, JSON, CSV, AVRO, and TEXT. If not specified the default format is DELTA.
- `storage_location`: The storage location associated with the table. It is a mandatory field for EXTERNAL tables.
- `properties`: [Optional] The properties of the entity in JSON format (e.g., '{"key1": "value1", "key2": "value2"}'). Make sure to either escape the double quotes(\") inside the properties string or just use single quotes('') around the same.

Run the command below with the correct `path/to/storage` to create a new PARQUET table with 2 colummns: `some_numbers` and `some_letters`.
You can get the correct storage location using a `bin/uc table get ...` call to fetch some table metadata.

```sh
bin/uc table create --full_name unity.default.test --columns "some_numbers INT, some_letters STRING" --storage_location file:///Users/avriiil/Documents/git/my-forks/unitycatalog/etc/data/external/unity/default/tables/test
```

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

## Pros and Cons of Using Parquet

Parquet is a popular file format for tabular data. It is an efficient data storage format and offers great features like predicate pushdown, column pruning and partition skipping.

Parquet also has some important drawbacks you should be aware of compared to open table formats like Delta Lake and Apache Iceberg.

Here are some of the challenges of working with Parquet tables:

- No ACID transactions for Parquet data lakes means it's easier to accidentally corrupt your data
- It is not easy to delete rows from Parquet tables
- No DML transactions
- There is no change data feed
- Slow file listing overhead
- Expensive footer reads to gather statistics for file skipping
- There is no way to rename, reorder, or drop columns without rewriting the whole table

Open table formats like Delta Lake and Apache Iceberg were specifically designed to overcome these limitations. Storing your data in one of those formats is almost always more advantageous than storing it in Parquet files.

You can read a detailed comparison of [Delta Lake vs Parquet](https://delta.io/blog/delta-lake-vs-parquet-comparison/) for more information.

## Using Parquet Tables with Spark

ref: https://books.japila.pl/unity-catalog-internals/spark-integration/#spark_catalog

1. Build Spark integration module: `build/sbt clean package publishLocal spark/publishLocal`
2. Launch a Spark shell `bin/spark-shell` from Spark directory or `pyspark` in venv with pyspark installed from pip

### PySpark

3. spark.catalog.listCatalogs()
   output: `[CatalogMetadata(name='spark_catalog', description=None)]`
4. spark.catalog.currentCatalog()
   `'spark_catalog'`
5. create `spark_catalog` in Unity Catalog
   ./bin/uc catalog create --name spark_catalog
   ./bin/uc schema create --catalog spark_catalog --name default
6. create a table
   ./bin/uc table create --full_name spark_catalog.default.uc_demo --columns 'id INT' --storage_location /tmp/uc_demo --format delta
7. !! Spark doesn't recognize the table we've created in UC
   ?? maybe version issue, spark==3.5.0 on my system and 3.5.1 in the docs
   > > actually probably because i haven't installed the right jars etc
   > > mmm installing the right conf + jars broke the `listCatalogs()` call
   > > maybe just Scala and not pyspark yet
