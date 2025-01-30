# Table Formats

This page explains how you can work with various table storage format tables in your Unity Catalog. It will also
explain the advantages and drawbacks of working with these storage formats including Parquet, ORC, JSON, CSV, Avro,
and TEXT.

## Set up Unity Catalog

To follow along, make sure you have a local instance of Unity Catalog running by launching the following command from
a terminal window:

```sh
bin/start-uc-server
```

This local UC server will come with some sample data pre-loaded. To list all of the tables in your local
Unity Catalog, use:

```sh
bin/uc table list --catalog unity --schema default
```

```console
┌─────────────────┬──────────────┬─────┬────────────────────────────────────┐
│      NAME       │ CATALOG_NAME │ ... │              TABLE_ID              │
├─────────────────┼──────────────┼─────┼────────────────────────────────────┤
│marksheet        │unity         │ ... │c389adfa-5c8f-497b-8f70-26c2cca4976d│
├─────────────────┼──────────────┼─────┼────────────────────────────────────┤
│marksheet_uniform│unity         │ ... │9a73eb46-adf0-4457-9bd8-9ab491865e0d│
├─────────────────┼──────────────┼─────┼────────────────────────────────────┤
│numbers          │unity         │ ... │32025924-be53-4d67-ac39-501a86046c01│
├─────────────────┼──────────────┼─────┼────────────────────────────────────┤
│user_countries   │unity         │ ... │26ed93b5-9a18-4726-8ae8-c89dfcfea069│
└─────────────────┴──────────────┴─────┴────────────────────────────────────┘
```

As you can see, there are currently four (4) Delta tables pre-loaded in this catalog.  

## Create a table using a different storage format

To create a table storage format table such as Parquet, ORC, Avro, CSV, JSON, or TEXT, use the
`bin/uc table create ...` command with the `--format` flag.

The following creates a new table in the `path/to/storage` LOCATION two colummns: `some_numbers` and `some_letters`

=== "Parquet"

    ```sh
    bin/uc table create --full_name unity.default.test \
       --columns "some_numbers INT, some_letters STRING" \
       --storage_location /path/to/storage \
       --format PARQUET
    ```

=== "JSON"

    ```sh
    bin/uc table create --full_name unity.default.test \
       --columns "some_numbers INT, some_letters STRING" \
       --storage_location /path/to/storage \
       --format JSON
    ```
    
=== "CSV"

    ```sh
    bin/uc table create --full_name unity.default.test \
       --columns "some_numbers INT, some_letters STRING" \
       --storage_location /path/to/storage \
       --format CSV
    ```

=== "ORC"

    ```sh
    bin/uc table create --full_name unity.default.test \
       --columns "some_numbers INT, some_letters STRING" \
       --storage_location /path/to/storage \
       --format ORC
    ```

=== "Avro"

    ```sh
    bin/uc table create --full_name unity.default.test \
       --columns "some_numbers INT, some_letters STRING" \
       --storage_location /path/to/storage \
       --format AVRO
    ```
      
=== "Text"

    ```sh
    bin/uc table create --full_name unity.default.test \
       --columns "some_numbers INT, some_letters STRING" \
       --storage_location /path/to/storage \
       --format TEXT
    ```

!!! Note "Setting your /path/to/storage"
    You will need to manually set the `/path/to/storage` to the correct storage location. If you don't know where
    Unity Catalog is storing your files, then take a look at the metadata of an existing table using
    `bin/uc table get --full_name <catalog.schema.table>` to see its storage location.

After you run the `table create` command, your output should look similar to the following *abridged* output of a JSON
table:

```console
┌───────────────────┬───────────────────────────────────────────────┐
│        KEY        │                  VALUE                        │
├───────────────────┼───────────────────────────────────────────────┤
│NAME               │test_json                                      │
├───────────────────┼───────────────────────────────────────────────┤
│CATALOG_NAME       │unity                                          │
├───────────────────┼───────────────────────────────────────────────┤
│SCHEMA_NAME        │default                                        │
├───────────────────┼───────────────────────────────────────────────┤
│TABLE_TYPE         │EXTERNAL                                       │
├───────────────────┼───────────────────────────────────────────────┤
│DATA_SOURCE_FORMAT │JSON                                           │
├───────────────────┼───────────────────────────────────────────────┤
│COLUMNS            │{"name":"some_numbers","type_text":"int","type\│
│                   │"nullable\":true,\"metadata\":{}}","type_name"t│
│                   │ype":null,"position":0,"comment":null,"nullabl │
│                   │{"name":"some_letters","type_text":"string","t"│
│                   │,\"nullable\":true,\"metadata\":{}}","type_namr│
│                   │val_type":null,"position":1,"comment":null,"nu │
├───────────────────┼───────────────────────────────────────────────┤
│STORAGE_LOCATION   │file:///tmp/tables/test_json/                  │
├───────────────────┼───────────────────────────────────────────────┤
│...                │...                                            │
├───────────────────┼───────────────────────────────────────────────┤
│TABLE_ID           │9d73eb9c-8d40-46f2-a2c0-4e5d2a3e0611           │
└───────────────────┴───────────────────────────────────────────────┘
```

This command has multiple parameters:

| Parameter | Description |
| --------- | ----------- |
| `full_name` | The full name of the table, which is a concatenation of the catalog name, schema name, and table name separated by dots (e.g., catalog_name.schema_name.table_name). |
| `columns` |  The columns of the table in SQL-like format "column_name column_data_type". Supported data types include `BOOLEAN, BYTE, SHORT, INT, LONG, FLOAT, DOUBLE, DATE, TIMESTAMP, TIMESTAMP_NTZ, STRING, BINARY, DECIMAL`. Separate multiple columns with a comma (e.g., "`id INT, name STRING`") |
| `format` | [Optional] The format of the data source. Supported values are DELTA, PARQUET, ORC, JSON, CSV, AVRO, and TEXT. If not specified the default format is DELTA. |
| `storage_location` | The storage location associated with the table. It is a mandatory field for EXTERNAL tables. |
| `properties` |  [Optional] The properties of the entity in JSON format (e.g., '{"key1": "value1", "key2": "value2"}'). Make sure to either escape the double quotes(\") inside the properties string or just use single quotes('') around the same. |

## Challenges using table storage formats

While popular, each table storage format (e.g., Parquet, ORC, JSON, CSV, Avro, TEXT, etc.) has their own set of
distinct advantages. But the challenges when working with these formats include:

- No ACID transactions for these data lakes meaning it's easier to accidentally corrupt your data
- It is not easy to delete rows from these tables
- These table storage foramts do not offer DML transactions
- They lack advanced features from schema evolution and enforcement to deletion vectors to change data feed
- Slow file listing overhead when working with cloud object stores such as AWS S3, Azure ADLSgen2, and
    Google Cloud Storage
- Potentialy expensive footer reads to gather statistics for file skipping

Open table formats like Apache Iceberg and Delta Lake are specifically designed to overcome these challenges. Storing
your data in a lakehouse format is almost always more advantageous than storing it in traditional table storage
formats.
