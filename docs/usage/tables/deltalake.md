# Delta Lake

This page explains how you can work with Delta Lake tables in your Unity Catalog.

To follow along, make sure you have a local instance of Unity Catalog running by launching the following command from a terminal window:

```sh
bin/start-uc-server
```

This local UC server will come with some sample data pre-loaded.

You can list all the tables in your Unity Catalog using:

```sh
bin/uc table list --catalog unity --schema default
```

Your output should look something like this:

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

As you can see, there are 4 tables in this catalog. All 4 tables are in the DELTA format.

Let's take a look at how we can work with these Delta Lake tables.

## How to Get Table Metadata

Delta Lake tables have rich metadata. You can use the `bin/uc table get ...` command to take a look at a table's metadata.

Let's take a look at the `numbers` table:

```sh
bin/uc table get --full_name unity.default.numbers
```

Your output should look something like this:

```console
┌────────────────────┬────────────────────────────────────────────────────────────────────────────────────────────────────┐
│        KEY         │                                               VALUE                                                │
├────────────────────┼────────────────────────────────────────────────────────────────────────────────────────────────────┤
│NAME                │numbers                                                                                             │
├────────────────────┼────────────────────────────────────────────────────────────────────────────────────────────────────┤
│CATALOG_NAME        │unity                                                                                               │
├────────────────────┼────────────────────────────────────────────────────────────────────────────────────────────────────┤
│SCHEMA_NAME         │default                                                                                             │
├────────────────────┼────────────────────────────────────────────────────────────────────────────────────────────────────┤
│TABLE_TYPE          │EXTERNAL                                                                                            │
├────────────────────┼────────────────────────────────────────────────────────────────────────────────────────────────────┤
│DATA_SOURCE_FORMAT  │DELTA                                                                                               │
├────────────────────┼────────────────────────────────────────────────────────────────────────────────────────────────────┤
│COLUMNS             │{"name":"as_int","type_text":"int","type_json":"{\"name\":\"as_int\",\"type\":\"integer\",\"nullable│
│                    │\":false,\"metadata\":{}}","type_name":"INT","type_precision":0,"type_scale":0,"type_interval_type":│
│                    │null,"position":0,"comment":"Int                    column","nullable":false,"partition_index":null}│
│                    │{"name":"as_double","type_text":"double","type_json":"{\"name\":\"as_double\",\"type\":\"double\",\"│
│                    │nullable\":false,\"metadata\":{}}","type_name":"DOUBLE","type_precision":0,"type_scale":0,"type_inte│
│                    │rval_type":null,"position":1,"comment":"Double column","nullable":false,"partition_index":null}     │
├────────────────────┼────────────────────────────────────────────────────────────────────────────────────────────────────┤
│STORAGE_LOCATION    │file:///Users/avriiil/Documents/git/my-forks/unitycatalog/etc/data/external/unity/default/tables/nu│
│                    │mbers/                                                                                              │
├────────────────────┼────────────────────────────────────────────────────────────────────────────────────────────────────┤
│COMMENT             │External table                                                                                      │
├────────────────────┼────────────────────────────────────────────────────────────────────────────────────────────────────┤
│PROPERTIES          │{"key1":"value1","key2":"value2"}                                                                   │
├────────────────────┼────────────────────────────────────────────────────────────────────────────────────────────────────┤
│CREATED_AT          │1721238005617                                                                                       │
├────────────────────┼────────────────────────────────────────────────────────────────────────────────────────────────────┤
│UPDATED_AT          │1721238005617                                                                                       │
├────────────────────┼────────────────────────────────────────────────────────────────────────────────────────────────────┤
│TABLE_ID            │32025924-be53-4d67-ac39-501a86046c01                                                                │
└────────────────────┴────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

## How to Read Delta Tables

Use the `bin/uc table read ...` command to read your Delta table. You can optionally limit the number of results with the `max_results` flag.

```sh
bin/uc table read --full_name unity.default.numbers --max_results 3
```

This should output:

```console
┌───────────────────────────────────────┬──────────────────────────────────────┐
│as_int(integer)                        │as_double(double)                     │
├───────────────────────────────────────┼──────────────────────────────────────┤
│564                                    │188.75535598441473                    │
├───────────────────────────────────────┼──────────────────────────────────────┤
│755                                    │883.6105633023361                     │
├───────────────────────────────────────┼──────────────────────────────────────┤
│644                                    │203.4395591086936                     │
└───────────────────────────────────────┴──────────────────────────────────────┘
```

## How to Create a New Delta Table

Use the `bin/uc table create ...` command to create a new Delta table in your Unity Catalog.

This command has multiple parameters:

| Parameter | Description |
| --------- | ----------- |
| `full_name` | The full name of the table, which is a concatenation of the catalog name, schema name, and table name separated by dots (e.g., `catalog_name.schema_name.table_name`). |
| `columns` | The columns of the table in SQL-like format "column_name column_data_type". Supported data types include `BOOLEAN, BYTE, SHORT, INT, LONG, FLOAT, DOUBLE, DATE, TIMESTAMP, TIMESTAMP_NTZ, STRING, BINARY, DECIMAL`. Separate multiple columns with a comma (e.g., "`id INT, name STRING`"). |
| `format`| [Optional] The format of the data source. Supported values are DELTA, PARQUET, ORC, JSON, CSV, AVRO, and TEXT. If not specified the default format is DELTA. |
| `storage_location` | The storage location associated with the table. It is a mandatory field for EXTERNAL tables. |
| `properties` | [Optional] The properties of the entity in JSON format (e.g., `'{"key1": "value1", "key2": "value2"}'`). Make sure to either escape the double quotes(") inside the properties string or just use single quotes(`''`) around the same. |

Run the command below with the correct `path/to/storage` to create a new DELTA table with 2 columns: `some_numbers` and `some_letters`.
You can get the storage location from the `STORAGE_LOCATION` field of your `bin/uc table get ...` call above.

```sh
bin/uc table create --full_name unity.default.test --columns "some_numbers INT, some_letters STRING, some_times TIMESTAMP" --storage_location file:///Users/avriiil/Documents/git/my-forks/unitycatalog/etc/data/external/unity/default/tables/test
```

This should output:

```console
Table created successfully at: file:///Users/avriiil/Documents/git/my-forks/unitycatalog/etc/data/external/unity/default/tables/test

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
│DATA_SOURCE_FORMAT  │DELTA                                                                                               │
├────────────────────┼────────────────────────────────────────────────────────────────────────────────────────────────────┤
│COLUMNS             │{"name":"some_numbers","type_text":"int","type_json":"{\"name\":\"some_numbers\",\"type\":\"integer\│
│                    │",\"nullable\":true,\"metadata\":{}}","type_name":"INT","type_precision":0,"type_scale":0,"type_inte│
│                    │rval_type":null,"position":0,"comment":null,"nullable":true,"partition_index":null}                 │
│                    │{"name":"some_letters","type_text":"string","type_json":"{\"name\":\"some_letters\",\"type\":\"strin│
│                    │g\",\"nullable\":true,\"metadata\":{}}","type_name":"STRING","type_precision":0,"type_scale":0,"type│
│                    │_interval_type":null,"position":1,"comment":null,"nullable":true,"partition_index":null}            │
├────────────────────┼────────────────────────────────────────────────────────────────────────────────────────────────────┤
│STORAGE_LOCATION    │file:///Users/avriiil/Documents/git/my-forks/unitycatalog/etc/data/external/unity/default/tables/te │
│                    │st2                                                                                                 │
├────────────────────┼────────────────────────────────────────────────────────────────────────────────────────────────────┤
│COMMENT             │null                                                                                                │
├────────────────────┼────────────────────────────────────────────────────────────────────────────────────────────────────┤
│PROPERTIES          │{}                                                                                                  │
├────────────────────┼────────────────────────────────────────────────────────────────────────────────────────────────────┤
│CREATED_AT          │1721644623209                                                                                       │
├────────────────────┼────────────────────────────────────────────────────────────────────────────────────────────────────┤
│UPDATED_AT          │1721644623209                                                                                       │
├────────────────────┼────────────────────────────────────────────────────────────────────────────────────────────────────┤
│TABLE_ID            │2e8b23f2-4ff7-4a10-8d23-b8c7bae2bdb0                                                                │
└────────────────────┴────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

## How to Write to Delta Tables

Use the `bin/uc write ...` command to write data to a Delta table.

Let's use our new `test` table as an example. This table should be empty. Let's confirm:

```sh
bin/uc table read --full_name unity.default.test
```

This should output:

```console
┌───────────────────────────────────────┬──────────────────────────────────────┐
│some_numbers(integer)                  │some_letters(string)                  │
└───────────────────────────────────────┴──────────────────────────────────────┘
```

Now use the following command to write some sample data to this table.

```sh
bin/uc table write --full_name <catalog>.<schema>.<table>
```

This is an experimental feature. Currently, this will only write sample data and supports only some primitive data types.

## How to Delete a Delta Table

Use the `bin/uc table delete ...` command to delete a Delta table from your Unity Catalog.

Let's remove the `test` table we've just created:

```sh
 bin/uc table delete --full_name unity.default.test
```

This will remove the Delta table. Let's confirm by listing the tables in our catalog and schema:

```sh
bin/uc table list --catalog unity --schema default
```

This should output:

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

Nicely done!

### Delta Lake with Daft

Here is an example of how you can use Delta Lake’s powerful features from Unity Catalog using [Daft](https://getdaft.io)

You need to have a Unity Catalog server running to connect to.

For testing purposes, you can spin up a local server by running the code below in a terminal:

```sh
bin/start-uc-server
```

Then import Daft and the UnityCatalog abstraction:

```python
import daft
from daft.unity_catalog import UnityCatalog
```

Next, point Daft to your UC server

```python
unity = UnityCatalog(
    endpoint="http://127.0.0.1:8080",
    token="not-used",
)
```

And now you can access your Delta tables stored in Unity Catalog. First by listing the tables:

```python
print(unity.list_tables("unity.default"))
```

This should output:

```console
['unity.default.numbers', 'unity.default.marksheet_uniform', 'unity.default.marksheet']
```

And then accessing the Delta table stored in Unity Catalog:

```python
unity_table = unity.load_table("unity.default.numbers")
df = daft.read_delta_lake(unity_table)
df.show()
```

This should output:

```console
as_int  as_double
564     188.755356
755     883.610563
644     203.439559
75      277.880219
42      403.857969
680     797.691220
821     767.799854
484     344.003740
477     380.678561
131     35.443732
294     209.322436
150     329.197303
539     425.661029
247     477.742227
958     509.371273
```

Take a look at the [Integrations](../../integrations/unity-catalog-duckdb.md) page for more examples of working with Delta tables stored in Unity Catalog.
