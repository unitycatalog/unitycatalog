# Tutorial
Let's take Unity Catalog for spin. In this tutorial, we are going to do the following:
- In one terminal, run the UC server.
- In another terminal, we will explore the contents of the UC server using the UC CLI,
  which is example UC connector provided to demonstrate how to use the UC SDK for various assets,
  as well as provide a convenient way to explore the content of any UC server implementation.

### Prerequisites
You have to ensure that you local environment has the following:
- Clone this repository.
- Ensure the `JAVA_HOME` environment variable your terminal is configured to point to JDK11+.
- Compile the project running `build/sbt package` in the repository root directory.

### Run the UC Server
In a terminal, in the cloned repository root directory, start the UC server.

```
bin/start-uc-server
```

For the rest of the steps, continue in a different terminal.

### List the catalogs and schemas with the CLI
Unity Catalog stores all assets in a 3-level namespaces:
1. catalog
2. schema
3. assets like tables, volumes, functions, etc.

The UC server is pre-populated with a few sample catalogs, schemas, Delta tables, etc.

Let's start by listing the catalogs using the CLI.
```
bin/uc catalog list
```
You should see a catalog named `unity`. Let's see what's in this `unity` catalog (pun intended).
```
bin/uc schema list --catalog unity
```
You should see that there is a schema named in the `default`. To go deeper into the contents of this schema,
you have to list different asset types separately. Let's start with tables.

### Operate on Delta tables with the CLI
Let's list the tables.
```
bin/uc table list --catalog unity --schema default
```
You should see a few tables. Some details are truncated because of the nested nature of the data.
To see all the content, you can add `--format jsonPretty` to any command.

Next, let's get the metadata of one those tables.

```
bin/uc table get --full_name unity.default.numbers
```

You can see that it is a Delta table. Now, specifically for Delta tables, this CLI can
print snippet of the contents of a Delta table (powered by the [Delta Kernel Java](https://delta.io/blog/delta-kernel/) project).
Let's try that.

```
bin/uc table read --full_name unity.default.numbers
```

Let's try creating a new table.

```
bin/uc table create --full_name unity.default.myTable --columns "col1 int, col2 double" --storage_location /tmp/uc/myTable
```

If you list the tables again you should see this new table. Next, let's write to the table with
some randomly generated data (again, powered by [Delta Kernel Java](https://delta.io/blog/delta-kernel/)] and read it back.

```
bin/uc table write --full_name unity.default.myTable
bin/uc table read --full_name unity.default.myTable
```

For detailed information on the commands, please refer to [CLI documentation](./cli.md)

### Operate on Delta tables with DuckDB

For trying with DuckDB, you will have to [install it](https://duckdb.org/docs/installation/) (at least version 1.0).
Let's start DuckDB and install a couple of extensions. To start DuckDB, run the command `duckdb` in the terminal.
Then, in the DuckDB shell, run the following commands:
```sh
install uc_catalog from core_nightly;
load uc_catalog;
install delta;
load delta;
```
If you have installed these extensions before, you may have to run `update extensions` and restart DuckDB
for the following steps to work.

Now that we have DuckDB all set up, let's try connecting to UC by specifying a secret.
```sh
CREATE SECRET (
      TYPE UC,
      TOKEN 'not-used',
      ENDPOINT 'http://127.0.0.1:8080',
      AWS_REGION 'us-east-2'
 );
```
You should see it print a short table saying `Success` = `true`. Then we attach the `unity` catalog to DuckDB.
```sh
ATTACH 'unity' AS unity (TYPE UC_CATALOG);
```
Now we ready to query. Try the following

```sql
SHOW ALL TABLES;
SELECT * from unity.default.numbers;
```

You should see the tables listed and the contents of the `numbers` table printed.
TO quit DuckDB, run the command `Ctrl+D` or type `.exit` in the DuckDB shell.

### Operate on Volumes with the CLI

Let's list the volumes.
```
bin/uc volume list --catalog unity --schema default
```

You should see a few volumes. Let's get the metadata of one of those volumes.

```
bin/uc volume get --full_name unity.default.json_files
```

Now let's list the directories/files in this volume.

```
bin/uc volume read --full_name unity.default.json_files
```
You should see two text files listed and one directory. Let's read the content of one of those files.

```
bin/uc volume read --full_name unity.default.json_files --path c.json
```
Voila! You have read the content of a file stored in a volume.We can also list the contents of any subdirectory.
For e.g.:

```
bin/uc volume read --full_name unity.default.json_files --path dir1
```

Now let's try creating a new external volume. First physically create a directory with some files in it.
For example in the project root directory, create a directory `/tmp/myVolume` and put some files in it.
Then create the volume in UC.

```
bin/uc volume create --full_name unity.default.myVolume --storage_location /tmp/myVolume
```
Now you can see the contents of this volume.

```
bin/uc volume read --full_name unity.default.myVolume
```

### Operate on Functions with the CLI

Let's list the functions.
```
bin/uc function list --catalog unity --schema default
```

You should see a few functions. Let's get the metadata of one of those functions.

```
bin/uc function get --full_name unity.default.sum
```

In the printed metadata, pay attention to the columns `input_parameters` and `external_language` and `routine_definition`.
This seems like a simple python function that takes 3 arguments and returns the sum of them. Let's try calling this function.
The invoking of the function is achieved by calling the python script at `etc/data/function/python_engine.py`
with the function name and arguments.

```
bin/uc function call --full_name unity.default.sum --input_params "1,2,3"
```

Voila! You have called a function stored in UC. Lets try and create a new function.

```
bin/uc function create --full_name unity.default.myFunction --data_type INT --input_params "a int, b int" --def "c=a*b\nreturn c"
```
You can test out the newly created function by invoking it.

```
bin/uc function call --full_name unity.default.myFunction --input_params "2,3"
```
Bet you see the result as 6.


## Read Delta Uniform tables via Iceberg REST Catalog
Delta Tables with Uniform enabled can be accessed via Iceberg REST Catalog. The Iceberg REST Catalog is served at
`http://127.0.0.1:8080/api/2.1/unity-catalog/iceberg/`.

A pre-populated Delta Uniform table can be prepared by running `cp -r etc/data/external/unity/default/tables/marksheet_uniform /tmp/marksheet_uniform`.

### Setting up REST Catalog with Apache Spark
The following is an example of the settings to configure OSS Apache Spark to read UniForm as Iceberg:
```
"spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
"spark.sql.catalog.iceberg": "org.apache.iceberg.spark.SparkCatalog",
"spark.sql.catalog.iceberg.catalog-impl": "org.apache.iceberg.rest.RESTCatalog",
"spark.sql.catalog.iceberg.uri": "http://127.0.0.1:8080/api/2.1/unity-catalog/iceberg",
"spark.sql.catalog.iceberg.token":"not_used",
```

When querying Iceberg REST Catalog for Unity Catalog, tables are identified using the following pattern `iceberg.<catalog-name>.<schema-name>.<table-name>`,
e.g. `iceberg.unity.default.marksheet_uniform`.

### Setting up REST Catalog with Trino
After setting up Trino, REST Catalog connection can be setup by adding a `etc/catalog/iceberg.properties` file to configure Trino to use Unity Catalog's Iceberg REST API Catalog endpoint.
```
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=http://127.0.0.1:8080/api/2.1/unity-catalog/iceberg
iceberg.rest-catalog.security=OAUTH2
iceberg.rest-catalog.oauth2.token=not_used
```

Once your properties file is configured, you can run the Trino CLI and issue a SQL query against the Delta UniForm table:
```
SELECT * FROM iceberg."unity.default".marksheet_uniform
```

## APIs and Compatibility
- Open API specification: The Unity Catalog Rest API is documented [here](../api).
- Compatibility and stability: The APIs are currently evolving and should not be assumed to be stable.