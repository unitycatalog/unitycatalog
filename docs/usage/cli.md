# Unity Catalog CLI

## Introduction

This CLI tool allows users to interact with a Unity Catalog server to create and manage catalogs, schemas, tables across different formats (DELTA, UNIFORM, PARQUET, JSON, and CSV), volumes with unstructured data, and functions.

The Unity Catalog server can be a standalone server (see [the server guide](./server.md) for how to start one), or simply configure the CLI to interface with Databricks Unity Catalog.

# Table of Contents
1. [Catalog Management CLI Usage](#catalog-management-cli-usage)
2. [Schema Management CLI Usage](#schema-management-cli-usage)
3. [Table Management CLI Usage](#table-management-cli-usage)
4. [Volume Management CLI Usage](#volume-management-cli-usage)
5. [Function Management CLI Usage](#function-management-cli-usage)

## Catalog Management CLI Usage

This section outlines the usage of the `bin/uc` script for managing catalogs within your system. 
The script supports various operations such as creating, retrieving, listing and updating catalogs.

### List Catalogs
```sh
bin/uc catalog list
```

### Get details of a Catalog
```sh
bin/uc catalog get --name <name> 
```
- `name` : The name of the catalog.

### Create a Catalog
```sh
bin/uc catalog create --name <name> [--comment <comment>]
```
- `name`: The name of the catalog.
- `comment`: *[Optional]* The description of the catalog.

Example: 
```sh
bin/uc catalog create --name my_catalog --comment "My First Catalog"
```

### Update a Catalog
```sh
bin/uc catalog update --name <name> [--new_name <new_name>] [--comment <comment>]
```
- `name` : The name of the existing catalog.
- `new_name` : *[Optional]* The new name of the catalog.
- `comment` : *[Optional]* The new description of the catalog.

*Note:* at least one of the optional parameters must be specified.

Example:
```sh
bin/uc catalog update --name my_catalog --new_name my_updated_catalog --comment "Updated Catalog"
```

### Delete a Catalog
```sh
bin/uc catalog delete --name <name>
```
- `name` : The name of the catalog.

## Schema Management CLI Usage

This section outlines the usage of the `bin/uc` script for managing schemas within your system. 
The script supports various operations such as creating, retrieving, listing, updating and deleting schemas.

### List Schemas
```sh
bin/uc schema list --catalog <catalog> [--max_results <max_results>]
```
- `catalog`: The name of the catalog.
- `max_results`: *[Optional]* The maximum number of results to return.

### Get a Schema
Retrieve the details of a schema using the full name of the schema.
```sh
bin/uc schema get --full_name <catalog>.<schema>
```
- `catalog` : The name of the catalog.
- `schema` : The name of the schema.

### Create a Schema
```sh
bin/uc schema create --catalog <catalog> --name <name> [--comment <comment>]
```
- `catalog`: The name of the catalog.
- `name`: The name of the schema.
- `comment`: *[Optional]*  The description of the schema.

Example:
```sh
bin/uc schema create --catalog my_catalog --name my_schema --comment "My Schema"
```

### Update a Schema
```sh
bin/uc schema update --full_name <full_name> --new_name <new_name> [--comment <comment>]
```
- `full_name`: The full name of the existing schema. The full name is the concatenation of the catalog name and schema name separated by a dot (e.g., `catalog_name.schema_name`).
- `new_name`: The new name of the schema.
- `comment`: *[Optional]* The new description of the schema.

Example:
```sh
bin/uc schema update --full_name my_catalog.my_schema --new_name my_updated_schema --comment "Updated Schema"
```

### Delete a Schema
```sh
bin/uc schema delete --full_name <catalog>.<schema>
```
- `catalog` : The name of the catalog.
- `schema` : The name of the schema.


## Table Management CLI Usage

This section outlines the usage of the `bin/uc` script for managing tables within your system. 
The script supports various operations such as creating, retrieving, listing and deleting tables. 
There's additional functionality to write sample data to a DELTA table and read data from a DELTA table.

### List Tables
```sh
bin/uc table list --catalog <catalog> --schema <schema> [--max_results <max_results>]
```
- `catalog` : The name of the catalog.
- `schema` : The name of the schema.
- `max_results` *[Optional]* : The maximum number of results to return.

###  Retrieve Table Information
```sh
bin/uc table get --full_name <catalog>.<schema>.<table>
```
- `catalog` : The name of the catalog.
- `schema` : The name of the schema.
- `table` : The name of the table.

### Create a Table

```sh
bin/uc table create --full_name <full_name> --columns <columns> --storage_location <storage_location> [--format <format>] [--properties <properties>]
```
- `full_name`: The full name of the table, which is a concatenation of the catalog name, 
  schema name, and table name separated by dots (e.g., `catalog_name.schema_name.table_name`).
- `columns`: The columns of the table in SQL-like format `"column_name column_data_type"`.
  Supported data types include `BOOLEAN`, `BYTE`, `SHORT`, `INT`, `LONG`, `FLOAT`, `DOUBLE`, `DATE`, `TIMESTAMP`,
  `TIMESTAMP_NTZ`, `STRING`, `BINARY`, `DECIMAL`. Separate multiple columns with a comma
  (e.g., `"id INT, name STRING"`).
- `format`: *[Optional]* The format of the data source. Supported values are `DELTA`, `PARQUET`, `ORC`, `JSON`, `CSV`, `AVRO`, and `TEXT`. 
If not specified the default format is `DELTA`.
- `storage_location`: The storage location associated with the table. It is a mandatory field for `EXTERNAL` tables.
- `properties`:  *[Optional]* The properties of the entity in JSON format (e.g., `'{"key1": "value1", "key2": "value2"}'`). 
Make sure to either escape the double quotes(`\"`) inside the properties string or just use single quotes(`''`) around the same.

Example:

* Create an external DELTA table with columns `id` and `name` in the schema `my_schema` of catalog `my_catalog` with storage location `/path/to/storage`:

```sh
bin/uc table create --full_name my_catalog.my_schema.my_table --columns "id INT, name STRING" --storage_location "/path/to/storage"
```

When running against UC OSS server, the storage location can be a local path(absolute path) or an S3 path. When S3 path is provided, 
the [server](./server.md) will vend temporary credentials to access the S3 bucket and server properties must be set up accordingly.
When running against Databricks Unity Catalog, the storage location for EXTERNAL table can only be an S3 location which
has been configured as an `external location` in your Databricks workspace.

### Read a DELTA Table
```sh
bin/uc table read --full_name <catalog>.<schema>.<table> [--max_results <max_results>]
```
- `catalog` : The name of the catalog.
- `schema` : The name of the schema.
- `table` : The name of the table.
- `max_results` : *[Optional]* The maximum number of rows to return.

### Write Sample Data to a DELTA Table
```sh
bin/uc table write --full_name <catalog>.<schema>.<table>
```
- `catalog` : The name of the catalog.
- `schema` : The name of the schema.
- `table` : The name of the table.

This is an experimental feature and only some primitive types are supported for writing sample data.

### Delete a Table
```sh
bin/uc table delete --full_name <catalog>.<schema>.<table>
```
- `catalog` : The name of the catalog.
- `schema` : The name of the schema.
- `table` : The name of the table.

## Volume Management CLI Usage

This section outlines the usage of the `bin/uc` script for managing volumes within your system. 
The script supports various operations such as creating, retrieving, listing and deleting volumes.

### List Volumes
```sh
bin/uc volume list --catalog <catalog> --schema <schema> [--max_results <max_results>]
```
- `catalog` : The name of the catalog.
- `schema` : The name of the schema.
- `max_results` : *[Optional]* The maximum number of results to return.

### Retrieve Volume Information
```sh
bin/uc volume get --full_name <catalog>.<schema>.<volume>
```
- `catalog` : The name of the catalog.  
- `schema` : The name of the schema.
- `volume` : The name of the volume.

### Create a Volume

```sh
bin/uc volume create --full_name <catalog>.<schema>.<volume> --storage_location <storage_location> [--comment <comment>]
```
- `catalog` : The name of the catalog.
- `schema` : The name of the schema.
- `volume` : The name of the volume.
- `storage_location` : The storage location associated with the volume. When running against UC OSS server, 
the storage location can be a local path(absolute path) or an S3 path.
When S3 path is provided, the [server](./server.md) will vend temporary credentials to access the S3 bucket and server properties must be set up accordingly.
When running against Databricks Unity Catalog, the storage location for EXTERNAL volume can only be an S3 location which
has been configured as an `external location` in your Databricks workspace.
- `comment` : *[Optional]* The description of the volume.

Example:
* Create an external volume with full name `my_catalog.my_schema.my_volume` with storage location `/path/to/storage` :
```sh
bin/uc volume create --full_name my_catalog.my_schema.my_volume --storage_location "/path/to/storage"
```

### Update a Volume
```sh
bin/uc volume update --full_name <catalog>.<schema>.<volume> --new_name <new_name> [--comment <comment>]
```
- `catalog` : The name of the catalog.
- `schema` : The name of the schema.
- `volume` : The name of the volume.
- `new_name` : The new name of the volume.
- `comment` : *[Optional]* The new description of the volume.

Example:
```sh
bin/uc volume update --full_name my_catalog.my_schema.my_volume --new_name my_updated_volume --comment "Updated Volume"
```

### Read a Volume's content
```sh
bin/uc volume read --full_name <catalog>.<schema>.<volume> [--path <path>]
```
- `catalog` : The name of the catalog.
- `schema` : The name of the schema.
- `volume` : The name of the volume.
- `path` : *[Optional]* The path relative to the volume root. 
If no path is provided, the volume root is read.
If the final path is a directory, the contents of the directory are listed(*ls*). 
If the final path is a file, the contents of the file are displayed(*cat*).

### Write Sample Data to a Volume
```sh
bin/uc volume write --full_name <catalog>.<schema>.<volume>
```
Writes sample txt data to a randomly generated file name(UUID) in the volume. This is an experimental feature.

- `catalog` : The name of the catalog.
- `schema` : The name of the schema.
- `volume` : The name of the volume.

### Delete a Volume
```sh
bin/uc volume delete --full_name <catalog>.<schema>.<volume>
```
- `catalog` : The name of the catalog.
- `schema` : The name of the schema.
- `volume` : The name of the volume. 

## Function Management CLI Usage

This section outlines the usage of the `bin/uc` script for managing functions within your catalog. 
The script supports various operations such as creating, retrieving, listing and deleting functions.

### List Functions
```sh
bin/uc function list --catalog <catalog> --schema <schema> [--max_results <max_results>]
```
- `catalog` : The name of the catalog.
- `schema` : The name of the schema.
- `max_results` : *[Optional]* The maximum number of results to return.

### Retrieve Function Information
```sh
bin/uc function get --full_name <catalog>.<schema>.<function_name>
```
- `catalog` : The name of the catalog.
- `schema` : The name of the schema.
- `function_name` : The name of the function.

### Create a Function
```sh
bin/uc function create --full_name <catalog>.<schema>.<function_name> --input_params <input_params> --data_type <data_type> --def <definition> [--comment <comment>] [--language <language>]
```
- `full_name`: The full name of the function, which is a concatenation of the catalog name, schema name, and function name separated by dots (e.g., `catalog_name.schema_name.function_name`).
- `input_params` : The input parameters to the function in SQL-like format `"param_name param_data_type"`.
Multiple input parameters should be separated by a comma (e.g., `"param1 INT, param2 STRING"`).
- `data_type`: The data type of the function. Either a type_name(for e.g. `INT`,`DOUBLE`, `BOOLEAN`, `DATE`), or `TABLE_TYPE` if this is a table valued function.
- `def`: The definition of the function. The definition should be a valid SQL statement or a python routine with the function logic and return statement.
- `comment`: *[Optional]* The description of the function.
- `language`: *[Optional]*  The language of the function. If not specified, the default value is `PYTHON`.

Example:
Create a python function that takes two integer inputs and returns the sum of the inputs:
```sh
bin/uc function create --full_name my_catalog.my_schema.my_function --input_params "param1 INT, param2 INT" --data_type INT --def "return param1 + param2" --comment "Sum Function"
```

### Invoke a Function
```sh
bin/uc function call --full_name <catalog>.<schema>.<function_name> --input_params <input_params>
```
- `full_name`: The full name of the function, which is a concatenation of the catalog name, schema name, and function name separated by dots (e.g., `catalog_name.schema_name.function_name`).
- `input_params` : The value of input parameters to the function separated by a comma (e.g., `"param1,param2"`).

This is an experimental feature and only supported for python functions that take in primitive types as input parameters.
It runs the functions using the python engine script at `etc/data/function/python_engine.py`.

Example:
Invoke a python sum function that takes two integer inputs:
```sh
bin/uc function call --full_name my_catalog.my_schema.my_function --input_params "1,2"
```

### Delete a Function
```sh
bin/uc function delete --full_name <catalog>.<schema>.<function_name>
```
- `catalog` : The name of the catalog.
- `schema` : The name of the schema.
- `function_name` : The name of the function.


## Server Configuration

By default, the CLI tool is configured to interact with a local reference server running at `http://localhost:8080`.
The CLI can be configured to talk to Databricks Unity Catalog by one of the following methods:

- Include the following params in CLI commands:
    - `--server <server_url>`: The URL of the Unity Catalog server.
    - `--auth_token <auth_token>`: The PAT(Personal Authorization Token) token obtained from Databricks' Workspace.
- Set the following properties in the CLI configuration file located at `examples/cli/src/main/resources/application.properties`:
    - `server`: The URL of the Unity Catalog server.
    - `auth_token`: The PAT(Personal Authorization Token) token obtained from Databricks' Workspace.

Each parameter can be configured either from the CLI or the configuration file, independently of each other.
The CLI will prioritize the values provided from the CLI over the configuration file.
