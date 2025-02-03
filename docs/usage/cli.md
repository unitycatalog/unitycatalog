# Unity Catalog CLI

This page shows you how to use the Unity Catalog CLI.

The CLI tool allows users to interact with a Unity Catalog server to create and manage catalogs, schemas, tables across different formats, volumes with unstructured data, functions, ML and AI models, and control catalog server and metastore configuration.

!!! note "Specify token for authenticated access"

    If you have set up authentication, you will need to provide an authentication token when executng all of the following commands on this page.
    For example, in the following section, to run the catalog list command, you would specify:

    ```sh
    bin/uc --auth_token $token catalog list
    ```

    where `$token` is the authentication token provided by an identity provider. For more information on how to support both authentication and authorization, please refer to the [auth](../server/auth.md) documentation.

## Catalog Management CLI Usage

You can use the Unity Catalog CLI to manage catalogs within your system. The `bin/uc` script supports various operations such as creating, retrieving, listing, updating and deleting catalogs. Let's take a look at each operation.

!!! note "Default local Unity Catalog instance"

    All examples on this page will use the local Unity Catalog instance which comes pre-loaded with a default catalog (`unity`), schema (`default`) and some default assets.

### List Catalogs

Here's how to list catalogs contained in a Unity Catalog instance.

```sh
bin/uc catalog list \
  [--max_results <max_results>] # (1)
```

1. `max_results`: optional flag to set the maximum number of results to return.

This will output:

```console
┌─────┬────────────┬──────────┬─────┬─────────────┬──────────┬──────────┬──────────┬────────────────────────────────────┐
│NAME │  COMMENT   │PROPERTIES│OWNER│ CREATED_AT  │CREATED_BY│UPDATED_AT│UPDATED_BY│                 ID                 │
├─────┼────────────┼──────────┼─────┼─────────────┼──────────┼──────────┼──────────┼────────────────────────────────────┤
│unity│Main catalog│{}        │null │1721238005334│null      │null      │null      │f029b870-9468-4f10-badc-630b41e5690d│
└─────┴────────────┴──────────┴─────┴─────────────┴──────────┴──────────┴──────────┴────────────────────────────────────┘
```

### Get details of a Catalog

Here's how to retrieve the details of a catalog:

```sh
bin/uc catalog get \
  --name <name> # (1)
```

1. `name`: The name of the catalog.

This should output:

```console
┌─────────────────────┬──────────────────────────────────────────┐
│         KEY         │                  VALUE                   │
├─────────────────────┼──────────────────────────────────────────┤
│NAME                 │unity                                     │
├─────────────────────┼──────────────────────────────────────────┤
│COMMENT              │Main catalog                              │
├─────────────────────┼──────────────────────────────────────────┤
│PROPERTIES           │{}                                        │
├─────────────────────┼──────────────────────────────────────────┤
│OWNER                │null                                      │
├─────────────────────┼──────────────────────────────────────────┤
│CREATED_AT           │1721238005334                             │
├─────────────────────┼──────────────────────────────────────────┤
│CREATED_BY           │null                                      │
├─────────────────────┼──────────────────────────────────────────┤
│UPDATED_AT           │null                                      │
├─────────────────────┼──────────────────────────────────────────┤
│UPDATED_BY           │null                                      │
├─────────────────────┼──────────────────────────────────────────┤
│ID                   │f029b870-9468-4f10-badc-630b41e5690d      │
└─────────────────────┴──────────────────────────────────────────┘
```

### Create a Catalog

You can create a new catalog using:

```sh
bin/uc catalog create \
  --name <name> \ # (1)
  [--comment <comment>] \ # (2)
  [--properties <properties>] # (3)
```

1. `name`: The name of the catalog.
2. `comment`: _\[Optional\]_ The description of the catalog.
3. `properties`: _\[Optional\]_ The properties of the catalog in JSON format
   (e.g., `'{"key1": "value1", "key2": "value2"}'`). Make sure to either escape the double quotes(`\"`) inside the
   properties string or just use single quotes(`''`) around the same.

Here's an example:

```sh
bin/uc catalog create --name my_catalog --comment "My First Catalog" --properties '{"key1": "value1", "key2": "value2"}'
```

### Update a Catalog

You can update an existing catalog using:

```sh
bin/uc catalog update \
--name <name> \ # (1)
[--new_name <new_name>] \ # (2)
[--comment <comment>] \ # (3)
[--properties <properties>] # (4)
```

1. `name`: The name of the existing catalog.
2. `new_name`: _\[Optional\]_ The new name of the catalog.
3. `comment`: _\[Optional\]_ The new description of the catalog.
4. `properties`: _\[Optional\]_ The new properties of the catalog in JSON format
   (e.g., `'{"key1": "value1", "key2": "value2"}'`). Make sure to either escape the double quotes(`\"`) inside the
   properties string or just use single quotes(`''`) around the same.

!!! note "At least one of the optional parameters must be specified."

Here's an example:

```sh
bin/uc catalog update \
  --name my_catalog \
  --new_name my_updated_catalog \
  --comment "Updated Catalog" \
  --properties '{"updated_key": "updated_value"}'
```

### Delete a Catalog

You can delete an existing catalog using:

```sh
bin/uc catalog delete \
  --name <name> # (1)
```

1. `name`: The name of the catalog.

## Schema Management CLI Usage

You can use the Unity Catalog CLI to manage schemas within your catalog. The `bin/uc` script supports various operations such as creating, retrieving, listing, updating and deleting schemas. Let's take a look at each operation.

### List Schemas

You can list schemas within your catalog using:

```sh
bin/uc schema list \
  --catalog <catalog> \ # (1)
  [--max_results <max_results>] # (2)
```

1. `catalog`: The name of the catalog.
2. `max_results`: _\[Optional\]_ The maximum number of results to return.

### Get a Schema

You can retrieve the details of a schema using:

```sh
bin/uc schema get \
  --full_name <full_name> # (1)
```

1. `full_name`: The full name of the existing schema. The full name is the concatenation of the catalog name and schema name separated by a dot (e.g., `catalog_name.schema_name`).

### Create a Schema

You can create a new schema stored within a catalog using:

```sh
bin/uc schema create \
  --catalog <catalog> \ # (1)
  --name <name> \ # (2)
  [--comment <comment>] \ # (3)
  [--properties <properties>] # (4)
```

1. `catalog`: The name of the catalog.
2. `name`: The name of the schema.
3. `comment`: _\[Optional\]_ The description of the schema.
4. `properties`: _\[Optional\]_ The properties of the schema in JSON format
   (e.g., `'{"key1": "value1", "key2": "value2"}'`). Make sure to either escape the double quotes(`\"`) inside the
   properties string or just use single quotes(`''`) around the same.

Here's an example:

```sh
bin/uc schema create \
  --catalog my_catalog \
  --name my_schema \
  --comment "My Schema"
  --properties '{"key1": "value1", "key2": "value2"}'
```

### Update a Schema

You can update an existing schema using:

```sh
bin/uc schema update \
  --full_name <full_name> \ # (1)
  [--new_name <new_name>] \ # (2)
  [--comment <comment>] \ # (3)
  [--properties <properties>] # (4)
```

1. `full_name`: The full name of the existing schema. The full name is the concatenation of the catalog name and schema name separated by a dot (e.g., `catalog_name.schema_name`).
2. `new_name`: _\[Optional\]_ The new name of the schema.
3. `comment`: _\[Optional\]_ The new description of the schema.
4. `properties`: _\[Optional\]_ The new properties of the schema in JSON format
   (e.g., `'{"key1": "value1", "key2": "value2"}'`). Make sure to either escape the double quotes(`\"`) inside the
   properties string or just use single quotes(`''`) around the same.

!!! note "At least one of the optional parameters must be specified."

Here's an example:

```sh
bin/uc schema update \
  --full_name my_catalog.my_schema \
  --new_name my_updated_schema \
  --comment "Updated Schema" \
  --properties '{"updated_key": "updated_value"}'
```

### Delete a Schema

You can delete an existing schema using:

```sh
bin/uc schema delete \
  --full_name <full_name> # (1)
```

1. `full_name`: The full name of the existing schema. The full name is the concatenation of the catalog name and schema name separated by a dot (e.g., `catalog_name.schema_name`).

## Table Management CLI Usage

You can use the Unity Catalog CLI to manage tables within your schema. The `bin/uc` script supports various operations such as creating, retrieving, listing, updating and deleting tables. Let's take a look at each operation.

You can also read more in the [Tables](./tables/deltalake.md) section.

### List Tables

You can list tables stored in a schema using:

```sh
bin/uc table list \
  --catalog <catalog> \ # (1)
  --schema <schema> \ # (2)
  [--max_results <max_results>] # (3)
```

1. `catalog`: The name of the catalog.
2. `schema`: The name of the schema.
3. `max_results`: _\[Optional\]_ The maximum number of results to return.

### Retrieve Table Information

You can retrieve table information using:

```sh
bin/uc table get \
  --full_name <full_name> # (1)
```

1. `full_name`: The full name of an existing table. The full name is the concatenation of the catalog name, schema name and table name separated by dots (e.g., `catalog_name.schema_name.table_name`).

### Create a Table

You can create a table using:

```sh
bin/uc table create \
  --full_name <full_name> \ # (1)
  --columns <columns> \ # (2)
  --storage_location <storage_location> \ # (3)
  [--format <format>] \ # (4)
  [--properties <properties>] # (5)
```

1. `full_name`: The full name of the table, which is a concatenation of the catalog name,
   schema name, and table name separated by dots (e.g., `catalog_name.schema_name.table_name`).
2. `columns`: The columns of the table in SQL-like format `"column_name column_data_type"`.
   Supported data types include `BOOLEAN`, `BYTE`, `SHORT`, `INT`, `LONG`, `FLOAT`, `DOUBLE`, `DATE`, `TIMESTAMP`, `TIMESTAMP_NTZ`, `STRING`, `BINARY`, `DECIMAL`. Separate multiple columns with a comma
   (e.g., `"id INT, name STRING"`).
3. `format`: _\[Optional\]_ The format of the data source. Supported values are `DELTA`, `PARQUET`, `ORC`, `JSON`,`CSV`, `AVRO`, and `TEXT`. If not specified the default format is `DELTA`.
4. `storage_location`: The storage location associated with the table. It is a mandatory field for `EXTERNAL` tables.
5. `properties`: _\[Optional\]_ The properties of the table in JSON format
   (e.g., `'{"key1": "value1", "key2": "value2"}'`). Make sure to either escape the double quotes(`\"`) inside the properties string or just use single quotes(`''`) around the same.

Here's an example to create an external DELTA table with columns `id` and `name` in the schema `my_schema` of catalog `my_catalog` with storage location `/path/to/storage`:

```sh
bin/uc table create \
  --full_name my_catalog.my_schema.my_table \
  --columns "id INT, name STRING" \
  --storage_location "/path/to/storage"
```

When running against UC server, the storage location can be a local path(absolute path) or an S3 path.
When S3 path is provided, the [server configuration](../server/configuration.md) will vend temporary credentials to access the S3 bucket and server properties must be set up accordingly.

### Read a Delta Table

You can read a Delta table using:

```sh
bin/uc table read \
  --full_name <full_name> \ # (1)
  [--max_results <max_results>] # (2)
```

1. `full_name`: The full name of the table, which is a concatenation of the catalog name,
   schema name, and table name separated by dots (e.g., `catalog_name.schema_name.table_name`).
2. `max_results`: _\[Optional\]_ The maximum number of rows to return.

### Write Sample Data to a Delta Table

You can write sample data to a Delta table using:

```sh
bin/uc table write \
  --full_name <catalog>.<schema>.<table> # (1)
```

1. `full_name`: The full name of the table, which is a concatenation of the catalog name,
   schema name, and table name separated by dots (e.g., `catalog_name.schema_name.table_name`).

This is an experimental feature and only some primitive types are supported for writing sample data.

### Delete a Table

You can delete an existing table using:

```sh
bin/uc table delete \
  --full_name <full_name> #(1)
```

1. `full_name`: The full name of the table, which is a concatenation of the catalog name,
   schema name, and table name separated by dots (e.g., `catalog_name.schema_name.table_name`).

## Volume Management CLI Usage

You can use the Unity Catalog CLI to manage volumes within your schema. The `bin/uc` script supports various operations such as creating, retrieving, listing, updating and deleting volumes. Let's take a look at each operation.

You can also read more in the [Volumes](./volumes.md) section.

### List Volumes

You can list all volumes stored in a Unity Catalog schema using:

```sh
bin/uc volume list \
  --catalog <catalog> \ # (1)
  --schema <schema> \ # (2)
  [--max_results <max_results>] # (3)
```

1. `catalog`: The name of the catalog.
2. `schema`: The name of the schema.
3. `max_results`: _\[Optional\]_ The maximum number of results to return.

### Retrieve Volume Information

You can inspect information about your volume using:

```sh
bin/uc volume get \
  --full_name <full_name> # (1)
```

1. `full_name`: The full name of the volume, which is a concatenation of the catalog name,
   schema name, and volume name separated by dots (e.g., `catalog_name.schema_name.volume_name`).

### Create a Volume

You can create a new volume using:

```sh
bin/uc volume create \
  --full_name <full_name> \ # (1)
  --storage_location <storage_location> \ # (2)
  [--comment <comment>] # (3)
```

1. `full_name`: The full name of the volume, which is a concatenation of the catalog name,
   schema name, and volume name separated by dots (e.g., `catalog_name.schema_name.volume_name`).
2. `storage_location`: The storage location associated with the volume. When running against UC OSS server,
   the storage location can be a local path(absolute path) or an S3 path. When S3 path is provided, the
   [server configuration](../server/configuration.md) will vend temporary credentials to access the S3 bucket and server properties must be set up accordingly. When running against Databricks Unity Catalog, the storage location for EXTERNAL volume can only be an S3 location which has been configured as an `external location` in your Databricks workspace.
3. `comment`: _\[Optional\]_ The description of the volume.

Here's an example that creates an external volume with full name `my_catalog.my_schema.my_volume` with storage location `/path/to/storage`:

```sh
bin/uc volume create \
  --full_name my_catalog.my_schema.my_volume \
  --storage_location "/path/to/storage"
```

### Update a Volume

You can update an existing volume using:

```sh
bin/uc volume update \
  --full_name <catalog>.<schema>.<volume> \ # (1)
  --new_name <new_name> \ # (2)
  [--comment <comment>] # (3)
```

1. `full_name`: The full name of the volume, which is a concatenation of the catalog name,
   schema name, and volume name separated by dots (e.g., `catalog_name.schema_name.volume_name`).
2. `new_name`: _\[Optional\]_ The new name of the volume.
3. `comment`: _\[Optional\]_ The new description of the volume.

!!! note "At least one of the optional parameters must be specified."

Here's an example:

```sh
bin/uc volume update \
  --full_name my_catalog.my_schema.my_volume \
  --new_name my_updated_volume \
  --comment "Updated Volume"
```

### Read Volume content

Here's how you can access the contens of a volume:

```sh
bin/uc volume read \
  --full_name <catalog>.<schema>.<volume> \ # (1)
  [--path <path>] # (2)
```

1. `full_name`: The full name of the volume, which is a concatenation of the catalog name,
   schema name, and volume name separated by dots (e.g., `catalog_name.schema_name.volume_name`).
2. `path`: _\[Optional\]_ The path relative to the volume root. If no path is provided, the volume root is read. If the final path is a directory, the contents of the directory are listed(`ls`). If the final path is a file, the contents of the file are displayed(`cat`).

### Write Sample Data to a Volume

Here's how you can write sample date to a volume:

```sh
bin/uc volume write \
  --full_name <full_name> # (1)
```

1. `full_name`: The full name of the volume, which is a concatenation of the catalog name,
   schema name, and volume name separated by dots (e.g., `catalog_name.schema_name.volume_name`).

This operation will write sample text data to a randomly generated file name(UUID) in the volume. This is an experimental feature.

### Delete a Volume

You can delete an existing volume using:

```sh
bin/uc volume delete \
  --full_name <full_name> # (1)
```

1. `full_name`: The full name of the volume, which is a concatenation of the catalog name,
   schema name, and volume name separated by dots (e.g., `catalog_name.schema_name.volume_name`).

## Function Management CLI Usage

You can use the Unity Catalog CLI to manage functions within your schema. The `bin/uc` script supports various operations such as creating, retrieving, listing, updating and deleting function. Let's take a look at each operation.

You can also read more in the [Functions](./functions.md) section.

### List Functions

You can list functions stored in a Unity Catalog schema using:

```sh
bin/uc function list \
  --catalog <catalog> \ # (1)
  --schema <schema> \ # (2)
  [--max_results <max_results>] # (3)
```

1. `catalog`: The name of the catalog.
2. `schema`: The name of the schema.
3. `max_results`: _\[Optional\]_ The maximum number of results to return.

### Retrieve Function Information

You can inspect metadata about your function stored in Unity Catalog using:

```sh
bin/uc function get \
  --full_name <full_name> # (1)
```

1. `full_name`: The full name of the function, which is a concatenation of the catalog name,
   schema name, and function name separated by dots (e.g., `catalog_name.schema_name.function_name`).

### Create a Function

You can create a new function using:

```sh
bin/uc function create \
  --full_name <full_name> \ # (1)
  --input_params <input_params> \ # (2)
  --data_type <data_type> \ # (3)
  --def <definition> \ # (4)
  [--comment <comment>] \ # (5)
  [--language <language>] # 6)
```

1. `full_name`: The full name of the function, which is a concatenation of the catalog name, schema name, and function
   name separated by dots (e.g., `catalog_name.schema_name.function_name`).
2. `input_params`: The input parameters to the function in SQL-like format `"param_name param_data_type"`.
   Multiple input parameters should be separated by a comma (e.g., `"param1 INT, param2 STRING"`).
3. `data_type`: The data type of the function. Either a type_name (for e.g. `INT`,`DOUBLE`, `BOOLEAN`, `DATE`), or
   `TABLE_TYPE` if this is a table valued function.
4. `def`: The definition of the function. The definition should be a valid SQL statement or a python routine with the
   function logic and return statement.
5. `comment`: _\[Optional\]_ The description of the function.
6. `language`: _\[Optional\]_ The language of the function. If not specified, the default value is `PYTHON`.

Here's an example that creates a Python function that takes two integer inputs and returns the sum of the inputs:

```sh
bin/uc function create \
  --full_name my_catalog.my_schema.my_function \
  --input_params "param1 INT, param2 INT" \
  --data_type INT \
  --def "return param1 + param2" \
  --comment "Sum Function"
```

### Invoke a Function

You can invoke a function stored in Unity Catalog using:

```sh
bin/uc function call \
  --full_name <catalog>.<schema>.<function_name> \ # (1)
  --input_params <input_params> # (2)
```

1. `full_name`: The full name of the function, which is a concatenation of the catalog name, schema name, and function name separated by dots (e.g., `catalog_name.schema_name.function_name`).
2. `input_params` : The value of input parameters to the function separated by a comma (e.g., `"param1,param2"`).

This is an experimental feature and only supported for Python functions that take in primitive types as input
parameters. It runs the functions using the Python engine script at `etc/data/function/python_engine.py`.

Here's an example that invokes a Python sum function that takes two integer inputs:

```sh
bin/uc function call \
  --full_name my_catalog.my_schema.my_function \
  --input_params "1,2"
```

### Delete a Function

You can delete a function using:

```sh
bin/uc function delete \
  --full_name <full_name> # (1)
```

1. `full_name`: The full name of the function, which is a concatenation of the catalog name, schema name, and function
   name separated by dots (e.g., `catalog_name.schema_name.function_name`).

## Registered model and model version management

You can use Unity Catalog with MLflow to govern and access your ML and AI models. Read more

Please refer to [MLflow documentation](https://mlflow.org/docs/latest/index.html) to learn how to use MLflow to create,
register, update, use, and delete registered models and model versions.

You can list the registered models in your UC namespace using:

```sh
bin/uc registered_model list \
  --catalog <catalog_name> \ # (1)
  --schema <schema_name> # (2)
```

1. `catalog`: The name of the catalog.
2. `schema`: The name of the schema.

You can list the model versions under a registered model using:

```sh
bin/uc model_version list \
  --full_name <full_name> # (1)
```

1. `full_name`: The full name of the model, which is a concatenation of the catalog name, schema name, and model name separated by dots (e.g., `catalog_name.schema_name.model_name`).

You can get the metadata of registered models or model versions using:

```sh
bin/uc registered_model get \
  --full_name <full_name> # (1)
```

1. `full_name`: The full name of the model, which is a concatenation of the catalog name, schema name, and model name separated by dots (e.g., `catalog_name.schema_name.model_name`).

You can update the comment or name of a registered models using:

```sh
bin/uc registered_model update \
  --full_name <full_name> \ # (1)
  --new_name <new_name> \ # (2)
  [--comment <comment>] # (3)
```

1. `full_name`: The full name of the model, which is a concatenation of the catalog name, schema name, and model name separated by dots (e.g., `catalog_name.schema_name.model_name`).
2. `new_name`: _\[Optional\]_ The new name of the volume.
3. `comment`: _\[Optional\]_ The description of the function.

Read more in the [Models](models.md) documentatino.

## CLI Server Configuration

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

!!! feedback "Different look for users CLI commands"

    We're trying out a different look for the CLI commands - which do you prefer - the format above this or the format below? Chime in UC GitHub discussion [529](https://github.com/unitycatalog/unitycatalog/discussions/529) and let us know!

## Manage Users

This section outlines the usage of the `bin/uc` script for managing users within UC.
The script supports various operations such as creating, getting, updating, listing, and deleting users.

### Create User

```sh
bin/uc user create [options]
```

_Required Params:_

    -- name: The name of the entity.
    -- email : The email address for the user

_Optional Params:_

    -- server: UC Server to connect to. Default is reference server.
    -- auth_token: PAT token to authorize uc requests.
    -- external_id: The identity provider's id for the user
    -- output: To indicate CLI output format preference. Supported values are json and jsonPretty.

### Delete User

```sh
bin/uc user delete [options]
```

_Required Params:_

    --id The unique id of the user

_Optional Params:_

    --server UC Server to connect to. Default is reference server.
    --auth_token PAT token to authorize uc requests.
    --output To indicate CLI output format preference. Supported values are json and jsonPretty.

### Get User

```sh
bin/uc user get [options]
```

_Required Params:_

    --id The unique id of the user

_Optional Params:_

    --server UC Server to connect to. Default is reference server.
    --auth_token PAT token to authorize uc requests.
    --output To indicate CLI output format preference. Supported values are json and jsonPretty.

### List Users

```sh
bin/uc user list [options]
```

_Required Params:_

    None

_Optional Params:_

    --server UC Server to connect to. Default is reference server.
    --auth_token PAT token to authorize uc requests.
    --output To indicate CLI output format preference. Supported values are json and jsonPretty.
    --filter Query by which the results have to be filtered
    --start_index Specifies the index (starting at 1) of the first result.
    --count Desired number of results per page

### Update User

```sh
bin/uc user update [options]
```

_Required Params:_

    --id The unique id of the user

_Optional Params:_

    --server UC Server to connect to. Default is reference server.
    --auth_token PAT token to authorize uc requests.
    --output To indicate CLI output format preference. Supported values are json and jsonPretty.
    --name The name of the entity.
    --external_id The identity provider's id for the user
    --email The email address for the user

## Metastore Management CLI Usage

This section outlines the usage of the `bin/uc` script to handle operations related to the metastore of the
UC OSS server.

### Get Metastore Information

Gets information about the metastore hosted by this Unity Catalog service
(currently the service hosts only one metastore)

```sh
bin/uc metastore get
```
