# Unity Catalog CLI

This page shows you how to use the Unity Catalog CLI.

The CLI tool allows users to interact with a Unity Catalog server to create and manage catalogs, schemas, tables across different formats, volumes with unstructured data, and functions.

!!! note "Specify token for authenticated access"

    For all the following commands, you will need to provide an authentication token when executng these commands.
    For example, in the following section, to run the catalog list command, you would specify:

    ```bash
    bin/uc --auth_token $token catalog list
    ```

    where $token is the authentication token provided by an identity provider. For more information on how to support
    both authentication and authorization, please refer to the [auth](../server/auth.md).

## Catalog Management CLI Usage

This section outlines the usage of the `bin/uc` script for managing catalogs within your system.
The script supports various operations such as creating, retrieving, listing and updating catalogs.

### List Catalogs

Here's how to list catalogs contained in a Unity Catalog instance.

```sh
bin/uc catalog list [--max_results <max_results>]
```

- `max_results`: _\[Optional\]_ The maximum number of results to return.

### Get details of a Catalog

```sh
bin/uc catalog get --name <name>
```

- `name`: The name of the catalog.

### Create a Catalog

```sh
bin/uc catalog create --name <name> [--comment <comment>] [--properties <properties>]
```

- `name`: The name of the catalog.
- `comment`: _\[Optional\]_ The description of the catalog.
- `properties`: _\[Optional\]_ The properties of the catalog in JSON format
  (e.g., `'{"key1": "value1", "key2": "value2"}'`). Make sure to either escape the double quotes(`\"`) inside the
  properties string or just use single quotes(`''`) around the same.

Example:

```sh
bin/uc catalog create --name my_catalog --comment "My First Catalog" --properties '{"key1": "value1", "key2": "value2"}'
```

### Update a Catalog

```sh
bin/uc catalog update --name <name> [--new_name <new_name>] [--comment <comment>] [--properties <properties>]
```

- `name`: The name of the existing catalog.
- `new_name`: _\[Optional\]_ The new name of the catalog.
- `comment`: _\[Optional\]_ The new description of the catalog.
- `properties`: _\[Optional\]_ The new properties of the catalog in JSON format
  (e.g., `'{"key1": "value1", "key2": "value2"}'`). Make sure to either escape the double quotes(`\"`) inside the
  properties string or just use single quotes(`''`) around the same.

**\*Note:** At least one of the optional parameters must be specified.\*

Example:

```sh
bin/uc catalog update --name my_catalog --new_name my_updated_catalog --comment "Updated Catalog" --properties '{"updated_key": "updated_value"}'
```

### Delete a Catalog

```sh
bin/uc catalog delete --name <name>
```

- `name`: The name of the catalog.

## Schema Management CLI Usage

This section outlines the usage of the `bin/uc` script for managing schemas within your system.
The script supports various operations such as creating, retrieving, listing, updating and deleting schemas.

### List Schemas

```sh
bin/uc schema list --catalog <catalog> [--max_results <max_results>]
```

- `catalog`: The name of the catalog.
- `max_results`: _\[Optional\]_ The maximum number of results to return.

### Get a Schema

Retrieve the details of a schema using the full name of the schema.

```sh
bin/uc schema get --full_name <catalog>.<schema>
```

- `catalog`: The name of the catalog.
- `schema`: The name of the schema.

### Create a Schema

```sh
bin/uc schema create --catalog <catalog> --name <name> [--comment <comment>] [--properties <properties>]
```

- `catalog`: The name of the catalog.
- `name`: The name of the schema.
- `comment`: _\[Optional\]_ The description of the schema.
- `properties`: _\[Optional\]_ The properties of the schema in JSON format
  (e.g., `'{"key1": "value1", "key2": "value2"}'`). Make sure to either escape the double quotes(`\"`) inside the
  properties string or just use single quotes(`''`) around the same.

Example:

```sh
bin/uc schema create --catalog my_catalog --name my_schema --comment "My Schema" --properties '{"key1": "value1", "key2": "value2"}'
```

### Update a Schema

```sh
bin/uc schema update --full_name <full_name> [--new_name <new_name>] [--comment <comment>] [--properties <properties>]
```

- `full_name`: The full name of the existing schema. The full name is the concatenation of the catalog name and schema
  name separated by a dot (e.g., `catalog_name.schema_name`).
- `new_name`: _\[Optional\]_ The new name of the schema.
- `comment`: _\[Optional\]_ The new description of the schema.
- `properties`: _\[Optional\]_ The new properties of the schema in JSON format
  (e.g., `'{"key1": "value1", "key2": "value2"}'`). Make sure to either escape the double quotes(`\"`) inside the
  properties string or just use single quotes(`''`) around the same.

**\*Note:** At least one of the optional parameters must be specified.\*

Example:

```sh
bin/uc schema update --full_name my_catalog.my_schema --new_name my_updated_schema --comment "Updated Schema" --properties '{"updated_key": "updated_value"}'
```

### Delete a Schema

```sh
bin/uc schema delete --full_name <catalog>.<schema>
```

- `catalog`: The name of the catalog.
- `schema`: The name of the schema.

## Table Management CLI Usage

This section outlines the usage of the `bin/uc` script for managing tables within your system.
The script supports various operations such as creating, retrieving, listing and deleting tables.
There's additional functionality to write sample data to a DELTA table and read data from a DELTA table.

### List Tables

```sh
bin/uc table list --catalog <catalog> --schema <schema> [--max_results <max_results>]
```

- `catalog`: The name of the catalog.
- `schema`: The name of the schema.
- `max_results`: _\[Optional\]_ The maximum number of results to return.

### Retrieve Table Information

```sh
bin/uc table get --full_name <catalog>.<schema>.<table>
```

- `catalog`: The name of the catalog.
- `schema`: The name of the schema.
- `table`: The name of the table.

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
- `format`: _\[Optional\]_ The format of the data source. Supported values are `DELTA`, `PARQUET`, `ORC`, `JSON`,
  `CSV`, `AVRO`, and `TEXT`. If not specified the default format is `DELTA`.
- `storage_location`: The storage location associated with the table. It is a mandatory field for `EXTERNAL` tables.
- `properties`: _\[Optional\]_ The properties of the table in JSON format
  (e.g., `'{"key1": "value1", "key2": "value2"}'`). Make sure to either escape the double quotes(`\"`) inside the
  properties string or just use single quotes(`''`) around the same.

Example:

- Create an external DELTA table with columns `id` and `name` in the schema `my_schema` of catalog `my_catalog` with
  storage location `/path/to/storage`:

```sh
bin/uc table create --full_name my_catalog.my_schema.my_table --columns "id INT, name STRING" --storage_location "/path/to/storage"
```

When running against UC server, the storage location can be a local path(absolute path) or an S3 path.
When S3 path is provided, the [server configuration](../server/configuration.md) will vend temporary credentials to
access the S3 bucket and server properties must be set up accordingly.

### Read a DELTA Table

```sh
bin/uc table read --full_name <catalog>.<schema>.<table> [--max_results <max_results>]
```

- `catalog`: The name of the catalog.
- `schema`: The name of the schema.
- `table`: The name of the table.
- `max_results`: _\[Optional\]_ The maximum number of rows to return.

### Write Sample Data to a DELTA Table

```sh
bin/uc table write --full_name <catalog>.<schema>.<table>
```

- `catalog`: The name of the catalog.
- `schema`: The name of the schema.
- `table`: The name of the table.

This is an experimental feature and only some primitive types are supported for writing sample data.

### Delete a Table

```sh
bin/uc table delete --full_name <catalog>.<schema>.<table>
```

- `catalog`: The name of the catalog.
- `schema`: The name of the schema.
- `table`: The name of the table.

## Volume Management CLI Usage

This section outlines the usage of the `bin/uc` script for managing volumes within your system.
The script supports various operations such as creating, retrieving, listing and deleting volumes.

### List Volumes

```sh
bin/uc volume list --catalog <catalog> --schema <schema> [--max_results <max_results>]
```

- `catalog`: The name of the catalog.
- `schema`: The name of the schema.
- `max_results`: _\[Optional\]_ The maximum number of results to return.

### Retrieve Volume Information

```sh
bin/uc volume get --full_name <catalog>.<schema>.<volume>
```

- `catalog`: The name of the catalog.
- `schema`: The name of the schema.
- `volume`: The name of the volume.

### Create a Volume

```sh
bin/uc volume create --full_name <catalog>.<schema>.<volume> --storage_location <storage_location> [--comment <comment>]
```

- `catalog`: The name of the catalog.
- `schema`: The name of the schema.
- `volume`: The name of the volume.
- `storage_location`: The storage location associated with the volume. When running against UC OSS server,
  the storage location can be a local path(absolute path) or an S3 path. When S3 path is provided, the
  [server configuration](../server/configuration.md) will vend temporary credentials to access the S3 bucket and
  server properties must be set up accordingly. When running against Databricks Unity Catalog, the storage location
  for EXTERNAL volume can only be an S3 location which has been configured as an `external location` in your
  Databricks workspace.
- `comment`: _\[Optional\]_ The description of the volume.

Example:

- Create an external volume with full name `my_catalog.my_schema.my_volume` with storage location `/path/to/storage`:

```sh
bin/uc volume create --full_name my_catalog.my_schema.my_volume --storage_location "/path/to/storage"
```

### Update a Volume

```sh
bin/uc volume update --full_name <catalog>.<schema>.<volume> --new_name <new_name> [--comment <comment>]
```

- `catalog`: The name of the catalog.
- `schema`: The name of the schema.
- `volume`: The name of the volume.
- `new_name`: _\[Optional\]_ The new name of the volume.
- `comment`: _\[Optional\]_ The new description of the volume.

_Note:_ at least one of the optional parameters must be specified.

Example:

```sh
bin/uc volume update --full_name my_catalog.my_schema.my_volume --new_name my_updated_volume --comment "Updated Volume"
```

### Read Volume content

```sh
bin/uc volume read --full_name <catalog>.<schema>.<volume> [--path <path>]
```

- `catalog`: The name of the catalog.
- `schema`: The name of the schema.
- `volume`: The name of the volume.
- `path`: _\[Optional\]_ The path relative to the volume root.
  If no path is provided, the volume root is read.
  If the final path is a directory, the contents of the directory are listed(`ls`).
  If the final path is a file, the contents of the file are displayed(`cat`).

### Write Sample Data to a Volume

```sh
bin/uc volume write --full_name <catalog>.<schema>.<volume>
```

- `catalog`: The name of the catalog.
- `schema`: The name of the schema.
- `volume`: The name of the volume.

Writes sample text data to a randomly generated file name(UUID) in the volume. This is an experimental feature.

### Delete a Volume

```sh
bin/uc volume delete --full_name <catalog>.<schema>.<volume>
```

- `catalog`: The name of the catalog.
- `schema`: The name of the schema.
- `volume`: The name of the volume.

## Function Management CLI Usage

This section outlines the usage of the `bin/uc` script for managing functions within your catalog.
The script supports various operations such as creating, retrieving, listing and deleting functions.

### List Functions

```sh
bin/uc function list --catalog <catalog> --schema <schema> [--max_results <max_results>]
```

- `catalog`: The name of the catalog.
- `schema`: The name of the schema.
- `max_results`: _\[Optional\]_ The maximum number of results to return.

### Retrieve Function Information

```sh
bin/uc function get --full_name <catalog>.<schema>.<function_name>
```

- `catalog`: The name of the catalog.
- `schema`: The name of the schema.
- `function_name`: The name of the function.

### Create a Function

```sh
bin/uc function create --full_name <catalog>.<schema>.<function_name> --input_params <input_params> --data_type <data_type> --def <definition> [--comment <comment>] [--language <language>]
```

- `full_name`: The full name of the function, which is a concatenation of the catalog name, schema name, and function
  name separated by dots (e.g., `catalog_name.schema_name.function_name`).
- `input_params`: The input parameters to the function in SQL-like format `"param_name param_data_type"`.
  Multiple input parameters should be separated by a comma (e.g., `"param1 INT, param2 STRING"`).
- `data_type`: The data type of the function. Either a type_name (for e.g. `INT`,`DOUBLE`, `BOOLEAN`, `DATE`), or
  `TABLE_TYPE` if this is a table valued function.
- `def`: The definition of the function. The definition should be a valid SQL statement or a python routine with the
  function logic and return statement.
- `comment`: _\[Optional\]_ The description of the function.
- `language`: _\[Optional\]_ The language of the function. If not specified, the default value is `PYTHON`.

Example:

- Create a python function that takes two integer inputs and returns the sum of the inputs:

```sh
bin/uc function create --full_name my_catalog.my_schema.my_function --input_params "param1 INT, param2 INT" --data_type INT --def "return param1 + param2" --comment "Sum Function"
```

### Invoke a Function

```sh
bin/uc function call --full_name <catalog>.<schema>.<function_name> --input_params <input_params>
```

- `full_name`: The full name of the function, which is a concatenation of the catalog name, schema name, and function
  name separated by dots (e.g., `catalog_name.schema_name.function_name`).
- `input_params` : The value of input parameters to the function separated by a comma (e.g., `"param1,param2"`).

This is an experimental feature and only supported for python functions that take in primitive types as input
parameters. It runs the functions using the python engine script at `etc/data/function/python_engine.py`.

Example:

- Invoke a python sum function that takes two integer inputs:

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

## Registered model and model version management

Please refer to [MLflow documentation](https://mlflow.org/docs/latest/index.html) to learn how to use MLflow to create,
register, update, use, and delete registered models and model versions.

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

    We're trying out a different look for the CLI commands - which do you prefer - the format above this or the format
    below? Chime in UC GitHub discussion [529](https://github.com/unitycatalog/unitycatalog/discussions/529) and let
    us know!

## Manage Users

This section outlines the usage of the `bin/uc` script for managing users within UC.
The script supports various operations such as creating, getting, updating, listing, and deleting users.

### Create User

```bash title="Usage"
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

```bash title="Usage"
bin/uc user delete [options]
```

_Required Params:_

    --id The unique id of the user

_Optional Params:_

    --server UC Server to connect to. Default is reference server.
    --auth_token PAT token to authorize uc requests.
    --output To indicate CLI output format preference. Supported values are json and jsonPretty.

### Get User

```bash title="Usage"
bin/uc user get [options]
```

_Required Params:_

    --id The unique id of the user

_Optional Params:_

    --server UC Server to connect to. Default is reference server.
    --auth_token PAT token to authorize uc requests.
    --output To indicate CLI output format preference. Supported values are json and jsonPretty.

### List Users

```bash title="Usage"
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

```bash title="Usage"
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
