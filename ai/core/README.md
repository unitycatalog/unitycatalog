# Unity Catalog AI Core library

The Unity Catalog AI Core library provides convenient APIs to interact with Unity Catalog functions, including the creation, retrieval and execution of functions.
The library includes clients for interacting with both the Open-Source Unity Catalog server and the Databricks-managed Unity Catalog service, in support of UC functions as tools in agents.

## Installation

```sh
# install from the source
pip install git+https://github.com/unitycatalog/unitycatalog.git#subdirectory=ai/core
```

> [!NOTE]
> Once this package is published to PyPI, users can install via `pip install unitycatalog-ai`

## Get started

### OSS UC

TODO: fill this section once OSS UC client implementation is done.

### Databricks-managed UC

To use Databricks-managed Unity Catalog with this package, follow the [instructions](https://docs.databricks.com/en/dev-tools/cli/authentication.html#authentication-for-the-databricks-cli) to authenticate to your workspace and ensure that your access token has workspace-level privilege for managing UC functions.

#### Prerequisites

- **[Highly recommended]** Use python>=3.10 for accessing all functionalities including function creation and function execution.
- Install databricks-sdk package with `pip install databricks-sdk`.
- For creating UC functions with SQL body, **only [serverless compute](https://docs.databricks.com/en/compute/use-compute.html#use-serverless-compute) is supported**.
  Install databricks-connect package with `pip install databricks-connect==15.1.0`, **python>=3.10** is a requirement to install this version.
- For executing the UC functions in Databricks, use either SQL warehouse or Databricks Connect with serverless:
  - SQL warehouse: create a SQL warehouse following [this instruction](https://docs.databricks.com/en/compute/sql-warehouse/create.html), and use the warehouse id when initializing the client.
    NOTE: **only `serverless` [SQL warehouse type](https://docs.databricks.com/en/admin/sql/warehouse-types.html#sql-warehouse-types) is supported** because of performance concerns.
  - Databricks connect with serverless: Install databricks-connect package with `pip install databricks-connect==15.1.0`. No config needs to be passed when initializing the client.

#### Client initialization

In this example, we use serverless compute as an example.

```python
from unitycatalog.ai.core.databricks import DatabricksFunctionClient

client = DatabricksFunctionClient()
```

#### Create a UC function

Create a UC function with SQL string should follow [this syntax](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-create-sql-function.html#create-function-sql-and-python).

```python
# make sure you have privilege in the corresponding catalog and schema for function creation
CATALOG = "..."
SCHEMA = "..."
func_name = "test"
sql_body = f"""CREATE FUNCTION {CATALOG}.{SCHEMA}.{func_name}(s string)
RETURNS STRING
LANGUAGE PYTHON
AS $$
  return s
$$
"""

function_info = client.create_function(sql_function_body=sql_body)
```

#### Retrieve a UC function

The client also provides API to get the UC function information details. Note that the function name passed in must be the full name in the format of `<catalog>.<schema>.<function_name>`.

```python
full_func_name = f"{CATALOG}.{SCHEMA}.{func_name}"
client.get_function(full_func_name)
```

#### List UC functions

To get a list of functions stored in a catalog and schema, you can use list API with wildcards to do so.

```python
client.list_functions(catalog=CATALOG, schema=SCHEMA, max_results=5)
```

#### Execute a UC function

Parameters passed into execute_function must be a dictionary that maps to the input params defined by the UC function.

```python
result = client.execute_function(full_func_name, {"s": "some_string"})
assert result.value == "some_string"
```

##### Function execution arguments configuration

To manage the function execution behavior using Databricks client under different configurations, we offer the following environment variables:

| Configuration Type                                | Environment Variable                                                | Description                                                                                                                                                                                                                                               | Default Value |
|---------------------------------------------------|---------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|
| **Warehouse Execution**                           | `UCAI_DATABRICKS_WAREHOUSE_EXECUTE_FUNCTION_WAIT_TIMEOUT`            | Time in seconds the call will wait for the function to execute. Set as `Ns` where `N` can be 0 or between 5 and 50.                                                                                                                                         | `30s`         |
|                                                   | `UCAI_DATABRICKS_WAREHOUSE_EXECUTE_FUNCTION_ROW_LIMIT`               | Maximum number of rows in the function execution result. Also sets the `truncated` field in the response to indicate if the result was trimmed due to the limit.                                                                                            | 100           |
|                                                   | `UCAI_DATABRICKS_WAREHOUSE_EXECUTE_FUNCTION_BYTE_LIMIT`              | Maximum byte size of the function execution result. If truncated due to this limit, the `truncated` field in the response is set to `true`.                                                                                                                | 4096          |
|                                                   | `UCAI_DATABRICKS_WAREHOUSE_RETRY_TIMEOUT`                            | Client-side retry timeout for function execution. If execution doesn't complete within `UCAI_DATABRICKS_WAREHOUSE_EXECUTE_FUNCTION_WAIT_TIMEOUT`, client retries with exponential wait times until this timeout is reached.                                  | 120           |
| **Serverless Compute Execution**                  | `UCAI_DATABRICKS_SERVERLESS_EXECUTION_RESULT_ROW_LIMIT`              | Maximum number of rows in the function execution result.                                                                                                                                                                                                  | 100           |

#### Reminders

- If the function contains a `DECIMAL` type parameter, it is converted to python `float` for execution, and this conversion may lose precision.
