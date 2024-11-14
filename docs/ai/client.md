# Client Guide

This guide provides detailed information about the Unity Catalog AI clients, including caveats, environment variables, public APIs, and examples to help you effectively utilize the Unity Catalog AI Core Library.

---

## Open Source Function Client

---

## Databricks Function Client

The `DatabricksFunctionClient` is a core component of the Unity Catalog AI Core Library that allows you to interact with Unity Catalog (UC) functions on Databricks. It provides APIs for creating, retrieving, listing, executing, and deleting UC functions.

### Caveats for Databricks

- **Python Version**: Python 3.10 or higher is **required** when using `databricks-connect` for serverless compute.
- **Databricks Connect**: To create UC functions using SQL body definitions or to execute functions using serverless compute, `databricks-connect` version `15.1.0` is required. This is the only supported version that is compatible.
- **Serverless Compute**: Function creation and execution using `databricks-connect` require serverless compute.
- **Warehouse**: If the `warehouse_id` is not provided during client initialization, `databricks-connect` with serverless compute will be used. 
    - Classic SQL Warehouses are not supported for function execution due to excessive latency, long startup times, and noticeable overhead with executing functions.
    Function creation can run on any Warehouse type.
    - The SQL Warehouse must be of a serverless type for function execution. To learn more about the different warehouse types, see [the docs](https://docs.databricks.com/en/admin/sql/warehouse-types.html).

### Environment Variables for Databricks

You can configure the behavior of function execution using the following environment variables:

| Environment Variable                                                | Description                                                                                                                                                                     | Default Value |
|---------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|
| `UCAI_DATABRICKS_WAREHOUSE_EXECUTE_FUNCTION_WAIT_TIMEOUT`           | Time in seconds to wait for function execution. Format: `Ns` where `N` is between 0 and 50. Setting to `0s` executes asynchronously.                                            | `30s`         |
| `UCAI_DATABRICKS_WAREHOUSE_EXECUTE_FUNCTION_ROW_LIMIT`              | Maximum number of rows in the function execution result.                                                                                                                        | `100`         |
| `UCAI_DATABRICKS_WAREHOUSE_EXECUTE_FUNCTION_BYTE_LIMIT`             | Maximum byte size of the function execution result. If exceeded, the `truncated` field in the response is set to `true`.                                                        | `4096`        |
| `UCAI_DATABRICKS_WAREHOUSE_RETRY_TIMEOUT`                           | Client-side retry timeout in seconds for function execution. If execution doesn't complete within `wait_timeout`, the client retries until this timeout is reached.             | `120`         |
| `UCAI_DATABRICKS_SERVERLESS_EXECUTION_RESULT_ROW_LIMIT`             | Maximum number of rows when executing functions using serverless compute with `databricks-connect`.                                                                              | `100`         |

### Initialization

In order to perform CRUD operations and to execute UC functions, you will need to instantiate an instance of the UC functions Client. This client interface
is used not only for direct interface with functions, but also is the mechanism by which a function will be called as a tool by a GenAI application.

``` python
from ucai.core.databricks import DatabricksFunctionClient

# Initialize with a warehouse ID (for executing functions using a SQL Warehouse)
client = DatabricksFunctionClient(warehouse_id="YOUR_WAREHOUSE_ID")

# Or initialize without a warehouse ID to use serverless compute with databricks-connect
client = DatabricksFunctionClient()
```

---

## Caveats

- **Type Annotations**: When creating functions from Python code, all parameters and return types must have type annotations.
- **Supported Types**: Not all Python types are supported in SQL. Refer to the [Python to SQL Compatibility Matrix](index.md#python-to-sql-compatibility-matrix) for supported types.
- **Docstrings**: Use Google-style docstrings to provide metadata for functions. This enhances function discoverability and usability in GenAI applications.

---

## Client Public APIs

## Creating Functions

### Creating functions from a Python Callable

The UC Functions Client's `create_python_function` API provides a high-level Python-native approach to creating a UC function.
This API automates the conversion process between a Python function's definition and the required syntax of a SQL Body statement by
parsing the contents of your function definition and extracting the relevant information.

#### Caveats of Python Callable function creation

- **type hints**: All declared arguments and the return of the function must use valid type hints. Failing to define these hints will raise an Exception.
- **type hints in collections**: If you are using a collection (`tuple`, `list`, or `dict`), all internal types **must be defined**. Generic collections are not permitted.
- **generics**: the `Any` type is not supported. All types must be concrete.
- **defaults**: Default values that are defined in your function signature will be extracted as UC function `DEFAULT` entries in your function. Ensure that the default values are valid for your function and that your type hints are correct (using `Optional[<type>]`).
- **docstring formatting**: In order for relevant information to be extracted from the docstring, the syntax of your docstring must match the [Google Docstring]() conventions. `reST`, `Epytext` and `Numpydoc` style docstrings are **not supported**.
- **external dependencies**: There is limited library support within the function execution environment. The list of supported available libraries [can be seen here](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-create-sql-function.html#supported-libraries-in-python-udfs) and is subject to change over time.
- **overwriting functions**: To replace an existing function, specify the argument `replace=True` when calling `create_python_function`.

#### Example of a Valid Function

``` python
def your_function_name(param: str) -> str:
    """
    Converts the input string to uppercase.

    Args:
        param (str): The input string.

    Returns:
        str: The uppercase string.
    """
    return param.upper()

client.create_python_function(
    func=your_function_name,
    catalog="your_catalog",
    schema="your_schema"
)

```

#### Example of an Invalid Function

The following function definition will not successfully be created as a UC function due to several reasons:

- Missing type annotations for arguments and for the return value.
- Missing docstring.

``` python
def invalid_func(a, b):
    return a + b

# This will raise a ValueError
client.create_python_function(
    func=invalid_func,
    catalog="your_catalog",
    schema="your_schema"
)
```

### Creating functions from a SQL Body statement

If you prefer to have full control over your function definition, the `create_function` API in the client allows you to submit your function definition as a [SQL Body statement](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-create-sql-function.html#syntax).

``` python
sql_body = """
CREATE FUNCTION your_catalog.your_schema.your_function_name(param STRING COMMENT 'A string to convert to uppercase.')
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'Converts an input string to uppercase.'
AS $$
    return param.upper()
$$
"""

client.create_function(sql_function_body=sql_body)
```

The general guidelines for function creation within UC apply to the SQL Body statement. There are no additional restrictions applied to this API.

> Note: It is highly recommended to provide verbose and accurate `COMMENT` blocks to both the function and the parameters configured. This information is
provided to LLMs that will be deciding on whether it is appropriate to call the defined tool. Detailed descriptions can reduce the chances of a tool call
failing due to ambiguous references on how to use the tool.

If you create a function without both a `COMMENT` block (the function description) and `COMMENT` entries for each parameter defined for your function,
a warning will be issued upon creation. It is **highly advised** to correct your function definition and overwrite your function when you see this warning.
Most LLM's will not be able to effectively use your defined function as a tool if the description is a placeholder or is lacking appropriate information
that describes the purpose of and how to use your defined function as a tool.

## Retrieving Functions

You can retrieve the function definition from UC by using the `get_function` client API:

``` python
function_info = client.get_function("your_catalog.your_schema.your_function_name")
```

## Listing Functions

You can list all functions that are defined within a given catalog and schema by using the `list_functions` client API:

``` python
functions = client.list_functions(catalog="your_catalog", schema="your_schema", max_results=10)
```

## Deleting Functions

A function can be deleted through the use of the `delete_function` client API:

``` python
client.delete_function("your_catalog.your_schema.your_function_name")
```

## Executing Functions

Executing a function directly using the client is done through the `execute_function` API:

``` python
result = client.execute_function(
    "your_catalog.your_schema.uppercase_function",
    parameters={"param": "hello world"}
)

print(result.value)  # Outputs: HELLO WORLD
```

---

## Examples

### Create and Execute a Function

``` python
from ucai.core.databricks import DatabricksFunctionClient

# Initialize the client
client = DatabricksFunctionClient(warehouse_id="YOUR_WAREHOUSE_ID")

# Define the function
def add_numbers(a: float, b: float) -> float:
    """
    Adds two numbers and returns the result.

    Args:
        a (float): First number.
        b (float): Second number.

    Returns:
        float: The sum of the two numbers.
    """
    return a + b

# Create the function in UC
client.create_python_function(
    func=add_numbers,
    catalog="your_catalog",
    schema="your_schema"
)

# Execute the function
result = client.execute_function(
    "your_catalog.your_schema.add_numbers",
    parameters={"a": 10.5, "b": 5.5}
)

print(result.value)  # Outputs: 16.0

```

### Using Environment Variables

Adjust the function execution timeout value by overriding the default via the environment variable. 

``` python
import os
from ucai.core.databricks import DatabricksFunctionClient

# Set the wait timeout to 50 seconds
os.environ["UCAI_DATABRICKS_WAREHOUSE_EXECUTE_FUNCTION_WAIT_TIMEOUT"] = "50s"

client = DatabricksFunctionClient(warehouse_id="YOUR_WAREHOUSE_ID")

# Execute your function as before
result = client.execute_function(
    "your_catalog.your_schema.your_function_name",
    parameters={"param": "test"}
)
```

### Additional Notes

- **Error Handling**: If a function execution fails, `FunctionExecutionResult` will contain an `error` attribute with details on the failure.
- **Asynchronous Execution**: Setting UCAI_DATABRICKS_WAREHOUSE_EXECUTE_FUNCTION_WAIT_TIMEOUT to 0s will execute the function asynchronously. The call will immediately return in async mode and the result will need to be polled and fetched for its completed state when executing in async mode.

