# Client Guide

This guide provides detailed information about the Unity Catalog AI clients, including caveats, environment variables, public APIs, and examples to help you effectively utilize the Unity Catalog AI Core Library.

---

## Open Source Function Client

### UnityCatalog Client Overview

The `UnitycatalogFunctionClient` is a specialized client within the Unity Catalog AI Core Library designed for managing and executing functions in the open-source version of Unity Catalog (UC). Built on top of the asynchronous `unitycatalog-client`, this client provides both asynchronous and synchronous interfaces to interact seamlessly with UC functions. It facilitates the creation, retrieval, listing, execution, and deletion of UC functions, enabling developers to integrate both CRUD and execution capabilities into GenAI authoring workflows easily.

#### Caveats

When using the `UnitycatalogFunctionClient` be aware of the following points:

1. **Asynchronous API usage**: The `unitycatalog-client` SDK is based on aiohttp, making it an async client. The `UnitycatalogFuncitonClient` exposes async methods for nearly all of its interfaces (except for `execute_function`) to provide seamless experience for asynchronous calls to a Unity Catalog server. If you are using asynchronous methods within an environment that has a running event loop (such as a Jupyter Notebook), ensure that you are not attempting to create additional event loops for async method calls. Additionally, all methods within `unitycatalog-ai` offer **synchronous** versions, in case you prefer a simpler interface.
2. **Security Considerations**: Function execution occurs within the environment where a Unity Catalog function is called for execution. Be aware of the contents of any function prior to execution by checking the `routine_definition` entry by calling the `get_function` API if you did not author the function. Additionally, be aware that GenAI-generated Python code (for the use case of building a Unity Catalog function whose sole purpose is to execute arbitrary code) execution is inherently unsafe and should be performed in an isolated VM that is not connected to user data or is in an environment that is not ephemeral (a VM is ideal for this use case).
3. **External Dependencies**: The environment that is executing the function that you're calling needs to have the appropriate packages pre-installed in order for functions that use external non-core-Python import statements. Ensure that dependent libraries are installed via `pip`. Additionally, import statements should be contained within the function body (using local imports) to ensure that the dependency is available for use by your function's internal logic.
4. **Function overwriting**: The `create_function` and `create_function_async` APIs expose the ability to overwrite a function definition for convenience. Ensure that you are fully aware of the implications of replacing a function as this operation is permanent and could be disruptive to other Agent workflows that are dependent on existing functionality.

#### Key Features

- **Asynchronous and Synchronous Operations**: Offers both async and sync methods, allowing flexibility based on the application's concurrency requirements.
- **Function Management**: Simplifies the process of creating, retrieving, listing, executing, and deleting UC functions.
- **Integration with GenAI**: Supports the registration of UC functions as tools for Generative AI agents, enabling intelligent tool calling within AI-driven workflows.
- **Type Validation and Caching**: Ensures that function parameters and return types adhere to defined schemas and caches function executions for optimized performance.

### Using the UnityCatalog Functions Client

**Warning**: Function execution for the OSS UnityCatalog APIs involves executing code locally from the point of invocation of
your GenAI application. For deterministic functions that you are authoring yourself, this does not pose a security concern.
However, if you are writing an application that allows for code to be injected to the function from an LLM
(a Python code execution function), there is a risk to the environment that you are running your agent from if the code being
executed is unsafe (involving file system operations or accessing networks). If your use case involves such a function declaration that permits execution of arbitrary Python code, it is **highly advised** to run your Agent from an isolated environment
with restricted permissions.

**Note**: Future development efforts may involve the creation of a secure function execution environment to eliminate the risks
associated with GenAI-generated Python code execution from within Unity Catalog functions.

### Using the Client for Agent tool calling

#### Installation

- **Installation**: ensure that you have the following packages installed from PyPI:

```shell
pip install unitycatalog-client unitycatalog-ai unitycatalog-openai aiohttp nest_asyncio
```

#### Initialization

Configure and instantiate the Unity Catalog Python Client to connect to your running Unity Catalog server:

```python
import asyncio
from unitycatalog.ai.core.oss import UnitycatalogFunctionClient
from unitycatalog.client import ApiClient, Configuration

config = Configuration()
config.host = "http://localhost:8080/api/2.1/unity-catalog"

# The base ApiClient is async
api_client = ApiClient(configuration=config)

uc_client = UnitycatalogFunctionClient(uc=api_client)

CATALOG = "my_catalog"
SCHEMA = "my_schema"
```

> Tip: Ensure that you have created a catalog and a schema before attempting to create functions. The `ApiClient` via
the `unitycatalog-client` package can be used to instantiate the `CatalogApi` and the `SchemaApi` via async calls for
creating both a catalog and a schema respectively.

#### Creating Functions for tool use

In order to make a Python callable recognizable to the Unity Catalog functions data model, there are a few requirements that need to be met in the authoring of your functions:

1. The function must use type hints in both the signature and the return of the function.
2. A Google-style docstring must be used. A description of the function is required and descriptions of the parameters are strongly recommended for proper use by GenAI agents.
3. Dependent libraries must be imported within the body of the function. Imports outside of the function will not be resolvable when executing otherwise.

Below, we show a proper function definition (with type hints and docstring) that is then used to register the function to Unity Catalog using the async API for creating functions.

```python
def my_test_func(a: str, b: str) -> str:
    """
    Returns an upper case concatenation of two strings separated by a space.

    Args:
        a: the first string
        b: the second string

    Returns:
        Uppercased concatenation of the two strings.
    """
    concatenated = f"{a} {b}"
    return concatenated.upper()

# Asynchronously create the function within a Jupyter notebook
my_function = await uc_client.create_python_function_async(
    func=my_test_func,
    catalog=CATALOG,
    schema=SCHEMA,
    replace=True,
)
```

> Note: within a Python script, you will need to directly call `asyncio.run()` on your async API call
in order to create an event loop. In a Jupyter Notebook environment, an event loop is provided for you.

Alternatively, you can use the synchronous API:

```python
my_function = uc_client.create_python_function(
    func=my_test_func,
    catalog=CATALOG,
    schema=SCHEMA,
    replace=True,
)
```

#### Testing a function

Before attempting to use the function within an Agent integration, you can validate that the function is behaving as
intended by using the `execute_function` API. Unlike with the variants of the `create_python_function` API, there is **not** an async version of this API.

Testing with the synchronous API:

```python
result = uc_client.execute_function(
    function_name=f"{CATALOG}.{SCHEMA}.my_test_func",
    parameters={"a": "hello", "b": "world"}
)

result.value  # Outputs: HELLO WORLD
```

#### Building and using a toolkit instance with an OpenAI Agent

In order for a supported GenAI library to use our function, we need to wrap it in a `UCFunctionToolkit` instance. These abstractions exist for all supported integrations within the `unitycatalog-ai` ecosystem and permit their respective integration package to directly interact with the functions that you define within Unity Catalog.

For example, creating a `UCFunctionToolkit` instance for integrating with OpenAI is done as follows:

```python
import openai
from unitycatalog.ai.openai.toolkit import UCFunctionToolkit

# Assume we have two functions for our agent:
function_1 = f"{CATALOG}.{SCHEMA}.calculate_dew_point"
function_2 = f"{CATALOG}.{SCHEMA}.calculate_vapor_pressure_deficit"

toolkit = UCFunctionToolkit(
    client=uc_client,
    function_names=[function1, function2]
)

agent_tools = toolkit.tools

message = [
    {
        "role": "system",
        "content": "You are an assistant that provides accurate assessments of atmospheric conditions based on questions asked."
    },
    {
        "role": "user",
        "content": "It's 98.9F today with 60 percent humidity. After it rains this afternoon, how long will it be before the haze lifts?"
    }
]

tool_call_response = openai.chat.completions.create(
    model="gpt-4o-mini",
    messages=message,
    tools=agent_tools,
)
```

The response from calling `gpt-4o-mini` will show a request for the execution of the functions that we provided in order to answer the original question.

We can use the `generate_tool_call_messages` utility API within `unitycatalog-openai` to process this request, execute the functions, and generate the appropriate interface structure for send back to the LLM for the final processing.

```python
from unitycatalog.ai.openai.utils import generate_tool_call_messages

response_message = generate_tool_call_messages(response=tool_call_response, client=uc_client)

# merge the conversation history for context
final_message = message + response_message

answer = openai.chat.completions.create(
    model="gpt-4o-mini",
    messages=final_message,
    tools=agent_tools,
)
```

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
- **Supported Types**: Not all Python types are supported in SQL. Refer to the [Python to SQL Compatibility Matrix](usage.md#python-to-sql-compatibility-matrix) for supported types.
- **Docstrings**: Use Google-style docstrings to provide metadata for functions. This enhances function discoverability and usability in GenAI applications.

---

## Client Public APIs

### Creating functions from a Python Callable

The UC Functions Client's `create_python_function` API provides a high-level Python-native approach to creating a UC function.
This API automates the conversion process between a Python function's definition and the required syntax of a SQL Body statement by
parsing the contents of your function definition and extracting the relevant information.

#### Caveats of Python Callable function creation

- **type hints**: All declared arguments and the return of the function must use valid type hints. Failing to define these hints will raise an Exception.
- **type hints in collections**: If you are using a collection (`tuple`, `list`, or `dict`), all internal types **must be defined**. Generic collections are not permitted.
- **generics**: the `Any` type is not supported. All types must be concrete.
- **defaults**: Default values that are defined in your function signature will be extracted as UC function `DEFAULT` entries in your function. Ensure that the default values are valid for your function and that your type hints are correct (using `Optional[<type>]`).
- **docstring formatting**: In order for relevant information to be extracted from the docstring, the syntax of your docstring must match the [Google Docstring](https://sphinxcontrib-napoleon.readthedocs.io/en/latest/example_google.html) conventions. `reST`, `Epytext` and `Numpydoc` style docstrings are **not supported**.
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
