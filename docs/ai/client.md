# Client Guide

This guide provides detailed information about the Unity Catalog AI clients, including caveats, environment variables, public APIs, and examples to help you effectively utilize the Unity Catalog AI Core Library.

---

## Unity Catalog Function Client

### Client Overview

The `UnitycatalogFunctionClient` is a specialized client within the Unity Catalog AI Core Library designed for managing and executing functions within Unity Catalog (UC). Built on top of the asynchronous `unitycatalog-client`, this client provides both asynchronous and synchronous interfaces to interact seamlessly with UC functions. It facilitates the creation, retrieval, listing, execution, and deletion of UC functions, enabling developers to integrate both CRUD and execution capabilities into GenAI authoring workflows easily.

#### Caveats

When using the `UnitycatalogFunctionClient` be aware of the following points:

1. **Asynchronous API usage**: The `unitycatalog-client` SDK is based on aiohttp, making it an async client. The `UnitycatalogFuncitonClient` exposes async methods for nearly all of its interfaces (except for `execute_function`) to provide seamless experience for asynchronous calls to a Unity Catalog server. If you are using asynchronous methods within an environment that has a running event loop (such as a Jupyter Notebook), ensure that you are not attempting to create additional event loops for async method calls. Additionally, all methods within `unitycatalog-ai` offer **synchronous** versions, in case you prefer a simpler interface.
2. **Security Considerations**: Function execution occurs within the environment where a Unity Catalog function is called for execution. Be aware of the contents of any function prior to execution by checking the `routine_definition` entry by calling the `get_function` API if you did not author the function. Additionally, be aware that GenAI-generated Python code (for the use case of building a Unity Catalog function whose sole purpose is to execute arbitrary code) execution is **inherently unsafe and should be performed in an isolated VM** that is not connected to user data or is in an environment that is not ephemeral (a VM is ideal for this use case).
3. **External Dependencies**: The environment that is executing the function that you're calling needs to have the appropriate packages pre-installed in order for functions that use external non-core-Python import statements. Ensure that dependent libraries are installed via `pip`. Additionally, import statements should be contained within the function body (using local imports) to ensure that the dependency is available for use by your function's internal logic.
4. **Function overwriting**: The `create_function` and `create_function_async` APIs expose the ability to overwrite a function definition for convenience. Ensure that you are fully aware of the implications of replacing a function as this operation is permanent and could be disruptive to other Agent workflows that are dependent on existing functionality.

#### Key Features

- **Asynchronous and Synchronous Operations**: Offers both async and sync methods, allowing flexibility based on the application's concurrency requirements.
- **Function Management**: Simplifies the process of creating, retrieving, listing, executing, and deleting UC functions.
- **Integration with GenAI**: Supports the registration of UC functions as tools for Generative AI agents, enabling intelligent tool calling within AI-driven workflows.
- **Type Validation and Caching**: Ensures that function parameters and return types adhere to defined schemas and caches function executions for optimized performance.
- **Wrapped Function Creation**: Registers wrapped functions by in-lining helper functions into a primary function’s definition. The primary function serves as the interface for function execution while helper functions are bundled into the definition, promoting modular design and ease of deployment.

### Using the UnityCatalog Functions Client

**Warning**: Function execution for the UnityCatalog APIs involves executing code locally from the point of invocation of
your GenAI application. For deterministic functions that you are authoring yourself, this does not pose a security concern.
However, if you are writing an application that allows for code to be injected to the function from an LLM
(a Python code execution function), there is a risk to the environment that you are running your agent from if the code being
executed is unsafe (involving file system operations or accessing networks). If your use case involves such a function declaration that permits execution of arbitrary Python code, it is **highly advised** to run your Agent from an isolated environment
with restricted permissions.

**Note**: Future development efforts may involve the creation of a secure function execution environment to eliminate the risks
associated with GenAI-generated Python code execution from within Unity Catalog functions.

### Using the Client for Agent tool calling

#### Installation

- **Installation**: ensure that you have the following package installed from PyPI:

```shell
pip install unitycatalog-openai
```

#### Initialization

Configure and instantiate the Unity Catalog Python Client to connect to your running Unity Catalog server:

```python
import asyncio
from unitycatalog.ai.core.client import UnitycatalogFunctionClient
from unitycatalog.client import ApiClient, Configuration

config = Configuration()
config.host = "http://localhost:8080/api/2.1/unity-catalog"

# The base ApiClient is async
api_client = ApiClient(configuration=config)

uc_client = UnitycatalogFunctionClient(api_client=api_client)

CATALOG = "my_catalog"
SCHEMA = "my_schema"
```

##### Catalog and Schema Handlers

As a measure of convenience, a catalog and schema handler class is available. This can either be instantiated independently:

```python
from unitycatalog.ai.core.client import UnitycatalogClient
from unitycatalog.client import ApiClient, Configuration

config = Configuration(host="http://localhost:8080/api/2.1/unity-catalog")
api_client = ApiClient(configuration=config)

# Instantiate the catalog and schema handler directly to create catalogs and schemas as needed
core_client = UnitycatalogClient(api_client=api_client)

catalog_info = core_client.create_catalog(
    name="MyTestCatalog",
    comment="A catalog used for testing purposes",
    properties={"key": "value"},
)
schema_info = core_client.create_schema(
    name="MyTestSchema",
    catalog_name="MyTestCatalog",
    comment="A schema for testing",
    properties={"key": "value"},
)
```

Alternatively, these same APIs are available by accessing the `uc` property on the instance of `UnitycatalogFunctionClient`, as shown below.

> Tip: Ensure that you have created a catalog and a schema before attempting to create functions.

The `UnitycatalogFunctionClient` provides helper methods for creating both Catalogs and Schemas in Unity Catalog. For full
API-based CRUD operations, you will need to use the `unitycatalog-client` package.

To create a Catalog, you can call the `create_catalog` method:

```python
uc_client.uc.create_catalog(
    name=CATALOG,
    comment="A catalog for demonstrating the use of Unity Catalog function usage in GenAI applications",
)
```

> NOTE: If the Catalog already exists, you will receive a warning and the existing `CatalogInfo` metadata will be returned.

To create a Schema, you can call the `create_schema` method:

```python
uc_client.uc.create_schema(
    name=SCHEMA,
    catalog_name=CATALOG,
    comment="A schema for holding UC functions for GenAI use cases",
)
```

> NOTE: Similar to the `create_catalog` API, if a schema exists with the name that you specify, you will receive a warning
and the existing schema's metadata will be returned.

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
)
```

#### Creating Wrapped Functions

Wrapped functions are created using the `create_wrapped_function` and `create_wrapped_function_async` APIs. In a wrapped function, the primary function serves as the interface for callers, and additional helper functions are in-lined into the primary function’s definition. This bundles related functionality together and simplifies the registration process.

For example, consider the following definitions:

```python
def a(x: int) -> int:
    return x + 1

def b(y: int) -> int:
    return y + 2

def wrapper(x: int, y: int) -> int:
    """
    Calls the helper functions `a` and `b` and returns their combined result.

    Args:
        x (int): The first number.
        y (int): The second number.

    Returns:
        int: The sum of a(x) and b(y).
    """
    return a(x) + b(y)
```

The wrapped function is registered as follows:

```python
wrapped_function = uc_client.create_wrapped_function(
    primary_func=wrapper,
    functions=[a, b],
    catalog=CATALOG,
    schema=SCHEMA,
)
```

This API in-lines the helper functions (`a` and `b`) into the primary function (`wrapper`) so that they are part of the same function definition stored in Unity Catalog.

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
- **Serverless Compute**: In order to create and execute functions, a serverless compute connection **is required**.

### Dependencies and Environments

In Databricks runtime version 17 and higher, the ability to specify dependencies within a function execution environment is supported. Earlier runtime
versions do not support this feature and will error if the arguments `dependencies` or `environment` are submitted with a `create_python_function` or `create_wrapped_python_function` call.

To specify PyPI dependencies to include in your execution environment, you can see the minimum example below:

```python
# Define a function that requires an external PyPI dependency

def dep_check(x: str) -> str:
    """
    A function to test the dependency support for UC

    Args:
        x: An input string
    
    Returns:
        A string that reports the dependency support for UC
    """

    import scrapy  # NOTE that you must still import the library to use within the function.

    return scrapy.__version__

# Create the function and supply the dependency in standard PyPI format
client.create_python_function(func=dep_check, catalog=CATALOG, schema=SCHEMA, replace=True, dependencies=["scrapy==2.10.1"])
```

### Environment Variables for Databricks

You can configure the behavior of function execution using the following environment variables:

| Environment Variable                                                | Description                                                                                                                                                                     | Default Value |
|---------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|
| `UCAI_DATABRICKS_SESSION_RETRY_MAX_ATTEMPTS`                        | Maximum number of attempts to retry refreshing the session client in case of token expiry.                                                               | `5`         |
| `UCAI_DATABRICKS_SERVERLESS_EXECUTION_RESULT_ROW_LIMIT`             | Maximum number of rows when executing functions using serverless compute with `databricks-connect`.                                                                              | `100`         |

### Initialization

In order to perform CRUD operations and to execute UC functions, you will need to instantiate an instance of the UC functions Client. This client interface
is used not only for direct interface with functions, but also is the mechanism by which a function will be called as a tool by a GenAI application.

``` python
from unitycatalog.ai.core.databricks import DatabricksFunctionClient

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

> Note: the parameters `dependencies` and `environment_version` for the `create_python_function` API are only compatible with Databricks runtime versions that support these SQL parameters for function creation. Currently, this is Databricks runtime versions **17 and higher**.

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

> Note: Specifying environment and dependency configurations on version of Databricks runtime prior to verison 17 will generate exceptions if the SQL body contains these statements.

#### Specifying Custom Package Dependencies

If you are on Databricks Runtime **17 or above**, you can specify external package dependencies when writing your SQL body, as follows:

``` python
sql_body = """
CREATE FUNCTION my_catalog.my_schema.my_func()
RETURNS STRING
LANGUAGE PYTHON
ENVIRONMENT (dependencies='["pandas", "fastapi", "httpx"]', environment_version='1')
COMMENT 'A function that uses external additional libraries.'
AS $$
    import fastapi
    return fastapi.__version__
$$
"""
```

## Retrieving Functions

You can retrieve the function definition from UC by using the `get_function` client API:

``` python
function_info = client.get_function("your_catalog.your_schema.your_function_name")
```

## Retrieving a UC function callable

The `Databricks` client has a separate API that can be used for retrieving the base callable definition (as a string) of a registered Unity Catalog Python function.
In order to use this API, the function that you are fetching **must be** and `EXTERNAL` (python function) type function. When called, the function's metadata will
be retrieved and the structure of the original callable will be rebuilt and returned as a string.

Note: The UnityCatalogClient (non-Databricks) provides a utility function in the client module for recreating a callable, `dynamically_construct_python_function` which accepts as input the return of a `get_function` call. The `get_python_callable` API only exists in the `DatabricksFunctionClient` due to differences in how function execution works.

For example:

```python
# Define a python callable

def sample_python_func(a: int, b: int) -> int:
    """
    Returns the sum of a and b.

    Args:
        a: an int
        b: another int

    Returns:
        The sum of a and b
    """
    return a + b

# Create the function within Unity Catalog
client.create_python_function(catalog=CATALOG, schema=SCHEMA, func=sample_python_func, replace=True)

# Fetch the callable definition
my_func_def = client.get_python_callable(function_name=f"{CATALOG}.{SCHEMA}.sample_python_function")
```

The returned value from the `get_python_callable` API will be the same as the original input with a few caveats:

- `tuple` types will be cast to `list` due to the inability to express a Python `tuple` within Unity Catalog
- The docstring of the original function will be stripped out. Unity Catalog persists the docstring information in the logged function and it is available in the return of the `get_function` API call if needed.

The result of calling the `get_python_callable` API on the `sample_python_func` registered function will be (when printed):

```text
def sample_python_func(a: int, b: int) -> bigint:

    return a + b
```

Note: If you want to convert the extracted string back into an actual Python callable, you will need to use the `exec` function. A simple example:

```python
# Caution - evaluate the contents of any function definition before executing in this manner! 
local_namespace = {}
exec(my_func_def, globals(), local_namespace)
callable_fn = local_namespace["sample_ppython_func"]  # You must refer to the original name of the function
callable_fn(a=1, b=3)  # returns `4`
```

This API is useful for extracting already-registered functions that will be used as additional in-line calls within another function through the use of the `create_wrapped_python_function` API, saving the effort required to either hand-craft a function definition or having to track down where the original implementation of a logged function was defined.

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

## Function Parameter Defaults

Defining and executing functions with parameter defaults behave similarly to standard Python function argument defaults. If a parameter is not provided that is marked as having a default value when called via the `execute_function` API, the existing default parameter value will be mapped to the function invocation call.

If using defaults in your function signatures, ensure that the descriptions are accurate and declare what the default value is to ensure that Agentic use of your function is accurate.

---

## Examples

### Create and Execute a Function

``` python
from unitycatalog.ai.core.databricks import DatabricksFunctionClient

# Initialize the client, connecting to serverless
client = DatabricksFunctionClient()

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

### Additional Notes

- **Error Handling**: If a function execution fails, `FunctionExecutionResult` will contain an `error` attribute with details on the failure.
