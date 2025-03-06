# Unity Catalog AI Core library

The Unity Catalog AI Core library provides convenient APIs to interact with Unity Catalog functions, including the creation, retrieval and execution of functions.
The library includes clients for interacting with both Unity Catalog servers and Databricks-managed Unity Catalog services, in support of UC functions as tools in agents.

## Installation

```sh
pip install unitycatalog-ai
```

If you are using the Databricks-managed version of Unity Catalog, you can install the optional additional Databricks dependencies by providing the option:

```sh
pip install unitycatalog-ai[databricks]
```

## Get started

### Unity Catalog Function Client

The Unity Catalog (UC) function client is a core component of the Unity Catalog AI Core Library, enabling seamless interaction with a Unity Catalog server. This client allows you to manage and execute UC functions, providing both asynchronous and synchronous interfaces to cater to various application needs. Whether you're integrating UC functions into GenAI workflows or managing them directly, the UC client offers robust and flexible APIs to facilitate your development process.

#### Key Features

- **Asynchronous and Synchronous Operations**: Flexibly choose between async and sync methods based on your application's concurrency requirements.
- **Comprehensive Function Management**: Easily create, retrieve, list, execute, and delete UC functions.
- **Wrapped Function Support**: In addition to standard single-function creation, you can create *wrapped functions* that in-line additional helper functions within a function's definition to simplify code reuse and modularity.
- **Integration with GenAI**: Seamlessly integrate UC functions as tools within Generative AI agents, enhancing intelligent automation workflows.
- **Type Safety and Caching**: Enforce strict type validation and utilize caching mechanisms to optimize performance and reduce redundant executions.

#### Caveats

When using the `UnitycatalogFunctionClient` for UC, be mindful of the following considerations:

- **Asynchronous API Usage**:
    - The `UnitycatalogFunctionClient` is built on top of the asynchronous [unitycatalog-client SDK](https://pypi.org/project/unitycatalog/), which utilizes aiohttp for REST communication with the UC server.
    - The function client for Unity Catalog offers **both asynchronous and synchronous methods**. The synchronous methods are wrappers around the asynchronous counterparts, ensuring compatibility with environments that may not support asynchronous operations.
    - **Important**: Avoid creating additional event loops in environments that already have a running loop (e.g., Jupyter Notebooks) to prevent conflicts and potential runtime errors.
- **Security Considerations**:
    - **WARNING** Function execution occurs **locally** within the environment where your application is running.
    - **Caution**: Executing GenAI-generated Python code can pose security risks, especially if the code includes operations like file system access or network requests.
    - **Recommendation**: Run your application in an isolated and secure environment with restricted permissions to mitigate potential security threats.
- **External Dependencies**:
    - Ensure that any external libraries required by your UC functions are pre-installed in the execution environment.
    - Best Practice: Import external dependencies within the function body to guarantee their availability during execution.
- **Function Overwriting**:
    - The `create_function`, `create_function_async`, `create_wrapped_function` and `create_wrapped_function_async` methods allow overwriting existing functions by setting the replace parameter to True.
    - **Warning**: Overwriting functions can disrupt workflows that depend on existing function definitions. Use this feature judiciously and ensure that overwriting is intentional.
- **Type Validation and Compatibility**:
    - The client performs strict type validation based on the defined schemas. Ensure that your function parameters and return types adhere to the expected types to prevent execution errors.

#### Prerequisites

Before using the UC functions client, ensure that your environment meets the following requirements:

- **Python Version**: Python 3.10 or higher is recommended to leverage all functionalities, including function creation and execution.

- **Dependencies**: Install the necessary packages using pip:

    ```sh
    pip install unitycatalog-client unitycatalog-ai
    ```

- **Unity Catalog Server**: Ensure that you have access to a running instance of the open-source Unity Catalog server. Follow the [Unity Catalog Installation Guide](https://docs.unitycatalog.io/quickstart/) to set up your server if you haven't already.

#### Client Initialization

To interact with UC functions, initialize the `UnitycatalogFunctionClient` as shown below:

```python
import asyncio
from unitycatalog.ai.core.client import UnitycatalogFunctionClient
from unitycatalog.client import ApiClient, Configuration

# Configure the Unity Catalog API client
config = Configuration(
    host="http://localhost:8080/api/2.1/unity-catalog"  # Replace with your UC server URL
)

# Initialize the asynchronous ApiClient
api_client = ApiClient(configuration=config)

# Instantiate the UnitycatalogFunctionClient
uc_client = UnitycatalogFunctionClient(api_client=api_client)

# Example catalog and schema names
CATALOG = "my_catalog"
SCHEMA = "my_schema"
```

#### Creating a UC Function

You can create a UC function either by providing a Python callable or by submitting a `FunctionInfo` object. Below is an example (recommended) of using the `create_python_function` API that accepts a Python callable (function) as input.

To create a UC function from a Python function, define your function with appropriate type hints and a Google-style docstring:

```python
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

# Create the function within the Unity Catalog catalog and schema specified
function_info = uc_client.create_python_function(
    func=add_numbers,
    catalog=CATALOG,
    schema=SCHEMA,
    replace=False,  # Set to True to overwrite if the function already exists
)

print(function_info)
```

#### Creating a Wrapped UC Function

In addition to standard function creation, you can create *wrapped functions*. A wrapped function uses a primary function as the interface while in-lining additional helper functions (wrapped functions) into the primary function’s definition. This feature is useful when you want to keep helper logic bundled together with the main function without needing to replicate existing common utilities within your function definitions.

For example, consider the following helper functions and the primary wrapper function that has direct dependencies on the helper functions:

```python
def a(x: int) -> int:
    return x + 1

def b(y: int) -> int:
    return y + 2

def wrapper(x: int, y: int) -> int:
    """
    Wrapper function that in-lines helper functions a and b.

    Args:
        x (int): The first argument.
        y (int): The second argument.

    Returns:
        int: The combined result of a(x) and b(y).
    """
    return a(x) + b(y)
```

To register this wrapped function as a single UC function, you can call the `create_wrapped_function` API:

```python
function_info = uc_client.create_wrapped_function(
    primary_func=wrapper,
    functions=[a, b],
    catalog=CATALOG,
    schema=SCHEMA,
    replace=False,  # Set to True to overwrite if the function already exists
)
```

#### Retrieving a UC Function

To retrieve details of a specific UC function, use the get_function method with the full function name in the format `<catalog>.<schema>.<function_name>`:

```python
full_func_name = f"{CATALOG}.{SCHEMA}.add_numbers"

# Retrieve the function information and metadata
function_info = uc_client.get_function(full_func_name)

print(function_info)
```

#### Listing Functions

```python
# List all created functions within a given schema
functions = uc_client.list_functions(
    catalog=CATALOG,
    schema=SCHEMA,
    max_results=10  # Paginated results will contain a continuation token that can be submitted with additional requests
)

for func in functions.items:
    print(func)
```

#### Executing a Function

Functions are executed by specifying a fully qualified function name to the `execute_function` or `execute_function_async` APIs. Integration packages
(Toolkit instances) will call this client API when a GenAI service indicates that a tool call is needed to fulfill a request.

The manual mode of executing a function via the client API is:

```python
full_func_name = f"{CATALOG}.{SCHEMA}.add_numbers"
parameters = {"a": 10.5, "b": 5.5}

# Async access
result = await uc_client.execute_function_async(full_func_name, parameters)

# Sync access
result = uc_client.execute_function(full_func_name, parameters)

print(result.value)  # Outputs: 16.0
```

There are two options for executing functions with the `UnitycatalogFunctionClient`:

##### Sandbox Mode

The `"sandbox"` option for callable execution allows for several enhanced security measures and system stability features that are not available
in the within-main-process execution mode of `"local"`. It is the default configuration for instances of `UnitycatalogFunctionClient`.

The sandbox mode offers:

- Isolated process execution
- Restrictions on total CPU runtime of the callable execution (to protect against computationally excessive functions)
- Restrictions on virtual memory allocated to the process running the callable (only available on Linux)
- Total wall-clock based timeout protection

These configurations can be controlled by setting the following environment variables (listed with their defaults):

| Environment Variable          | Default Value | Description                                             |
|-------------------------------|---------------|---------------------------------------------------------|
| `EXECUTOR_MAX_CPU_TIME_LIMIT` | 10 (seconds)  | Maximum allowable CPU execution time                    |
| `EXECUTOR_MAX_MEMORY_LIMIT`   | 100 (MB)      | Maximum allowable Virtual Memory allocation for process |
| `EXECUTOR_TIMEOUT`            | 20 (seconds)  | Maximum Total wall clock time                           |
| `EXECUTOR_DISALLOWED_MODULES` | (see below)   | A list of blocked library imports                       |

Note that the maximum CPU time limit is not based on wall clock time; rather, it is the time that the CPU has spent at 100% allocation working on executing
the callable. Based on system scheduling and concurrent process activity, this is almost never equal to wall clock time and is in reality longer in duration
than the wall clock execution time.

There are restrictions in which packages can be imported for use within a sandbox environment.

The following imports are not permitted:

- `sys`
- `subprocess`
- `ctypes`
- `socket`
- `importlib`
- `pickle`
- `marshall`
- `shutil`

If you want to customize the allowed package imports, you can override the entire list by submitting a list of standard package names to the
environment variable `EXECUTOR_DISALLOWED_MODULES` (must be a list[str]).

In addition, callables executed within the sandbox environment do not have access to the built-in file `open` command.

If your function requires access to these modules or needs to have access to the local operating system's file store, use the `"local"` mode of
execution instead.

##### Local Mode

When creating an instance of a `UnitycatalogFunctionClient` you can specify the `execution_mode` as `"local"` to run your function in the main
process in which you are calling the `execute_function` API.

Local execution mode has no restrictions regarding allowable imports or the ability to access local file system directories and files, unlike the
`"sandbox"` option. However, the sandbox mode is recommended in order to gain the stability benefits of isolated process execution, CPU and memory
limits for callable execution, and the inability to use potentially dangerous libraries within function calls (i.e., `sys`, `shutil`, `marshall`, `subprocess`)

Read the notes above about security considerations for unknown code execution before calling this API.

To configure the client to use `"local"` mode, you can instantiate your client as follows:

```python
import asyncio
from unitycatalog.ai.core.client import UnitycatalogFunctionClient
from unitycatalog.client import ApiClient, Configuration

# Configure the Unity Catalog API client
config = Configuration(
    host="http://localhost:8080/api/2.1/unity-catalog"  # Replace with your UC server URL
)

# Initialize the asynchronous ApiClient
api_client = ApiClient(configuration=config)

# Instantiate the UnitycatalogFunctionClient
uc_client = UnitycatalogFunctionClient(api_client=api_client, execution_mode="local")
```

#### Function Parameter Defaults

Defining and executing functions with parameter defaults behave similarly to standard Python function argument defaults. If a parameter is not provided that is marked as having a default value when called via the `execute_function` API, the existing default parameter value will be mapped to the function invocation call.

If using defaults in your function signatures, ensure that the descriptions are accurate and declare what the default value is to ensure that Agentic use of your function is accurate.

#### Deleting a Function

To delete a function that you have write authority to, you can use the following API:

```python
full_func_name = f"{CATALOG}.{SCHEMA}.add_numbers"

uc_client.delete_function(full_func_name)
```

### Databricks-managed UC

To use Databricks-managed Unity Catalog with this package, follow the [instructions](https://docs.databricks.com/en/dev-tools/cli/authentication.html#authentication-for-the-databricks-cli) to authenticate to your workspace and ensure that your access token has workspace-level privilege for managing UC functions.

#### Prerequisites

- **[Highly recommended]** Use python>=3.10 for accessing all functionalities including function creation and function execution.
- For creating UC functions with a SQL body definition, **only [serverless compute](https://docs.databricks.com/en/compute/use-compute.html#use-serverless-compute) is supported**.
  Install databricks-connect package with `pip install databricks-connect>=15.1.0` to access serverless compute. **python>=3.10** is a requirement to install this version of the package.
- For executing the UC functions within Databricks, use Databricks Connect with serverless:
    NOTE: **only `serverless` [SQL warehouse type](https://docs.databricks.com/en/admin/sql/warehouse-types.html#sql-warehouse-types) is supported** because of performance concerns.
    - Databricks connect with serverless: Install databricks-connect package with `pip install databricks-connect>=15.1.0`. No config needs to be passed when initializing the client.

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

#### Dependencies and Environments

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

#### Retrieve a UC function

The client also provides API to get the UC function information details. Note that the function name passed in must be the full name in the format of `<catalog>.<schema>.<function_name>`.

```python
full_func_name = f"{CATALOG}.{SCHEMA}.{func_name}"
client.get_function(full_func_name)
```

#### Retrieving a UC function callable

There are two primary ways of retrieving a function definition in native Python Callable format from Unity Catalog; one designed for use of the callable, and one designed for debugging.

##### Fetch a Python Callable directly

The `get_function_as_callable` API is used to retrieve a recreated callable from a registered Unity Catalog Python function.
The return type is directly usable:

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

my_callable = client.get_function_as_callable(function_name=f"{CATALOG}.{SCHEMA}.sample_python_func)

# Use the callable directly
my_callable(2, 4)
```

This API exposes 2 additional parameters:

- `register_function`: boolean value that determines whether to register the recreated function to the global (or, if provided, a custom) namespace.
- `namespace`: A dict representing a namespace definition that can be used to register the recreated function to if a global scope is not desired for function reference usage.

##### Fetch a Python Callable as a string

The `get_function_source` API is used to retrieve a recreated python callable definition (as a string) from a registered Unity Catalog Python function.
In order to use this API, the function that you are fetching **must be** an `EXTERNAL` (python function) type function. When called, the function's metadata will
be retrieved and the structure of the original callable will be rebuilt and returned as a string.

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
my_func_def = client.get_function_source(function_name=f"{CATALOG}.{SCHEMA}.sample_python_function")
```

The returned value from the `get_function_source` API will be the same as the original input with a few caveats:

- `tuple` types will be cast to `list` due to the inability to express a Python `tuple` within Unity Catalog
- The docstring of the original function will be stripped out. Unity Catalog persists the docstring information in the logged function and it is available in the return of the `get_function` API call if needed.
- Collection types for open source Unity Catalog will only capture the outer type (i.e., `list` or `dict`) as inner collection type metadata is not preserved
within the `FunctionInfo` object. In Databricks, full typing is supported for collecitons.

The result of calling the `get_function_source` API on the `sample_python_func` registered function will be (when printed):

```text
def sample_python_func(a: int, b: int) -> int:
    """
    Returns the sum of a and b.

    Args:
        a: an int
        b: another int

    Returns:
        int
    """
    return a + b
```

Note: If you want to convert the extracted string back into an actual Python callable, you can use the utility `load_function_from_string` in the module `unitycatalog.ai.core.utils.execution_utils`. See below for further details on this API.

This API is useful for extracting already-registered functions that will be used as additional in-line calls within another function through the use of the `create_wrapped_python_function` API, saving the effort required to either hand-craft a function definition or having to track down where the original implementation of a logged function was defined.

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

Functions are executed by specifying a fully qualified function name to the `execute_function` API. Integration packages
(Toolkit instances) will call this client API when a GenAI service indicates that a tool call is needed to fulfill a request.

There are two options for executing functions with the `DatabricksFunctionClient`:

##### Serverless Mode

```python
client = DatabricksFunctionClient(execution_mode="serverless")
```

The `"serverless"` option for callable execution allows for enhanced remote callable execution via a SQL Serverless endpoint, keeping your
agent's process free from the burden or security risks associated with arbitrary code execution locally. This is the default configuration
of the `DatabricksFunctionClient` and is highly recommended for production use cases.

When your agent requests a tool to be executed, a request will be made with the appropriate function name and the parameters to pass in order
to successfully execute the function. This remote code execution helps to ensure that callables with excessive computational complexity will not
impact the functionality of your agent or impact the VM that it is running within.

##### Local Mode

```python
client = DatabricksFunctionClient(execution_mode="local")
```

To help simplify development, the `"local"` execution mode is available. This mode of operation allows the `DatabricksFunctionClient` to utilize
a local subprocess to execute your tool calls without having to make a request to a SQL serverless endpoint. It can be benficial when debugging
agents and their tool calls to have a local stack trace for debugging. However, there are some restrictions on the content of callables within this mode.

The `"local"` mode offers:

- Restrictions on total CPU runtime of the callable execution (to protect against computationally excessive functions)
- Restrictions on virtual memory allocated to the process running the callable (only available on Linux)
- Total wall-clock based timeout protection

These configurations can be controlled by setting the following environment variables (listed with their defaults):

| Environment Variable          | Default Value | Description                                             |
|-------------------------------|---------------|---------------------------------------------------------|
| `EXECUTOR_MAX_CPU_TIME_LIMIT` | 10 (seconds)  | Maximum allowable CPU execution time                    |
| `EXECUTOR_MAX_MEMORY_LIMIT`   | 100 (MB)      | Maximum allowable Virtual Memory allocation for process |
| `EXECUTOR_TIMEOUT`            | 20 (seconds)  | Maximum Total wall clock time                           |

Note that the maximum CPU time limit is not based on wall clock time; rather, it is the time that the CPU has spent at 100% allocation working on executing
the callable. Based on system scheduling and concurrent process activity, this is almost never equal to wall clock time and is in reality longer in duration
than the wall clock execution time.

#### Execute a UC Python function locally

A utility `load_function_from_string` is available in `unitycatalog.ai.core.utils.execution_utils.py`. This utility allows you to couple the functionality
in the `get_function_source` API to create a locally-available python callable that can be direclty accessed, precisely as if it were originally defined
within your current REPL.

```python
from unitycatalog.ai.core.utils.execution_utils import load_function_from_string

func_str = """
def multiply_numbers(a: int, b: int) -> int:
    \"\"\"
    Multiplies two numbers.

    Args:
        a: first number.
        b: second number.

    Returns:
        int
    \"\"\"
    return a * b
"""

# If specifying `register_global=False`, the original function name cannot be called and must be used
# with the returned callable reference.
my_new_multiplier = load_function_from_string(func_str, register_global=False)
my_new_multiplier(a=1, b=2)  # returns `2`

# Alternatively, if allowing for global reference `register_global=True` (default)
# The original callable name can be used. This will not work in interactive environments like Jupyter.
load_function_from_string(func_str)
multiply_numbers(a=2, b=2)  # returns `4`

# For interactive environments, setting the return object directly within globals() is required in order
# to utilize the original function name
alias = load_function_from_string(func_str)
globals()["multiply_numbers"] = alias
multiply_numbers(a=3, b=3)  # returns `9`

# Additionally, a scoped namespace can be provided to restrict scope and access to scoped arguments
from types import SimpleNamespace

func_str2 = """
def multiply_numbers_with_constant(a: int, b: int) -> int:
    \"\"\"
    Multiplies two numbers with a constant.

    Args:
        a: first number.
        b: second number.
    
    Returns:
        int
    \"\"\"
    return a * b * c
"""

c = 100  # Not part of the scoped namespace; local constant

scoped_namespace = {
    "__builtins__": __builtins__,
    "c": 42,
}
    
load_function_from_string(func_str, register_function=True, namespace=scoped_namespace)

scoped_ns = SimpleNamespace(**scoped_namespace)

scoped_ns.multiply_numbers_with_constant(a=2, b=3)  # returns 252, utilizing the `c` constant of the namespace

```

##### Function execution arguments configuration

To manage the function execution behavior using Databricks client under different configurations, we offer the following environment variables:

| Environment Variable                                                | Description                                                                                                                                                                     | Default Value |
|---------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|
| `UCAI_DATABRICKS_SESSION_RETRY_MAX_ATTEMPTS`                        | Maximum number of attempts to retry refreshing the session client in case of token expiry.                                                               | 5           |
| `UCAI_DATABRICKS_SERVERLESS_EXECUTION_RESULT_ROW_LIMIT`             | Maximum number of rows when executing functions using serverless compute with `databricks-connect`.                                                                              | 100           |
                         | 100           |

#### Reminders

- If the function contains a `DECIMAL` type parameter, it is converted to python `float` for execution, and this conversion may lose precision.
