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
    - The `create_function` and `create_function_async` methods allow overwriting existing functions by setting the replace parameter to True.
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

Note that function execution occurs in the main process of where you are calling this API from. Read the notes above about security considerations for unknown code execution before calling this API.

```python
full_func_name = f"{CATALOG}.{SCHEMA}.add_numbers"
parameters = {"a": 10.5, "b": 5.5}

# Or synchronously
result = uc_client.execute_function(full_func_name, parameters)

print(result.value)  # Outputs: 16.0
```

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
  Install databricks-connect package with `pip install databricks-connect==15.1.0` to access serverless compute. **python>=3.10** is a requirement to install this version of the package.
- For executing the UC functions within Databricks, use either SQL warehouse or Databricks Connect with serverless:
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
|                                                   | `UCAI_DATABRICKS_WAREHOUSE_EXECUTE_FUNCTION_BYTE_LIMIT`              | Maximum byte size of the function execution result. If truncated due to this limit, the `truncated` field in the response is set to `true`.                                                                                                                | 1048576       |
|                                                   | `UCAI_DATABRICKS_WAREHOUSE_RETRY_TIMEOUT`                            | Client-side retry timeout for function execution. If execution doesn't complete within `UCAI_DATABRICKS_WAREHOUSE_EXECUTE_FUNCTION_WAIT_TIMEOUT`, client retries with exponential wait times until this timeout is reached.                                  | 120           |
| **Serverless Compute Execution**                  | `UCAI_DATABRICKS_SERVERLESS_EXECUTION_RESULT_ROW_LIMIT`              | Maximum number of rows in the function execution result.                                                                                                                                                                                                  | 100           |

#### Reminders

- If the function contains a `DECIMAL` type parameter, it is converted to python `float` for execution, and this conversion may lose precision.
