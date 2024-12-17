# Using Unity Catalog AI with the Gemini SDK

You can use the Unity Catalog AI package with the Gemini SDK to utilize functions that are defined in Unity Catalog to be used as tools within Gemini LLM calls.

## Installation

### Client Library

To use this package with **Unity Catalog**, you will need to install:

```sh
pip install unitycatalog-gemini
```

To use this package with **Databricks Unity Catalog**, you will need to install:

```sh
pip install unitycatalog-gemini[databricks]
```

## Getting started

### Creating a Unity Catalog Client

To interact with your Unity Catalog server, initialize the `UnitycatalogFunctionClient` as shown below:

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

### Creating a Unity Catalog Function

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

## Databricks-managed Unity Catalog

To use Databricks-managed Unity Catalog with this package, follow the [instructions](https://docs.databricks.com/en/dev-tools/cli/authentication.html#authentication-for-the-databricks-cli) to authenticate to your workspace and ensure that your access token has workspace-level privilege for managing UC functions.

### Client setup

Initialize a client for managing UC functions in a Databricks workspace, and set it as the global client.

```python
from unitycatalog.ai.core.base import set_uc_function_client
from unitycatalog.ai.core.databricks import DatabricksFunctionClient

client = DatabricksFunctionClient(
    warehouse_id="..." # replace with the warehouse_id
    cluster_id="..." # optional, only pass when you want to use cluster for function creation
)

# sets the default uc function client
set_uc_function_client(client)
```

### Create a Function in UC

Create a python UDF in Unity Catalog with the client

```python
# replace with your own catalog and schema
CATALOG = "catalog"
SCHEMA = "schema"

func_name = f"{CATALOG}.{SCHEMA}.python_exec"
# define the function body in SQL
sql_body = f"""CREATE OR REPLACE FUNCTION {func_name}(location STRING COMMENT 'Retrieves the current weather from a provided location.')
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'Returns the current weather from a given location and returns the temperature in degrees Celsius.'
AS $$
    return "31.9 C"
$$
"""

client.create_function(sql_function_body=sql_body)
```

Now that the function is created and stored in the corresponding catalog and schema, we can use it within Gemini's SDK.

## Using the Function as a GenAI Tool

### Create a UCFunctionToolkit instance

Tool use through the [Google GenAI SDK](https://ai.google.dev/gemini-api/docs) allows you to connect external client-side tools and
functions to provide [Gemini](https://ai.google.dev/gemini-api/docs/models/gemini-v2) with a greater range of capabilities to augment its ability to respond to user messages.

To begin, we will need an instance of the tool function interface from the `unitycatalog.ai.gemini` toolkit.

```python
from unitycatalog.ai.gemini.toolkit import UCFunctionToolkit

# Create an instance of the toolkit with the function that was created earlier.
toolkit = UCFunctionToolkit(function_names=[f"{CATALOG}.{SCHEMA}.python_exec"], client=client)

# Access the tool definitions that are in the interface that Gemini's SDK expects
tools = toolkit.generate_callable_tool_list()

```

Now that we have the defined tools from Unity Catalog, we can directly pass this definition into a messages request.

### Use the tools within a request to Gemini models

Gemini will generate a stopping condition of `"tool_use"` when a relevant tool definition is provided to a message creation call, responding with the
function's name to call and the input arguments to provide to the tool.

```python
# Interface with Gemini via their SDK
from google.generativeai import GenerativeModel

multi = "What's the weather in Nome, AK and in Death Valley, CA?"


model = GenerativeModel(
    model_name="gemini-2.0-flash-exp", tools=tools
)

chat = model.start_chat(enable_automatic_function_calling=True)

response = chat.send_message("What's the weather in Nome, AK and in Death Valley, CA?")
print(response)
```

### Showing Details of the Tool Call

```python
for content in chat.history:
    print(content.role, "->", [type(part).to_dict(part) for part in content.parts])
    print("-" * 80)
```

### Configurations for Databricks-only UC function execution

We provide configurations for the Databricks Client to control the function execution behaviors, check [function execution arguments section](../../README.md#function-execution-arguments-configuration).
