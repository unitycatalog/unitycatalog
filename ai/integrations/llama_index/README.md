# 🦙 Using Unity Catalog AI with LlamaIndex

You can use functions defined within Unity Catalog (UC) directly as tools within [LlamaIndex](https://docs.llamaindex.ai/en/stable/) with this package.

## Installation

### Client Library

To install the Unity Catalog function client SDK and the `LlamaIndex` integration, simply install from PyPI:

```sh
pip install unitycatalog-llamaindex
```

If you are working with **Databricks Unity Catalog**, you can install the optional package:

```sh
pip install unitycatalog-llamaindex[databricks]
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

### Databricks-managed Unity Catalog

To use Databricks-managed UC with this package, follow the [instructions here](https://docs.databricks.com/en/dev-tools/cli/authentication.html#authentication-for-the-databricks-cli) to authenticate to your workspace and ensure that your access token has workspace-level privilege for managing UC functions.

#### Client setup

Initialize a client for managing UC functions in a Databricks workspace, and set it as the global client.

```python
from unitycatalog.ai.core.base import set_uc_function_client
from unitycatalog.ai.core.databricks import DatabricksFunctionClient

client = DatabricksFunctionClient()

# sets the default uc function client
set_uc_function_client(client)
```

#### Create a UC function

To provide an executable function for your tool to use, you need to define and create the function within UC. To do this,
create a Python function that is wrapped within the SQL body format for UC and then utilize the `DatabricksFunctionClient` to store this in UC:

```python
# Replace with your own catalog and schema for where your function will be stored
CATALOG = "catalog"
SCHEMA = "schema"

func_name = f"{CATALOG}.{SCHEMA}.python_exec"
# define the function body in UC SQL functions format
sql_body = f"""CREATE OR REPLACE FUNCTION {func_name}(code STRING COMMENT 'Python code to execute. Remember to print the final result to stdout.')
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'Executes Python code and returns its stdout.'
AS $$
    import sys
    from io import StringIO
    stdout = StringIO()
    sys.stdout = stdout
    exec(code)
    return stdout.getvalue()
$$
"""

client.create_function(sql_function_body=sql_body)
```

Now that the function exists within the Catalog and Schema that we defined, we can interface with it from llamaindex using the `unitycatalog.ai.llama_index` package.

## Using the Function as a GenAI Tool

### Create a UCFunctionToolkit instance

[LlamaIndex Tools](https://docs.llamaindex.ai/en/stable/module_guides/deploying/agents/tools/) are callable external functions that GenAI applications (called by
an LLM), which are exposed with a UC interface through the use of the `unitycatalog.ai.llama_index` package via the `UCFunctionToolkit` API.

```python
from unitycatalog.ai.llama_index.toolkit import UCFunctionToolkit

# Pass the UC function name that we created to the constructor
toolkit = UCFunctionToolkit(function_names=[func_name])

# Get the LlamaIndex-compatible tools definitions
tools = toolkit.tools
```

If you would like to validate that your tool is functional prior to proceeding to integrate it with LlamaIndex, you can call the tool directly:

```python
my_tool = tools[0]

my_tool.fn(**{"code": "print(1)"})

# or use the `call` API
my_tool.call(code="print(1)")
```

### Utilize our function as a tool within a ReActAgent in LlamaIndex

With our interface to our UC function defined as a LlamaIndex tool collection, we can directly use it within a LlamaIndex agent application.
Below, we are going to create a simple `ReActAgent` and verify that our agent properly calls our UC function.

```python
from llama_index.llms.openai import OpenAI
from llama_index.core.agent import ReActAgent

llm = OpenAI()

agent = ReActAgent.from_tools(tools, llm=llm, verbose=True)

agent.chat("Please call a python execution tool to evaluate the result of 42 + 97.")
```

### Configurations for Databricks managed UC functions execution

We provide configurations for databricks client to control the function execution behaviors, check [function execution arguments section](../../README.md#function-execution-arguments-configuration).
