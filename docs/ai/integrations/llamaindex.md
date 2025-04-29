# 🦙 Using Unity Catalog AI with LlamaIndex

Integrate Unity Catalog AI with [LlamaIndex](https://docs.llamaindex.ai/en/stable/) to directly use UC functions as tools in LlamaIndex-based agent applications. This guide covers installation, client setup, and examples to get started.

---

## Installation

Install the Unity Catalog AI LlamaIndex integration from PyPI:

```sh
pip install unitycatalog-llamaindex
```

## Prerequisites

- **Python version**: Python 3.10 or higher is required.

>Note: Depending on what you're doing with LlamaIndex, you may need to install additional packages from PyPI.

### Unity Catalog

Ensure that you have a functional UC server set up and that you are able to access the catalog and schema where defined functions are stored.

### Databricks Unity Catalog

To interact with Databricks Unity Catalog, install the optional package dependency when installing the integration package:

```sh
pip install unitycatalog-llamaindex[databricks]
```

## Tutorial

### Client Setup

Create an instance of the Functions Client

```python
from unitycatalog.client import ApiClient, Configuration
from unitycatalog.ai.core.client import UnitycatalogFunctionClient

config = Configuration()
# This is the default address when starting a UnityCatalog server locally. Update this to the uri
# of your running UnityCatalog server.
config.host = "http://localhost:8080/api/2.1/unity-catalog"

# Create the UnityCatalog client
api_client = ApiClient(configuration=config)

# Use the UnityCatalog client to create an instance of the AI function client
client = UnitycatalogFunctionClient(api_client=api_client)
```

### Client Setup - Databricks

Create an instance of the Unity Catalog Functions client

``` python
from unitycatalog.ai.core.databricks import DatabricksFunctionClient

client = DatabricksFunctionClient()
```

### Creating a UC function

Create a Python function within Unity Catalog

``` python
CATALOG = "your_catalog"
SCHEMA = "your_schema"

func_name = f"{CATALOG}.{SCHEMA}.code_function"

def code_function(code: str) -> str:
    """
    Executes Python code.

    Args:
        code (str): The python code to execute.
    Returns:
        str: The result of the execution of the Python code.
    """
    import sys
    from io import StringIO
    stdout = StringIO()
    sys.stdout = stdout
    exec(code)
    return stdout.getvalue()

client.create_python_function(
    func=code_function,
    catalog=CATALOG,
    schema=SCHEMA
)
```

### Creating a toolkit instance

Here we create an instance of our UC function as a toolkit, then verify that the tool is behaving properly by executing the function.

``` python
from unitycatalog.ai.llama_index.toolkit import UCFunctionToolkit

# Create a UCFunctionToolkit that includes the UC function
toolkit = UCFunctionToolkit(function_names=[func_name])

# Fetch the tools stored in the toolkit
tools = toolkit.tools
python_exec_tool = tools[0]

# Execute the tool directly
result = python_exec_tool.call(code="print(1 + 1)")
print(result)  # Outputs: {"format": "SCALAR", "value": "2\n"}

```

### Using the tool in a LlamaIndex ReActAgent

With our interface to our UC function defined as a LlamaIndex tool collection, we can directly use it within a LlamaIndex agent application.
Below, we are going to create a simple `ReActAgent` and verify that our agent properly calls our UC function.

```python
from llama_index.llms.openai import OpenAI
from llama_index.core.agent import ReActAgent

llm = OpenAI()

agent = ReActAgent.from_tools(tools, llm=llm, verbose=True)

agent.chat("Please call a python execution tool to evaluate the result of 42 + 97.")
```
