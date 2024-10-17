# Quickstart Guide

Welcome to the **Unity Catalog AI Core Library**! This guide will help you get started with installing the library, setting up your environment, and creating and executing Unity Catalog (UC) functions. Whether you're using your own UC open-source server or connecting with a Databricks-managed UC, this quickstart will walk you through the essential steps.

---

## Installation

Install the Unity Catalog AI Core library directly from PyPI:

```sh
pip install ucai-core
```

## Prerequisites

Before you begin, ensure you have the following:

- **Python Version**: Python 3.10 or higher is recommended for simplified type hint processing within the Unity Catalog functions client.

### Unity Catalog Open Source

If you're looking to use an Open Source Unity Catalog service, ensure that you have met all of the prerequisites and have followed the
[Unity Catalog Quickstart Guide](../quickstart.md). Once your server is up and running and you are able to create a catalog, schemas, and functions, you're all set to continue with this guide.

### Databricks Unity Catalog

If you are going to interface with ``Databricks Unity Catalog``, ensure that you have installed the ``Databricks SDK`` to enable access
to your Databricks Unity Catalog service.

```sh
pip install databricks-sdk
```

**Databricks Connect** (Optional): This package is required if you are going to create UC functions direclty using a SQL body definition.

```sh
pip install databricks-connect>=15.1.0
```

>Note: Python 3.10 or higher is required to use `databricks-connect` with serverless compute.

- **Serverless Compute**: For creating UC functions directly with a SQL body definition, only serverless compute is supported. You cannot run the
function creation APIs while attached to a classic SQL Warehouse.

- **SQL Warehouse**: Needed for executing UC functions.

Create a serverless SQL warehouse as per [this guide](https://docs.databricks.com/en/compute/sql-warehouse/create.html). After creating the warehouse,
note down the `warehouse id` for use within the `DatabricksFunctionClient`. 

## Using a function with LangChain

### Integration package installation

For this quick example with [LangChain](https://python.langchain.com/v0.2/docs/introduction/), we're first going to need to install the Unity Catalog
AI LangChain Integration package, `ucai-langchain`. This package contains the tool definition logic specifically for LangChain, allowing you to
seamlessly define a native LangChain tool that can interface with functions stored in Unity Catalog and be used in your GenAI applications.

``` sh
pip install ucai-langchain
```

### Client Setup

For us to be able to both create and execute a function defined within UC as a tool in LangChain, we need to initialize the UC function client.
In this example, we're connecting to Databricks UC and specifying a `warehouse_id` that will be used for executing the functions that are
defined as tools. When accessing functions by name, we will need to specify which catalog and schema the function resides in, so we're defining constants
to store those values.

``` python
from ucai.core.databricks import DatabricksFunctionClient

client = DatabricksFunctionClient(warehouse_id="YOUR_WAREHOUSE_ID")

CATALOG = "my_catalog"
SCHEMA = "my_schema"
```

### Create a function in UC

In order to define a tool for a GenAI framework to use, we first need a function to be created and available within UC.
There are two primary APIs for creating Python functions: `create_python_function` (accepts a Python callable) and `create_function`
(accepts a SQL body create function statement).

We'll use the `create_python_function` API to create our function. Keep in mind that there are some requirements for the successful
use of this API:

- **type hints**: The function signature must define valid Python type hints. Both the named arguments and the return value must have their types defined.
- **no variable arguments**: All arguments must be defined. `*args` and `**kwargs` are not permitted.
- **type compatibility**: Not all python types are supported in SQL. Ensure that your function is using compatible types according to the
 [compatibility matrix](index.md#python-to-sql-compatiblity-matrix).
- **docstring verbosity and formatting**: The UC functions toolkit will read, parse, and extract important information from your docstring. 
    - Docstrings must be formatted according to the [Google docstring syntax](https://sphinxcontrib-napoleon.readthedocs.io/en/latest/example_google.html) (shown below).
    - The more descriptive the function description and argument descriptions that your docstring provides, the greater the chances are that an LLM will understand when and how to use your function.

``` python
def add_numbers(number_1: float, number_2: float) -> float:
    """
    A function that accepts two floating point numbers, adds them,
    and returns the resulting sum as a float.

    Args:
        number_1 (float): The first of the two numbers to add.
        number_2 (float): The second of the two numbers to add.

    Returns:
        float: The sum of the two input numbers.
    """
    return number_1 + number_2

# Create the function in UC
function_info = client.create_python_function(
    func=add_numbers,
    catalog=CATALOG,
    schema=SCHEMA
)
```

### Create a LangChain tool

For our LangChain application to understand how to call a function within UC, we need to define the toolkit specification first.
The example below shows a single UC function for simplicity. If you're defining multiple tools, simply define the function reference
names within the `function_names` list.

``` python
from ucai_langchain.toolkit import UCFunctionToolkit

# Define the UC function to be used as a tool
func_name = f"{CATALOG}.{SCHEMA}.add_numbers"

# Create a toolkit with the UC function
toolkit = UCFunctionToolkit(function_names=[func_name])
```

### Use the tool in an agent

Now that we have our toolkit defined, we can use the property `tools` of the `UCFunctionToolkit` object just as any other tool would
be defined for use in LangChain.

``` python
from langchain.agents import AgentExecutor, create_tool_calling_agent
from langchain_core.prompts import ChatPromptTemplate
from langchain_community.chat_models.databricks import ChatDatabricks

# Initialize the LLM
llm = ChatDatabricks(endpoint="databricks-meta-llama-3-1-70b-instruct")

# Define the prompt
prompt = ChatPromptTemplate.from_messages([
    ("system", "You are a helpful assistant. Use tools for computations if applicable."),
    ("placeholder", "{chat_history}"),
    ("human", "{input}"),
    ("placeholder", "{agent_scratchpad}")
])

# Create the agent with our tools
agent = create_tool_calling_agent(llm, toolkit.tools, prompt)

# Create the executor, adding our defined tools from the UCFunctionToolkit instance
agent_executor = AgentExecutor(agent=agent, tools=toolkit.tools, verbose=True)

# Run the agent with an input
agent_executor.invoke({"input": "What is the sum of 4321.9876 and 1234.5678?"})

```

## Next Steps

- To learn more about Unity Catalog AI functionality, see [the main guide](index.md).
- To explore the available GenAI framework integrations, see [the integrations guide](integrations/index.md).
