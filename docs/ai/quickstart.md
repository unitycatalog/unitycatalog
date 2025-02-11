# Quickstart Guide

Welcome to the **Unity Catalog AI Core Library**! This guide will help you get started with installing the library, setting up your environment, creating and executing Unity Catalog (UC) functions. Whether you're using your own UC open-source server or connecting with a Databricks-managed UC, this quickstart will walk you through the essential steps.

---

## Installation

Install the Unity Catalog AI Core library directly from PyPI:

```sh
pip install unitycatalog-ai
```

> Note: If you install any of the integration packages directly, the AI core client library `unitycatalog-ai` will be included as a dependency.

## Prerequisites

Before you begin, ensure you have the following:

- **Python Version**: Python 3.10 or higher is recommended for simplified type hint processing within the Unity Catalog functions client.

### Unity Catalog Open Source

Ensure that you have met all of the prerequisites and have followed the
[Unity Catalog Quickstart Guide](../quickstart.md). Once your server is up and running and you are able to create a catalog, schemas, and functions, you're all set to continue with this guide.

### Databricks Unity Catalog

If you are going to interface with ``Databricks Unity Catalog``, you can install the optional package extension from PyPI:

```sh
pip install unitycatalog-ai[databricks]
```

>Note: Python 3.10 or higher is required to use `unitycatalog-ai` with Databricks serverless compute for function execution.

- **Serverless Compute**: Interfacing with the DatabricksFunctionClient requires the use of serverless on Databricks. Ensure that it is enabled.

## Using a function with LangChain

### Integration package installation

For this quick example with [LangChain](https://python.langchain.com/v0.2/docs/introduction/), we're first going to need to install the Unity Catalog
AI LangChain Integration package, `unitycatalog-langchain`. This package contains the tool definition logic specifically for LangChain, allowing you to
seamlessly define a native LangChain tool that can interface with functions stored in Unity Catalog and be used in your GenAI applications.

``` sh
pip install unitycatalog-langchain
```

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

CATALOG = "my_catalog"
SCHEMA = "my_schema"
```

### Client Setup - Databricks

In order to be able to both create and execute a function defined within UC as a tool in LangChain, we need to initialize the UC function client.
 When accessing functions by name, we will need to specify which catalog and schema the function resides in, so we're defining constants
to store those values.

``` python
from unitycatalog.ai.core.databricks import DatabricksFunctionClient

client = DatabricksFunctionClient()

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
 [compatibility matrix](usage.md#python-to-sql-compatiblity-matrix).
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
    schema=SCHEMA,
)
```

### Create a LangChain tool

For our LangChain application to understand how to call a function within UC, we need to define the toolkit specification first.
The example below shows a single UC function for simplicity. If you're defining multiple tools, simply define the function reference
names within the `function_names` list.

``` python
from unitycatalog.ai.langchain.toolkit import UCFunctionToolkit

# Define the UC function to be used as a tool
func_name = f"{CATALOG}.{SCHEMA}.add_numbers"

# Create a toolkit with the UC function
toolkit = UCFunctionToolkit(function_names=[func_name], client=client)
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

- To learn more about Unity Catalog AI functionality, see [the main guide](usage.md).
- To explore the available GenAI framework integrations, see [the integrations guide](integrations/index.md).
