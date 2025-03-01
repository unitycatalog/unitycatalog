# 🦜🔗 Using Unity Catalog AI with LangChain

Integrate Unity Catalog AI with [LangChain](https://python.langchain.com) to seamlessly use Unity Catalog (UC) functions as tools in agent applications. This guide covers installation, setup, and examples to help you get started.

---

## Installation

Install the Unity Catalog AI LangChain integration from PyPI:

```sh
pip install unitycatalog-langchain
```

## Prerequisites

- **Python version**: Python 3.10 or higher is required.

### Unity Catalog

Ensure that you have a functional UC server set up and that you are able to access the catalog and schema where defined functions are stored.

### Databricks Unity Catalog

To interact with Databricks Unity Catalog, install the optional package dependency when installing the integration package:

```sh
pip install unitycatalog-langchain[databricks]
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
from unitycatalog.ai.langchain.toolkit import UCFunctionToolkit

# Create a UCFunctionToolkit that includes the UC function
toolkit = UCFunctionToolkit(function_names=[func_name], client=client)

# Fetch the tools stored in the toolkit
tools = toolkit.tools
python_exec_tool = tools[0]

# Execute the tool directly
result = python_exec_tool.invoke({"code": "print(1 + 1)"})
print(result)  # Outputs: 2
```

### Using the tool in a LangChain Agent

``` python
from langchain.agents import AgentExecutor, create_tool_agent
from langchain.llms import OpenAI
from langchain.prompts import ChatPromptTemplate

# Initialize the LLM (replace with your LLM of choice, if desired)
llm = OpenAI(temperature=0)

# Define the prompt
prompt = ChatPromptTemplate.from_messages(
    [
        (
            "system",
            "You are a helpful assistant. Make sure to use tool for information.",
        ),
        ("placeholder", "{chat_history}"),
        ("human", "{input}"),
        ("placeholder", "{agent_scratchpad}"),
    ]
)

# Define the agent, specifying the tools from the toolkit above
agent = create_tool_calling_agent(llm, tools, prompt)

# Create the agent executor
agent_executor = AgentExecutor(agent=agent, tools=tools, verbose=True)
agent_executor.invoke({"input": "What is 36939 * 8922.4?"})
```
