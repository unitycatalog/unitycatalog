# Using Unity Catalog AI with the OpenAI SDK

Integrate Unity Catalog AI with [OpenAI](https://platform.openai.com/docs/api-reference/introduction?lang=python) to directly use UC functions as tools in OpenAI interfaces. This guide covers installation, client setup, and examples to get started.

---

## Installation

To get started with the `unitycatalog-openai` integration, install the following packages from PyPI:

```sh
pip install unitycatalog-openai
```

## Prerequisites

- **Python version**: Python 3.10 or higher is required.

### Unity Catalog Server

Ensure that you have a functional UC server set up and that you are able to access the catalog and schema where defined functions are stored.

### Databricks Unity Catalog

To interact with Databricks Unity Catalog, install the optional package dependency when installing the integration package:

```sh
pip install unitycatalog-openai[databricks]
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
[OpenAI function calling](https://platform.openai.com/docs/guides/function-calling) allows a subset of their more recent models to accept external tool
calling capabilities. Ensure that the model that you are selecting to interface with has the capability to accept tool definitions.

``` python
from unitycatalog.ai.openai.toolkit import UCFunctionToolkit

# Create a UCFunctionToolkit that includes the UC function
toolkit = UCFunctionToolkit(function_names=[func_name])

# Fetch the tools stored in the toolkit
tools = toolkit.tools
python_exec_tool = tools[0]

# Execute the tool directly
result = python_exec_tool.invoke({"code": "print(1 + 1)"})
print(result)  # Outputs: 2
```

### Send a tool-enabled question to OpenAI

With the client defined, we can now submit the tools along with our request to our defined OpenAI model.

``` python
import openai

messages = [
            {
                "role": "system",
                "content": "You are a helpful customer support assistant. Use the supplied tools to assist the user.",
            },
            {"role": "user", "content": "What is the result of 2**10?"},
        ]
response = openai.chat.completions.create(
                model="gpt-4o-mini",
                messages=messages,
                tools=tools,
            )
# check the model response
print(response)
```

After the response is returned from OpenAI, you will need to invoke the UC function call to generate the response answer back to OpenAI.

``` python
import json

# OpenAI will only send a single request per tool call
tool_call = response.choices[0].message.tool_calls[0]
# extract arguments that the UC function will need for its execution
arguments = json.loads(tool_call.function.arguments)

# execute the function based on the arguments
result = client.execute_function(func_name, arguments)
print(result.value)
```

Once the answer has been returned, you can construct the response payload for the subsequent call to OpenAI.

``` python
# Create a message containing the result of the function call
function_call_result_message = {
    "role": "tool",
    "content": json.dumps({"content": result.value}),
    "tool_call_id": tool_call.id,
}
assistant_message = response.choices[0].message.to_dict()
completion_payload = {
    "model": "gpt-4o-mini",
    "messages": [*messages, assistant_message, function_call_result_message],
}

# Generate final response
openai.chat.completions.create(
    model=completion_payload["model"], messages=completion_payload["messages"]
)
```

### Utilities

To simplify the process of crafting the tool response, the ucai-openai package has a utility, `generate_tool_call_messages`, that will convert the
`ChatCompletion` response message from OpenAI so that it can be used for response generation.

``` python
from unitycatalog.ai.openai.utils import generate_tool_call_messages

messages = generate_tool_call_messages(response=response, client=client)
print(messages)
```

> Note: if the response contains multiple `choice` entries, you can pass the `choice_index` argument when calling `generate_tool_call_messages` to choose
which `choice` entry to utilize. There is currently no support for processing multiple `choice` entries.
