# Using Unity Catalog AI with OpenAI

Integrate Unity Catalog AI package with OpenAI to allow seamless usage of UC functions as tools in agents application.

## Installation

### Client Library

To install the Unity Catalog function client SDK and the `OpenAI` integration, simply install from PyPI:

```sh
pip install unitycatalog-openai
```

If you are working with **Databricks Unity Catalog**, you can install the optional package:

```sh
pip install unitycatalog-openai[databricks]
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

To use Databricks-managed Unity Catalog with this package, follow the [instructions](https://docs.databricks.com/en/dev-tools/cli/authentication.html#authentication-for-the-databricks-cli) to authenticate to your workspace and ensure that your access token has workspace-level privilege for managing UC functions.

#### Client setup

Initialize a client for managing UC functions in a Databricks workspace, and set it as the global client.

```python
from unitycatalog.ai.core.client import set_uc_function_client
from unitycatalog.ai.core.databricks import DatabricksFunctionClient

client = DatabricksFunctionClient()

# sets the default uc function client
set_uc_function_client(client)
```

#### Create a Function in UC

Create a python UDF in Unity Catalog with the client

```python
# replace with your own catalog and schema
CATALOG = "catalog"
SCHEMA = "schema"

func_name = f"{CATALOG}.{SCHEMA}.python_exec"
# define the function body in SQL
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

Now the function is created and stored in the corresponding catalog and schema.

## Using the Function as a GenAI Tool

### Create a UCFunctionToolkit instance

[OpenAI function calling](https://platform.openai.com/docs/guides/function-calling) allows you to connect models like `gpt-4o-mini` to external tools and systems, and UCFunctionToolkit provides the ability to use UC functions as tools in OpenAI calls.

```python
from unitycatalog.ai.openai.toolkit import UCFunctionToolkit

# create an UCFunctionToolkit that includes the above UC function
toolkit = UCFunctionToolkit(function_names=[f"{CATALOG}.{SCHEMA}.python_exec"])

# fetch the tools stored in the toolkit
tools = toolkit.tools

# this is the function definition of the tool accepted by OpenAI
python_exec_tool = tools[0]
```

### Use the tools in OpenAI models

Now we use the tools when calling OpenAI Chat Completion API.

```python
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

Handle the response and execute the function based on response result

```python
import json

# there should only be one tool call
tool_call = response.choices[0].message.tool_calls[0]
# extract arguments
arguments = json.loads(tool_call.function.arguments)

# execute the function based on the arguments
result = client.execute_function(func_name, arguments)
print(result.value)
```

Construct the OpenAI response from the tool calling result

```python
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

### FAQ

#### What if I want to use a different client for different toolkits?

To use different clients during toolkit creation stage, you could pass the client directly to UCFunctionToolkit:

```python
from unitycatalog.ai.openai.toolkit import UCFunctionToolkit

toolkit = UCFunctionToolkit(function_names=[...], client=your_own_client)
```

Please note that this client is only used for retrieving UC functions so we can generate OpenAI accepted function definitions, which you could pass to the OpenAI API call. After getting a response from the OpenAI API, you should be responsible for executing them using the corresponding client with `client.execute_function(...)` as above example.

#### How should I handle the tool call response?

We provide a helper function for converting OpenAI ChatCompletion response to messages that can be sent over for response creation.

```python
from unitycatalog.ai.openai.utils import generate_tool_call_messages

messages = generate_tool_call_messages(response=response, client=client)
print(messages)
```

If the response contains multiple choices, you could pass `choice_index` (starting from 0) to `generate_tool_call_messages` to choose a single choice. Multiple choices are not supported yet.

#### Configurations for UC functions execution within Databricks

We provide configurations for databricks client to control the function execution behaviors, check [function execution arguments section](../../README.md#function-execution-arguments-configuration).
