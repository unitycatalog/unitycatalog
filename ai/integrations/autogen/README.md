# Using Unity Catalog AI with the Autogen SDK

You can use the Unity Catalog AI package with the autogen SDK to utilize functions that are defined in Unity Catalog to be used as tools within autogen LLM calls.

> [!NOTE]
> Ensure that the base Autogen package is installed with version `autogen-agentchat~=0.2` or earlier, as there are significant changes in the API after this release.

## Installation

### Client Library

To install the Unity Catalog function client SDK and the `AutoGen` integration, simply install from PyPI:

```sh
pip install unitycatalog-autogen
```

If you are working with **Databricks Unity Catalog**, you can install the optional package:

```sh
pip install unitycatalog-autogen[databricks]
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
from unitycatalog.ai.core.base import set_uc_function_client
from unitycatalog.ai.core.databricks import DatabricksFunctionClient

client = DatabricksFunctionClient(
    warehouse_id="...", # replace with the warehouse_id
    cluster_id="..." # optional, only pass when you want to use cluster for function creation
)

# sets the default uc function client
set_uc_function_client(client)
```

#### Create a Function in UC

Create Python UDFs in Unity Catalog with the client.

```python
# replace with your own catalog and schema
CATALOG = "catalog"
SCHEMA = "schema"

from typing import Annotated

def get_temperature(location: Annotated[str, "Retrieves the current weather from a provided location."]) -> str:
    '''
    Returns the current temperature from a given location in degrees Celsius.
    '''
    return "31.9 C"

def temp_c_to_f(celsius: Annotated[str, "Temperature in Celsius"]) -> float:
    '''
    Converts temperature from Celsius to Fahrenheit.
    '''
    celsius = float(celsius)
    fahrenheit = (9/5 * celsius) + 32
    return fahrenheit

# Create UC functions
function_info_temp_c_to_f = client.create_python_function(
    func=TempCtoF,
    catalog=CATALOG,
    schema=SCHEMA
)

function_info_get_temp = client.create_python_function(
    func=get_temperature,
    catalog=CATALOG,
    schema=SCHEMA
)
```

Now that the functions are created and stored in the corresponding catalog and schema, we can use it within autogen's SDK.

## Using the Function as a GenAI Tool

### Create a UCFunctionToolkit instance

To begin, we will need an instance of the tool function interface from the `unitycatalog_autogen` toolkit.

```python
from unitycatalog.ai.autogen.toolkit import UCFunctionToolkit

# Create an instance of the toolkit with the functions that were created earlier.
# Use the full function names in 'catalog.schema.function_name' format
function_names = [
    f"{CATALOG}.{SCHEMA}.get_temperature",
    f"{CATALOG}.{SCHEMA}.TempCtoF"]

toolkit = UCFunctionToolkit(function_names=function_names,
                            client=client)

# Access the tool definitions that are in the interface that autogen's SDK expects
tools = toolkit.tools

```

Now that we have the defined tools from Unity Catalog, we can directly pass this definition to the Conversable Agent API within Autogen.

If you would like to validate that your tool is functional prior to proceeding to integrate it with Autogen, you can call the tool directly:

```python
my_tool = tools[0]

my_tool.fn(**{"location": "San Francisco"})
```

### Use the tools with a conversable Agent

```python

import os
from autogen import ConversableAgent, GroupChat, GroupChatManager


# Set up API keys
OPENAI_API_KEY = os.environ["OPENAI_API_KEY"]

# Define the assistant agent that suggests tool calls
assistant = ConversableAgent(
    name="Assistant",
    system_message="""You are a helpful AI assistant.
    You can tell the temperature of a location using function calling.
    Return 'TERMINATE' when the task is done and the final answer is returned.""",
    llm_config={"config_list": [{"model": "gpt-4", "api_key": OPENAI_API_KEY}]},
)

# The user proxy agent is used for interacting with the assistant agent
# and executes tool calls
user_proxy = ConversableAgent(
    name="User",
    llm_config=False,
    is_termination_msg=lambda msg: msg.get("content") is not None and "TERMINATE" in msg["content"],
    human_input_mode="NEVER",
)

converter = ConversableAgent(
    name="Fahrenheit_converter",
    system_message="You are a helpful AI assistant.",
    llm_config={"config_list": [{"model": "gpt-4", "api_key": OPENAI_API_KEY}]},
)
```

Once you have created a tool, you can register it with the agents involved in the conversation.

Similar to code executors, a tool must be registered with at least two agents for it to be useful in conversation: the agent which can call the tool and an agent which can execute the tool’s function.

You can use the `register_function` method to register a tool with both agents at once.

Below are examples of registering agent pairs with their corresponding functions.

```python

# Define agent pairs for each tool
agent_pairs_get_temp = {"callers": assistant, "executors": user_proxy}

agent_pairs_temp_c_to_f = {"callers": converter, "executors": user_proxy}

# Register the 'get_temperature' tool with its agent pairs
tool_get_temp = next(tool for tool in tools if 'get_temperature' in tool.name)
tool_get_temp.register_function(callers = agent_pairs_get_temp['callers'],
                                executors = agent_pairs_get_temp['executors'] )

# Register the 'TempCtoF' tool with its agent pairs
tool_temp_c_to_f = next(tool for tool in tools if 'TempCtoF' in tool.name)
tool_temp_c_to_f.register_function(callers = agent_pairs_temp_c_to_f['callers'],
                                executors = agent_pairs_temp_c_to_f['executors'] )

```

Alternatively, you can use the `register_with_agents` method from the UCFunctionToolkit class to register all tools at once:

```python

toolkit.register_with_agents(
    callers=[assistant, converter],
    executors=[user_proxy]
)

```

### Calling the function

```python
groupchat = GroupChat(
    agents=[user_proxy, assistant, converter],
    messages=[],
    max_round=10
)
manager = GroupChatManager(
    groupchat=groupchat,
    llm_config={"config_list": [{"model": "gpt-4", "api_key": OPENAI_API_KEY}]}
)

user_proxy.initiate_chat(
    manager, message="What is the temperature in SF in Fahrenheit?"
)

```

Output

```text
What is the temperature in SF in Fahrenheit?

--------------------------------------------------------------------------------
Next speaker: Assistant
>>>>>>>>> USING AUTO REPLY...
Assistant (to chat_manager):

***** Suggested tool call: get_temperature *****
Arguments: 
{
  "location": "SF"
}
***********************************************

--------------------------------------------------------------------------------
Next speaker: User
>>>>>>>>> EXECUTING FUNCTION get_temperature...
User (to chat_manager):

***** Response from calling tool *****
{"format": "SCALAR", "value": "31.9 C", "truncated": false}
***********************************************

--------------------------------------------------------------------------------
Next speaker: Fahrenheit_converter
>>>>>>>>> USING AUTO REPLY...
Fahrenheit_converter (to chat_manager):

***** Suggested tool call: temp_c_to_f *****
Arguments: 
{
  "celsius": "31.9"
}
***********************************************

--------------------------------------------------------------------------------
Next speaker: User
>>>>>>>>> EXECUTING FUNCTION temp_c_to_f...
User (to chat_manager):

***** Response from calling tool *****
{"format": "SCALAR", "value": "89.42", "truncated": false}
***********************************************

--------------------------------------------------------------------------------
Next speaker: Assistant
>>>>>>>>> USING AUTO REPLY...
Assistant (to chat_manager):

The temperature in SF is 89.42°F.

--------------------------------------------------------------------------------
Next speaker: User
User (to chat_manager):

--------------------------------------------------------------------------------
Next speaker: User
User (to chat_manager):

--------------------------------------------------------------------------------
Next speaker: Assistant
>>>>>>>>> USING AUTO REPLY...
Assistant (to chat_manager):

TERMINATE
--------------------------------------------------------------------------------


```

### Configurations for Databricks-only UC function execution

We provide configurations for the Databricks Client to control the function execution behaviors, check [function execution arguments section](../../README.md#function-execution-arguments-configuration).
