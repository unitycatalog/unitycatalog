# Using Unity Catalog AI with AutoGen

Integrate Unity Catalog AI with the [AutoGen SDK](https://github.com/microsoft/autogen) to utilize functions defined in Unity Catalog as tools in AutoGen Agent Application calls. This guide covers the installation, setup, caveats, environment variables, public APIs, and examples to help you get started.

> **NOTE:** Ensure that the base AutoGen package is installed with version `autogen-agentchat~=0.2` or earlier, as there are significant changes in the API after this release.

---

## Installation

Install the Unity Catalog AI AutoGen integration from PyPI:

```sh
pip install unitycatalog-autogen
```

## Prerequisites

- **Python version**: Python 3.10 or higher is required.

Install the Autogen SDK if you don't already have it in your environment:

```sh
pip install autogen
```

### Unity Catalog Open Source

Ensure that you have a functional UC server set up and that you are able to access the catalog and schema where defined functions are stored and that you install the Unity Catalog Python SDK:

```sh
pip install unitycatalog-client
```

### Databricks Unity Catalog

To interact with Databricks Unity Catalog, ensure that you have both the `databricks-sdk` and the `databricks-connect` packages installed:

```sh
pip install databricks-sdk "databricks-connect>=15.1.0"
```

#### Authentication with Databricks Unity Catalog

To use Databricks-managed Unity Catalog with this package, follow the [Databricks CLI authentication instructions](https://docs.databricks.com/en/dev-tools/cli/authentication.html#authentication-for-the-databricks-cli) to authenticate to your workspace and ensure that your access token has workspace-level privileges for managing UC functions.

## Tutorial

### Client Setup - OSS Unity Catalog

Create an instance of the Functions Client

```python
from unitycatalog.client import ApiClient, Configuration
from unitycatalog.ai.core.oss import UnitycatalogFunctionClient

config = Configuration()
# This is the default address when starting a UnityCatalog server locally. Update this to the uri
# of your running UnityCatalog server.
config.host = "http://localhost:8080/api/2.1/unity-catalog"

# Create the UnityCatalog client
api_client = ApiClient(configuration=config)

# Use the UnityCatalog client to create an instance of the AI function client
client = UnitycatalogFunctionClient(uc=api_client)
```

### Client Setup - Databricks

Initialize a client for managing Unity Catalog functions in a Databricks workspace and set it as the global client.

```python
from unitycatalog.ai.core.client import set_uc_function_client
from unitycatalog.ai.core.databricks import DatabricksFunctionClient

client = DatabricksFunctionClient(
    warehouse_id="your_warehouse_id",  # replace with your warehouse_id
    cluster_id="your_cluster_id"       # optional, only pass when you want to use a cluster for function creation
)

# Set the default UC function client
set_uc_function_client(client)
```

### Create UC functions to use as tools

Define and create Python functions that will be stored in Unity Catalog

```python
from typing import Annotated

CATALOG = "your_catalog"
SCHEMA = "your_schema"

def get_temperature(location: Annotated[str, "Retrieves the current weather from a provided location."]) -> str:
    """
    Returns the current temperature from a given location in degrees Celsius.
    """
    return "31.9 C"

def temp_c_to_f(celsius: Annotated[str, "Temperature in Celsius"]) -> float:
    """
    Converts temperature from Celsius to Fahrenheit.
    """
    celsius = float(celsius)
    fahrenheit = (9/5 * celsius) + 32
    return fahrenheit

# Create UC functions
function_info_temp_c_to_f = client.create_python_function(
    func=temp_c_to_f,
    catalog=CATALOG,
    schema=SCHEMA
)

function_info_get_temp = client.create_python_function(
    func=get_temperature,
    catalog=CATALOG,
    schema=SCHEMA
)
```

### Create a Toolkit instance

Create an instance of the toolkit to interface with the defined UC functions. This definition will be the bridge
between UC functions and AutoGen's tool calling interface.

```python
from unitycatalog.ai.autogen.toolkit import UCFunctionToolkit

# Define the full function names in 'catalog.schema.function_name' format
function_names = [
    f"{CATALOG}.{SCHEMA}.get_temperature",
    f"{CATALOG}.{SCHEMA}.temp_c_to_f"
]

# Create an instance of the toolkit with the specified functions
toolkit = UCFunctionToolkit(function_names=function_names, client=client)

# Access the tool definitions expected by Autogen's SDK
tools = toolkit.tools
```

### Use the tools with a Conversable Agent in AutoGen

Set up agents and register the tools to enable conversations that utilize the UC functions.

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

# Define another agent for additional tool execution if needed
converter = ConversableAgent(
    name="Fahrenheit_converter",
    system_message="You are a helpful AI assistant.",
    llm_config={"config_list": [{"model": "gpt-4", "api_key": OPENAI_API_KEY}]},
)
```

#### Registering the tools with the defined Agents

There are two ways to register your UC functions as tools from the toolkit instance:

**Option 1**: Register each tool with the appropriate agents to enable function calling within conversations.

```python
# Define agent pairs for each tool
agent_pairs_get_temp = {"callers": assistant, "executors": user_proxy}
agent_pairs_temp_c_to_f = {"callers": converter, "executors": user_proxy}

# Register the 'get_temperature' tool with its agent pair
tool_get_temp = next(tool for tool in tools if 'get_temperature' in tool.name)
tool_get_temp.register_function(
    callers=agent_pairs_get_temp['callers'],
    executors=agent_pairs_get_temp['executors']
)

# Register the 'temp_c_to_f' tool with its agent pair
tool_temp_c_to_f = next(tool for tool in tools if 'temp_c_to_f' in tool.name)
tool_temp_c_to_f.register_function(
    callers=agent_pairs_temp_c_to_f['callers'],
    executors=agent_pairs_temp_c_to_f['executors']
)
```

**Option 2**: Alternatively, register all tools at once using the `register_with_agents` method from the `UCFunctionToolkit` object:

```python
toolkit.register_with_agents(
    callers=[assistant, converter],
    executors=[user_proxy]
)
```

### Initialize a Conversation with your Agents

Create a group chat and initiate a conversation that utilizes the registered tools.

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

An example output of this chat interaction is:

```text
What is the temperature in SF in Fahrenheit?

--------------------------------------------------------------------------------
Next speaker: Assistant
>>>>>>>>>> USING AUTO REPLY...
Assistant (to chat_manager):

***** Suggested tool call: get_temperature *****
Arguments: 
{
  "location": "SF"
}
***********************************************

--------------------------------------------------------------------------------
Next speaker: User
>>>>>>>>>> EXECUTING FUNCTION get_temperature...
User (to chat_manager):

***** Response from calling tool *****
{"format": "SCALAR", "value": "31.9 C", "truncated": false}
***********************************************

--------------------------------------------------------------------------------
Next speaker: Fahrenheit_converter
>>>>>>>>>> USING AUTO REPLY...
Fahrenheit_converter (to chat_manager):

***** Suggested tool call: temp_c_to_f *****
Arguments: 
{
  "celsius": "31.9"
}
***********************************************

--------------------------------------------------------------------------------
Next speaker: User
>>>>>>>>>> EXECUTING FUNCTION temp_c_to_f...
User (to chat_manager):

***** Response from calling tool *****
{"format": "SCALAR", "value": "89.42", "truncated": false}
***********************************************

--------------------------------------------------------------------------------
Next speaker: Assistant
>>>>>>>>>> USING AUTO REPLY...
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
>>>>>>>>>> USING AUTO REPLY...
Assistant (to chat_manager):

TERMINATE
--------------------------------------------------------------------------------
```
