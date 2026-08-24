# Using Unity Catalog AI with AutoGen

Integrate Unity Catalog AI with the [AutoGen SDK](https://github.com/microsoft/autogen) to utilize functions defined in Unity Catalog as tools in AutoGen Agent Application calls. This guide covers the installation, setup, caveats, environment variables, public APIs, and examples to help you get started.

> **NOTE:** Ensure that the base Autogen package is installed with version `autogen-agentchat>=0.4.0`, as there has been a signficant series of API improvements made to autogen that are not backward compatible. This integration does not support the legacy APIs.
>
> **NOTE**: The official Microsoft AutoGen package has been renamed from `pyautogen` to `autogen-agentchat`.
There are additional forked version of the AutoGen package that are not contributed by Microsoft and will not work with this integration.
For further information, please see the [official clarification statement](https://github.com/microsoft/autogen/discussions/4217). The officially
maintained repository can be viewed [here](https://github.com/microsoft/autogen).

---

## Installation

Install the Unity Catalog AI AutoGen integration from PyPI:

```sh
pip install unitycatalog-autogen
```

## Prerequisites

- **Python version**: Python 3.10 or higher is required.

### Unity Catalog

Ensure that you have a functional UC server set up and that you are able to access the catalog and schema where defined functions are stored.

### Databricks Unity Catalog

To interact with Databricks Unity Catalog, install the optional package dependency when installing the integration package:

```sh
pip install unitycatalog-autogen[databricks]
```

#### Authentication with Databricks Unity Catalog

To use Databricks-managed Unity Catalog with this package, follow the [Databricks CLI authentication instructions](https://docs.databricks.com/en/dev-tools/cli/authentication.html#authentication-for-the-databricks-cli) to authenticate to your workspace and ensure that your access token has workspace-level privileges for managing UC functions.

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

Initialize a client for managing Unity Catalog functions in a Databricks workspace and set it as the global client.

```python
from unitycatalog.ai.core.base import set_uc_function_client
from unitycatalog.ai.core.databricks import DatabricksFunctionClient

client = DatabricksFunctionClient()

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

from autogen_agentchat.agents import AssistantAgent
from autogen_agentchat.messages import TextMessage
from autogen_core import CancellationToken
from autogen_ext.models.openai import OpenAIChatCompletionClient

OPENAI_API_KEY = os.environ["OPENAI_API_KEY"]

model_client = OpenAIChatCompletionClient(
    model="gpt-4o",
    api_key=OPENAI_API_KEY,
    seed=222,
    temperature=0,
)

weather_agent = AssistantAgent(
    name="assistant",
    system_message="You are a helpful AI assistant that specializes in answering questions about weather phenomena. "
    "If there are tools available to perform these calculations, please use them and ensure that you are operating "
    "with validated data calculations.",
    model_client=model_client,
    tools=tools,
    reflect_on_tool_use=False,
```

### Initialize a Conversation with your Agents

You can directly interact with the `AssistantAgent` by sending input queries as follows:

```python
user_input = "I need some help converting 973.2F to Celsius."

response = await weather_agent.on_messages(
    [TextMessage(content=user_input, source="User")], CancellationToken()
)

response.chat_message
```

An example output of this chat interaction is:

```text
TextMessage(source='assistant', models_usage=RequestUsage(prompt_tokens=286, completion_tokens=14), content='973.2°F is approximately 522.89°C.', type='TextMessage')
```
