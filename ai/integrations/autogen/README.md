# Using Unity Catalog AI with the Autogen SDK

You can use the Unity Catalog AI package with the autogen SDK to utilize functions that are defined in Unity Catalog to be used as tools within autogen LLM calls.

> [!NOTE]
> Ensure that the base Autogen package is installed with version `autogen-agentchat>=0.4.0`, as there has been a signficant series of API improvements made to autogen that are not backward compatible. This integration does not support the legacy APIs.

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

> **Note**: The official Microsoft AutoGen package has been renamed from `pyautogen` to `autogen-agentchat`.
There are additional forked version of the AutoGen package that are not contributed by Microsoft and will not work with this integration.
For further information, please see the [official clarification statement](https://github.com/microsoft/autogen/discussions/4217). The officially
maintained repository can be viewed [here](https://github.com/microsoft/autogen).

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

client = DatabricksFunctionClient()

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
)
```

Now that we have the AssistantAgent defined with our Unity Catalog tools configured, we can directly ask the Agent questions. 

### Calling the function

```python
user_input = "I need some help converting 973.2F to Celsius."

response = await weather_agent.on_messages(
    [TextMessage(content=user_input, source="User")], CancellationToken()
)

response.chat_message
```

Output

```text
TextMessage(source='assistant', models_usage=RequestUsage(prompt_tokens=286, completion_tokens=14), content='973.2°F is approximately 522.89°C.', type='TextMessage')
```

### Configurations for Databricks-only UC function execution

We provide configurations for the Databricks Client to control the function execution behaviors, check [function execution arguments section](../../README.md#function-execution-arguments-configuration).
