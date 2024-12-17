# Using Unity Catalog AI with the Anthropic SDK

Integrate Unity Catalog AI with the [Anthropic SDK](https://docs.anthropic.com/en/api/client-sdks) to utilize functions defined in Unity Catalog
as tools in Anthropic LLM calls. This guide covers installation, setup, caveats, environment variables, public APIs, and examples to help you get started.

---

## Installation

Install the Unity Catalog AI Anthropic integration from PyPI:

```sh
pip install unitycatalog-anthropic
```

## Prerequisites

- **Python version**: Python 3.10 or higher is required.

### Unity Catalog

Ensure that you have a functional UC server set up and that you are able to access the catalog and schema where defined functions are stored.

### Databricks Unity Catalog

To interact with Databricks Unity Catalog, install the optional package dependency when installing the integration package:

```sh
pip install unitycatalog-anthropic[databricks]
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

func_name = f"{CATALOG}.{SCHEMA}.weather_function"

def weather_function(location: str) -> str:
    """
    Fetches the current weather from a given location in degrees Celsius.

    Args:
        location (str): The location to fetch the current weather from.
    Returns:
        str: The current temperature for the location provided in Celsius.
    """
    return f"The current temperature for {location} is 24.5 celsius"

client.create_python_function(
    func=weather_function,
    catalog=CATALOG,
    schema=SCHEMA
)
```

### Creating a toolkit instance from a UC function

``` python
from unitycatalog.ai.anthropic.toolkit import UCFunctionToolkit

# Create an instance of the toolkit
toolkit = UCFunctionToolkit(function_names=[func_name], client=client)
```

### Use a tool within a call to Anthropic

``` python
import anthropic

anthropic_client = anthropic.Anthropic(api_key="YOUR_ANTHROPIC_API_KEY")

# User's question
question = [{"role": "user", "content": "What's the weather in New York City?"}]

# Make the initial call to Anthropic
response = anthropic_client.messages.create(
    model="claude-3-5-sonnet-20240620",
    max_tokens_to_sample=1024,
    tools=toolkit.tools,
    messages=question,
)

print(response)
```

### Construct a tool response

The response from the call to Claude will contain a tool request metadata block if a tool needs to be called.
In order to simplify the parsing and handling of a call to the UC function that has been created, the `ucai-anthropic` package includes a
message handler utility that will detect, extract, perform the call to the UC function, parse the response, and craft the next message
format for the continuation of the conversation with Claude.

>Note: The entire conversation history must be provided in the `conversation_history` argument to the `generate_tool_call_messages` API.
Claude models require the initialization of the conversation (the original user input question) as well as all subsequent LLM-generated responses
and multi-turn tool call results.

``` python
from unitycatalog.ai.anthropic.utils import generate_tool_call_messages

# Call the UC function and construct the required formatted response
tool_messages = generate_tool_call_messages(
    response=response,
    client=client,
    conversation_history=question
)

# Continue the conversation with Anthropic
tool_response = anthropic_client.messages.create(
    model="claude-3-5-sonnet-20240620",
    max_tokens_to_sample=1024,
    tools=tools,
    messages=tool_messages,
)

print(tool_response)
```

## Additional Notes

- **control**: If you need to intercept and filter or adjust content between a request for a tool call and the next turn's interaction, the `generate_tool_call_messages` utility may not meet your needs. This utility is entirely optional to use. You can always manually construct the conversation
history for multi-turn conversations with Claude.

For access to the lower-level API for more control over tool calling execution with Anthropic, you can use the `extract_tool_call_data` utility:

``` python
from unitycatalog.ai.anthropic.utils import extract_tool_call_data

# This returns a List[ToolCallData]
parsed_messages = extract_tool_call_data(response)

results = []
for message in parsed_messages:
    tool_result = message.execute(client)
    results.append(message.to_tool_result_message(tool_result))
```

If using the above lower-level approach, remember that you will still need to construct the entire conversation history yourself before calling
the Anthropic APIs for the next phase in the conversation.

- **multiple tool calls**: Claude models may submit requests for multiple tools to be called in a single response. Ensure that the handling logic for your application is capable of making multiple calls to your UC tools if needed.
