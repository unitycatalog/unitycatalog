# 🚅 LiteLLM: Using Unity Catalog AI with the LiteLLM SDK

Integrate Unity Catalog AI with [LiteLLM](https://docs.litellm.ai/) to seamlessly use functions defined in Unity Catalog as tools in your LiteLLM LLM calls. This guide covers installation, client setup for interfacing with Unity Catalog, and examples for using your UC functions as callable tools within LiteLLM.

---

## Installation

Install the Unity Catalog AI LiteLLM integration from PyPI:

```sh
pip install unitycatalog-litellm
```

### Databricks Unity Catalog

To interact with Databricks Unity Catalog, install the optional package dependency when installing the integration package:

```sh
pip install unitycatalog-litellm[databricks]
```

---

## Get Started

### Client Setup

If you are using an open source Unity Catalog deployment, create an instance of the Functions Client that corresponds to your deployment:

```python
from unitycatalog.client import ApiClient, Configuration
from unitycatalog.ai.core.client import UnitycatalogFunctionClient

config = Configuration()
# This is the default address when starting a UnityCatalog server locally.
# Update this to the URI of your running UnityCatalog server.
config.host = "http://localhost:8080/api/2.1/unity-catalog"

# Create the UnityCatalog client
api_client = ApiClient(configuration=config)

# Use the UnityCatalog client to create an instance of the AI function client
client = UnitycatalogFunctionClient(api_client=api_client)
```

### Client Setup - Databricks

First, initialize a client for managing UC functions in a Databricks workspace, and set it as the global client.

```python
from unitycatalog.ai.core.base import set_uc_function_client
from unitycatalog.ai.core.databricks import DatabricksFunctionClient

client = DatabricksFunctionClient()

# sets the default uc function client
set_uc_function_client(client)
```

---

## Tutorial

### Creating a UC function

To provide an executable function for your tool, first create a Python function wrapped within the SQL body format for UC. Then, use the appropriate client to store this function in Unity Catalog. For example:

```python
# Replace with your own catalog and schema for where your function will be stored
CATALOG = "main"
SCHEMA = "default"

def sf_weather_lookup_litellm(city_name: str) -> str:
    """
    Get the weather in San Francisco.

    Args:
        city_name (str): The name of the city.

    Returns:
        str: A string representing the weather condition.
    """
    return "cloudy and boring"

response = client.create_python_function(
    func=sf_weather_lookup_litellm,
    catalog=CATALOG,
    schema=SCHEMA
)
```

Once the function is stored within the designated Catalog and Schema, you can interface with it from LiteLLM.

### Creating a LiteLLM-Compatible Toolkit Instance

LiteLLM Tools are callable external functions that are used by supported LLMs. The `unitycatalog-litellm` package exposes these as a JSON tool collection using the `UCFunctionToolkit` API.

```python
from unitycatalog.ai.litellm.toolkit import UCFunctionToolkit

# Pass the UC function name to the toolkit constructor.
toolkit = UCFunctionToolkit(function_names=[f"{CATALOG}.{SCHEMA}.sf_weather_lookup_litellm"])

# Get the LiteLLM-compatible tool definitions
tools = toolkit.tools
```

To verify that your tool is functional, you can invoke it directly:

```python
# Execute the tool directly by calling its function with required parameters.
my_tool = tools[0]
result = my_tool.fn(**{"city_name": "Dogpatch"})
print(result)
```

**Expected Output**:

```text
'{"format": "SCALAR", "value": "cloudy and boring", "truncated": false}'
```

### Using the Function as a Tool within a LiteLLM Completion Call

With your UC function defined as a JSON tool collection, you can directly use it within a LiteLLM Completion call. Note that LiteLLM leverages a standardized JSON tool format (compatible with OpenAI’s tool format) across supported LLMs.

> [!NOTE] LiteLLM does not use tool objects internally. Instead, it leverages a JSON format that is passed directly to supported LLMs. For more details on supported
> models and the function calling format, please visit the [LiteLLM documentation](https://docs.litellm.ai/).

Below is an example of setting up a LiteLLM completion call that includes your defined UC tools:

```python
import litellm
import os

# Set up your API key for the LLM
os.environ["OPENAI_API_KEY"] = "your key"

# Define your request message
question = "What's the weather like in San Francisco?"
messages = [{"role": "user", "content": question}]

# Call LiteLLM's completion endpoint, passing in the tools
response = litellm.completion(
    model="gpt-4o-mini",
    messages=messages,
    tools=tools,
    tool_choice="auto",  # default
)
print("\nFirst LLM Response:\n", response)
```

**Sample Output**:

```text
ModelResponse(
    id="chatcmpl-AR5vLwQm66qMQKelVIfiaa1izMSIF",
    choices=[
        Choices(
            finish_reason="tool_calls",
            index=0,
            message=Message(
                content=None,
                role="assistant",
                tool_calls=[
                    ChatCompletionMessageToolCall(
                        function=Function(
                            arguments='{"city_name": "San Francisco"}',
                            name="main__default__sf_weather_lookup_litellm",
                        ),
                        id="call_QUj1gKkfY8i1sVc4Tlfr3hrM",
                        type="function",
                    )
                ],
                function_call=None,
            ),
        )
    ],
    created=1731020991,
    model="gpt-4o-mini",
    object="chat.completion",
    system_fingerprint="fp_e7d4a5f731",
    usage=Usage(
        completion_tokens=39,
        prompt_tokens=99,
        total_tokens=138,
        completion_tokens_details=CompletionTokensDetailsWrapper(
            accepted_prediction_tokens=0,
            audio_tokens=0,
            reasoning_tokens=0,
            rejected_prediction_tokens=0,
            text_tokens=None,
        ),
        prompt_tokens_details=PromptTokensDetailsWrapper(
            audio_tokens=0, cached_tokens=0, text_tokens=None, image_tokens=None
        ),
    ),
    service_tier=None,
)
```

### Calling the Function

There are two methods available to call your Unity Catalog function via LiteLLM:

#### Using the `generate_tool_call_messages` Utility (**Recommended**)

This API simplifies the workflow by extracting tool calling instructions from the LiteLLM response, executing the UC function, and returning the payload needed for the subsequent LiteLLM call. To ensure contextual references for subsequent LLM calls, ensure that your conversation history includes the initial user input.

```python
from unitycatalog.ai.litellm.utils import generate_tool_call_messages

# Extract tool calling instructions, execute the UC function,
# and construct the updated conversation history.
tool_messages = generate_tool_call_messages(response=response, client=client, conversation_history=messages)

# Make the subsequent LiteLLM call with the updated history
response_2 = litellm.completion(
    model="gpt-4o-mini",
    messages=tool_messages,
)
print("\nSecond LLM Response:\n", response_2)
```

**Expected Output**:

```text
ModelResponse(
    id="chatcmpl-AR5w1gfmKNXxQy97dAbia9ye1zBPi",
    choices=[
        Choices(
            finish_reason="stop",
            index=0,
            message=Message(
                content="The weather in San Francisco is currently cloudy.",
                role="assistant",
                tool_calls=None,
                function_call=None,
            ),
        )
    ],
    created=1731021033,
    model="gpt-4o-mini",
    object="chat.completion",
    system_fingerprint="fp_e7d4a5f731",
    usage=Usage(
        completion_tokens=9,
        prompt_tokens=60,
        total_tokens=69,
        completion_tokens_details=CompletionTokensDetailsWrapper(
            accepted_prediction_tokens=0,
            audio_tokens=0,
            reasoning_tokens=0,
            rejected_prediction_tokens=0,
            text_tokens=None,
        ),
        prompt_tokens_details=PromptTokensDetailsWrapper(
            audio_tokens=0, cached_tokens=0, text_tokens=None, image_tokens=None
        ),
    ),
    service_tier=None,
)
```

#### Using the `extract_tool_call_data` Utility (**Advanced Use**)

This lower-level API allows you to extract tool call data for more advanced use cases where you might want to intervene between the tool call request and its execution.

```python
from unitycatalog.ai.litellm.utils import extract_tool_call_data

# Extract a list of tool call data objects from the LiteLLM response.
parsed_messages = extract_tool_call_data(response)[0]  # Extract the first choice

# To inspect the parsed tool call data:
parsed = [message.to_dict() for message in parsed_messages]
print(parsed)

# Execute each tool call and obtain the formatted response for LiteLLM:
results = []
for message in parsed_messages:
    result = message.execute(client)
    results.append(message.to_tool_result_message(result))

print(results)
```

**Expected Output**:

```text
[{'function_name': 'main.default.sf_weather_lookup_litellm', 'arguments': {'city_name': 'San Francisco'}, 'tool_use_id': 'call_QUj1gKkfY8i1sVc4Tlfr3hrM'}]
[{'role': 'tool', 'tool_call_id': 'call_QUj1gKkfY8i1sVc4Tlfr3hrM', 'name': 'main.default.sf_weather_lookup_litellm', 'content': 'cloudy and boring'}]
```

---

By following this guide, you can integrate Unity Catalog AI with LiteLLM to deploy and execute UC functions as tools in your LLM applications. Enjoy building with Unity Catalog AI and LiteLLM!
