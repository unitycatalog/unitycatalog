# Using Unity Catalog AI with the LiteLLM SDK

You can use the Unity Catalog AI package with the LiteLLM integration to utilize functions that are defined in Unity Catalog to be used as tools within [LiteLLM](https://docs.litellm.ai/) LLM calls.

## Installation

```sh
pip install unitycatalog-litellm
```

## Get started

### Client Setup

To use open source Unity Catalog with this package, create an instance of the Functions Client that is in accordance with your deployment, as shown below.

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

To use Databricks-managed Unity Catalog with this package, follow the [instructions](https://docs.databricks.com/en/dev-tools/cli/authentication.html#authentication-for-the-databricks-cli) to authenticate to your workspace and ensure that your access token has workspace-level privilege for managing UC functions.

First, initialize a client for managing UC functions in a Databricks workspace, and set it as the global client.

```python
from unitycatalog.ai.core.base import set_uc_function_client
from unitycatalog.ai.core.databricks import DatabricksFunctionClient

client = DatabricksFunctionClient()

# sets the default uc function client
set_uc_function_client(client)
```

#### Create a UC function

To provide an executable function for your tool to use, you need to define and create the function within UC. To do this,
create a Python function that is wrapped within the SQL body format for UC and then utilize the `DatabricksFunctionClient` to store this in UC:

```python
# Replace with your own catalog and schema for where your function will be stored
CATALOG="main"
SCHEMA = "default"

def sf_weather_lookup_litellm(city_name: str) -> str:
    """
    Get the weather in san fransisco.

    Args:
        city_name (str): The name of the city 

    Returns:
        A string of the temperature.
    """
    return "cloudy and boring"

response = client.create_python_function(func=sf_weather_lookup_litellm, catalog=CATALOG, schema=SCHEMA)
```

Now that the function exists within the Catalog and Schema that we defined, we can interface with it from LiteLLM using the `unitycatalog-litellm` package.

#### Create an instance of a LiteLLM compatible tool

[LiteLLM Tools](https://docs.litellm.ai/docs/providers/ollama#example-usage---tool-calling) are callable external functions that GenAI applications can use (called by
their many [supported LLMs](https://docs.litellm.ai/docs/providers)), which are exposed with a UC interface through the use of the `unitycatalog-litellm` package via the `UCFunctionToolkit` API.

```python
from unitycatalog.ai.litellm.toolkit import UCFunctionToolkit

# Pass the UC function name that we created to the constructor
toolkit = UCFunctionToolkit(function_names=[f"{CATALOG}.{SCHEMA}.sf_weather_lookup_litellm"])

# Get the LiteLLM-compatible tools definitions
tools = toolkit.tools
```

If you would like to validate that your tool is functional prior to integrating it with LiteLLM, you can call the tool directly:

```python
my_tool = tools[0]

my_tool.fn(**{"city_name": "Dogpatch"})
```

Output

```text
'{"format": "SCALAR", "value": "cloudy and boring", "truncated": false}'
```

#### Utilize our function as a tool within a LiteLLM Completion Call

With our interface to our UC function defined as a JSON tool collection, we can directly use it within a LiteLLM Completion call.

> [!NOTE]
> LiteLLM doesn't have tool objects and instead looks to leverage a standard JSON format; these are passed directly to 
> supported LLMs. This integration standardizes on the OpenAI tool format, which is supported by most LiteLLM models.
> For more, please visit the [LiteLLM](https://docs.litellm.ai/docs/completion/function_call) docs on which models support tool calling.

```python
import litellm 

# Set up API keys
import os
os.environ["OPENAI_API_KEY"] = "your key"


# Define your request 
question = "What's the weather like in San Francisco?" 
messages = [{"role": "user", "content": question}]

# Show the response
response = litellm.completion(
    model="gpt-4o-mini",
    messages=messages,
    tools=tools,
    tool_choice="auto",  # auto is default, but we'll be explicit
)
print("\nFirst LLM Response:\n", response)
```

Output

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

#### Calling the function

There are two ways of calling the function within UC: using the `generate_tool_call_messages` function on the response and manually, via the returned values of `extract_tool_call_data`.

##### Use the `generate_tool_call_messages` function on the response

**This is the recommended API to use to simplify your workstream**. This option will extract the tool calling instructions, execute the appropriate
functions in Unity Catalog, and return the payload needed to call the `litellm.create` API directly. If there are no tool
calls to be made, this function will return the state of the conversation history up to this point.

Note that the conversation history up until this point (which must start with the initial user input message) is required for this API to function
correctly. LiteLLM requires the full scope of the history, including both the tool use request and the tool response messages in order to continue
providing an answer. In the example below, the only history that we have is the original initial user question.

```python
from unitycatalog.ai.litellm.utils import generate_tool_call_messages

# Call the Unity Catalog function and construct the required formatted response history for a subsequent call to LiteLLM 
tool_messages = generate_tool_call_messages(response=response, client=client, conversation_history=messages)

# Call the LiteLLM client with the parsed tool response from executing the Unity Catalog function
# Show the response
response_2 = litellm.completion(
    model="gpt-4o-mini",
    messages=tool_messages,
)
print("\nSecond LLM Response:\n", response_2)
```

Output

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

##### Use the returned values of `extract_tool_call_data`

**Note** this is a lower-level API and is intended for advanced use cases where logic needs to exist between the tool call request, its response,
and the construction of a subsequent call to LiteLLM.

This API is useful if you need to perform validation prior to calling a function or if you prefer to handle the direct return of the Unity Catalog
function call yourself. This lower-level approach will require a more complex integration with LiteLLM.

```python
from unitycatalog.ai.litellm.utils import extract_tool_call_data


# This returns a List[ToolCallData] for LiteLLM
parsed_messages = extract_tool_call_data(response)[0] # Extract the first choice

# # To see the parsed data that will be submitted for function calling in Unity Catalog:
parsed = [message.to_dict() for message in parsed_messages]

print(parsed)


# To call each tool and provide the formatted response objects:
# Note that you will need to construct the full conversation history to submit to LiteLLM if you use this API
# as the return of `to_tool_result_message` contains only the formatted response from a tool call.
results = []
for message in parsed_messages:
    result = message.execute(client)
    results.append(message.to_tool_result_message(result))

print(results)
```

Output

```text
[{'function_name': 'main.default.sf_weather_lookup_litellm', 'arguments': {'city_name': 'San Francisco'}, 'tool_use_id': 'call_QUj1gKkfY8i1sVc4Tlfr3hrM'}]
[{'role': 'tool', 'tool_call_id': 'call_QUj1gKkfY8i1sVc4Tlfr3hrM', 'name': 'main.default.sf_weather_lookup_litellm', 'content': 'cloudy and boring'}]
```
