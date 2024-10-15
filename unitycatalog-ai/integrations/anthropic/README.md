# Using Unity Catalog AI with the Anthropic SDK

You can use the Unity Catalog AI package with the Anthropic SDK to utilize functions that are defined in Unity Catalog to be used as tools within Anthropic LLM calls.

## Installation

```sh
# install from the source
pip install git+https://github.com/serena-ruan/unitycatalog-ai.git#subdirectory=integrations/anthropic
```

> [!NOTE]
> Once this package is published to PyPI, users can install via `pip install ucai-anthropic`

## Get started

### Databricks-managed UC

To use Databricks-managed Unity Catalog with this package, follow the [instructions](https://docs.databricks.com/en/dev-tools/cli/authentication.html#authentication-for-the-databricks-cli) to authenticate to your workspace and ensure that your access token has workspace-level privilege for managing UC functions.

#### Client setup

Initialize a client for managing UC functions in a Databricks workspace, and set it as the global client.

```python
from ucai.core.client import set_uc_function_client
from ucai.core.databricks import DatabricksFunctionClient

client = DatabricksFunctionClient(
    warehouse_id="..." # replace with the warehouse_id
    cluster_id="..." # optional, only pass when you want to use cluster for function creation
)

# sets the default uc function client
set_uc_function_client(client)
```

#### Create a function in UC

Create a python UDF in Unity Catalog with the client

```python
# replace with your own catalog and schema
CATALOG = "catalog"
SCHEMA = "schema"

func_name = f"{CATALOG}.{SCHEMA}.python_exec"
# define the function body in SQL
sql_body = f"""CREATE OR REPLACE FUNCTION {func_name}(location STRING COMMENT 'Retrieves the current weather from a provided location.')
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'Returns the current weather from a given location and returns the temperature in degrees Celsius.'
AS $$
    return "31.9 C"
$$
"""

client.create_function(sql_function_body=sql_body)
```

Now that the function is created and stored in the corresponding catalog and schema, we can use it within Anthropic's SDK.

#### Create a UCFunctionToolkit instance

Tool use through the [Anthropic SDK](https://docs.anthropic.com/en/docs/build-with-claude/tool-use) allows you to connect external client-side tools and
functions to provide [Claude](https://docs.anthropic.com/en/docs/welcome) with a greater range of capabilities to augment its ability to respond to user messages.

To begin, we will need an instance of the tool function interface from the `ucai_anthropic` toolkit.

```python
from ucai_anthropic.toolkit import UCFunctionToolkit

# Create an instance of the toolkit with the function that was created earlier.
toolkit = UCFunctionToolkit(function_names=[f"{CATALOG}.{SCHEMA}.python_exec"], client=client)

# Access the tool definitions that are in the interface that Anthropic's SDK expects
tools = tookit.tools

```

Now that we have the defined tools from Unity Catalog, we can directly pass this definition into a messages request.

#### Use the tools within a request to Anthropic models

Anthropic will generate a stopping condition of `"tool_use"` when a relevant tool definition is provided to a message creation call, responding with the
function's name to call and the input arguments to provide to the tool.

```python

import anthropic

anthropic_client = anthropic.Anthropic()

multi = "What's the weather in Nome, AK and in Death Valley, CA?"

question = [{"role": "user", "content": multi}]

response = anthropic_client.messages.create(
    model="claude-3-5-sonnet-20240620",
    max_tokens=1024,
    tools=tools,
    messages=question,
)

print(response)
```

Within the response, you will see instances of `ToolUseBlock` from Anthropic's SDK. These blocks, if present, indicate the name of the tool to use
and the inputs to provide to the defined tool's function.

#### Calling the function

There are two ways of calling the function within UC:

- Use the `generate_tool_call_messages` function on the response.

**This is the recommended API to use to simplify your workstream**. This option will extract the tool calling instructions, execute the appropriate
functions in Unity Catalog, and return the payload needed to call the `anthropic.Anthropic.messages.create` API directly. If there are no tool
calls to be made, this function will return the state of the conversation history up to this point.

Note that the conversation history up until this point (which must start with the initial user input message) is required for this API to function
correctly. Anthropic requires the full scope of the history, including both the tool use request and the tool response messages in order to continue
providing an answer. In the example below, the only history that we have is the original initial user question.

In the example shown here, there are two tool calls that will be requested by the Anthropic model (one for getting the weather in Nome Alaska, the
other for getting the weather in Death Valley California). This utility function will call our Unity Catalog function twice, preserving the tool call
id for each that maps to the `ToolUseBlock`'s `tool_use_id` entry for each call.

```python
from ucai_anthropic.utils import generate_tool_call_messages

# Call the Unity Catalog function and construct the required formatted response history for a subsequent call to Anthropic
tool_messages = generate_tool_call_messages(response=response, client=client, conversation_history=question)

# Call the Anthropic client with the parsed tool response from executing the Unity Catalog function
tool_response = anthropic_client.messages.create(
    model="claude-3-5-sonnet-20240620",
    max_tokens=1024,
    tools=tools,
    messages=tool_messages,
)

print(tool_response)
```

When integrating this logic within your application, keep in mind that Claude may do multi-turn function calling if there are dependencies needed between
function calls. Repeatedly calling `generate_tool_call_messages` with a conditional break if the `stop_reason` in the response is `end_turn` may be
required to answer complex questions that could involve conditionally dependent tools (such as a condition where you have one function that determines th
capital city of a country and another function that fetches the weather within a city, Claude will perform a multi-turn tool use call after the capital
city function's returned value is provided).

- Manually, via the returned values of `extract_tool_call_data`.

**Note** this is a lower-level API and is intended for advanced use cases where logic needs to exist between the tool call request, its response,
and the construction of a subsequent call to Anthropic.

This API is useful if you need to perform validation prior to calling a function or if you prefer to handle the direct return of the Unity Catalog
function call yourself. This lower-level approach and will require a more complex integration with the Anthropic SDK.

```python
from ucai_anthropic.utils import extract_tool_call_data


# This returns a List[ToolCallData] for Anthropic
parsed_messages = extract_tool_call_data(response)

# To see the parsed data that will be submitted for function calling in Unity Catalog:
parsed = [message.to_dict() for message in parsed_messages]

print(parsed)


# To call each tool and provide the formatted response objects:
# Note that you will need to construct the full conversation history to submit to Anthropic if you use this API
# as the return of `to_tool_result_message` contains only the formatted response from a tool call.
results = []
for message in parsed_messages:
    result = message.execute(uc_client)
    results.append(message.to_tool_result_message(result))

print(results)
```
