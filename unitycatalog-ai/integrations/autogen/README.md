# Using Unity Catalog AI with the Autogen SDK

You can use the Unity Catalog AI package with the autogen SDK to utilize functions that are defined in Unity Catalog to be used as tools within autogen LLM calls.

## Installation

```sh
# install from the source
pip install git+https://github.com/puneet-jain159/unitycatalog.git@autogen_ucai#subdirectory=unitycatalog-ai/integrations/autogenn
```

> [!NOTE]
> Once this package is published to PyPI, users can install via `pip install ucai-autogen`

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

Now that the function is created and stored in the corresponding catalog and schema, we can use it within autogen's SDK.

#### Create a UCFunctionToolkit instance


To begin, we will need an instance of the tool function interface from the `ucai_autogen` toolkit.

```python
from ucai_autogen.toolkit import UCFunctionToolkit

# Create an instance of the toolkit with the function that was created earlier.
toolkit = UCFunctionToolkit(function_names=[f"{CATALOG}.{SCHEMA}.python_exec"], client=client)

# Access the tool definitions that are in the interface that autogen's SDK expects
tools = tookit.tools

```

Now that we have the defined tools from Unity Catalog, we can directly pass this definition into a messages request.

If you would like to validate that your tool is functional prior to proceeding to integrate it with LlamaIndex, you can call the tool directly:

```python
my_tool = tools[0]

my_tool.fn(**{"location": "San Franscisco"})
```

#### Use the tools with a conversable Agent

```python

import os
from autogen import ConversableAgent


OPENAI_API_KEY ='.....'

# Let's first define the assistant agent that suggests tool calls.
assistant = ConversableAgent(
    name="Assistant",
    system_message="You are a helpful AI assistant. "
    "You can tell the temperature of a location "
    "Return 'TERMINATE' when the task is done.",
    llm_config={"config_list": [{"model": "gpt-4", "api_key": OPENAI_API_KEY}]},
)

# The user proxy agent is used for interacting with the assistant agent
# and executes tool calls.
user_proxy = ConversableAgent(
    name="User",
    llm_config=False,
    is_termination_msg=lambda msg: msg.get("content") is not None and "TERMINATE" in msg["content"],
    human_input_mode="NEVER",
)

```

Once you have created a tool, you can register it with the agents that are involved in conversation.

Similar to code executors, a tool must be registered with at least two agents for it to be useful in conversation. The agent which can call the tool and and Agent whic can execute the tool’s function.

you can use `register_function` method to register a tool with both agents at once.
below is an example

```python

for tool in tools:
    tool.register_function(assistant,user_proxy)

```

#### Calling the function

```python

chat_result = user_proxy.initiate_chat(assistant, message="what is the temperature in SF?")

```

