# Using Unity Catalog AI with the Gemini SDK

You can use the Unity Catalog AI package with the Gemini SDK to utilize functions that are defined in Unity Catalog to be used as tools within Gemini LLM calls.

## Installation

To use this package with **Unity Catalog**, you will need to install:

```sh
pip install unitycatalog-gemini
```

## Prerequisites

- **Python version**: Python 3.10 or higher is required.

### Unity Catalog

Ensure that you have a functional UC server set up and that you are able to access the catalog and schema where defined functions are stored.

### Databricks Unity Catalog

To interact with Databricks Unity Catalog, install the optional package dependency when installing the integration package:

```sh
pip install unitycatalog-gemini[databricks]
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

### Creating a Unity Catalog Function

You can create a UC function either by providing a Python callable or by submitting a `FunctionInfo` object. Below is an example (recommended) of using the `create_python_function` API that accepts a Python callable (function) as input.

To create a UC function from a Python function, define your function with appropriate type hints and a Google-style docstring:

```python
CATALOG = "your_catalog"
SCHEMA = "your_schema"

func_name = f"{CATALOG}.{SCHEMA}.add_numbers"

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

### Create a UCFunctionToolkit instance

Tool use through the [Google GenAI SDK](https://ai.google.dev/gemini-api/docs) allows you to connect external client-side tools and
functions to provide [Gemini](https://ai.google.dev/gemini-api/docs/models/gemini-v2) with a greater range of capabilities to augment its ability to respond to user messages.

To begin, we will need an instance of the tool function interface from the `unitycatalog.ai.gemini` toolkit.

```python
from unitycatalog.ai.gemini.toolkit import UCFunctionToolkit

# Create an instance of the toolkit with the function that was created earlier.
toolkit = UCFunctionToolkit(function_names=[func_name], client=client)

# Access the tool definitions that are in the interface that Gemini's SDK expects
tools = toolkit.generate_callable_tool_list()
```

Now that we have the defined tools from Unity Catalog, we can directly pass this definition into a messages request.

### Use the tools within a request to Gemini models

When you send a query to the Gemini model, it will automatically detect if it needs to call a tool (your UC function) to answer the question:

```python
# Interface with Gemini via their SDK
from google.generativeai import GenerativeModel

multi = "What is 49 + 82?"

model = GenerativeModel(
    model_name="gemini-2.0-flash-exp", tools=tools
)

chat = model.start_chat(enable_automatic_function_calling=True)

response = chat.send_message(multi)
print(response)
```

### Showing Details of the Tool Call

You can review the conversation history and see how the LLM decided to call the function:

```python
for content in chat.history:
    print(content.role, "->", [type(part).to_dict(part) for part in content.parts])
    print("-" * 80)
```

## Manually execute function calls

if you prefer more control, you can manually detect and execute function calls:

```python
from google.generativeai.types import content_types
from unitycatalog.ai.gemini.utils import get_function_calls,generate_tool_call_messages

history = []
question = "What is 23 + 99?"


content = content_types.to_content(question)
if not content.role:
    content.role = "user"

history.append(content)

response = model.generate_content(
   history)
while function_calls := get_function_calls(response):
    history , function_calls = generate_tool_call_messages(model=model ,response= response ,conversation_history = history )

    response = model.generate_content(history)

response
```
