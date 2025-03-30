# Using Unity Catalog AI with Semantic Kernel

You can use the Unity Catalog AI package with Semantic Kernel to utilize functions that are defined in Unity Catalog as tools within Semantic Kernel applications.

## Prerequisites
- Python 3.10 or later
- Access to Unity Catalog or Databricks workspace

## Installation

### Client Library

To install the Unity Catalog function client SDK and the Semantic Kernel integration, simply install from PyPI:

```sh
pip install unitycatalog-semantic-kernel
```

If you are working with **Databricks Unity Catalog**, you can install the optional package:

```sh
pip install unitycatalog-semantic-kernel[databricks]
```

## Getting started

### Creating a Unity Catalog Client

To interact with your Unity Catalog server, initialize the `UnitycatalogFunctionClient` as shown below:

```python
from unitycatalog.ai.core.client import UnitycatalogFunctionClient
from unitycatalog.client import ApiClient, Configuration

# Configure the Unity Catalog API client
config = Configuration(
    host="http://localhost:8080/api/2.1/unity-catalog"  # Replace with your UC server URL
)

# Initialize the ApiClient
api_client = ApiClient(configuration=config)

# Instantiate the UnitycatalogFunctionClient
uc_client = UnitycatalogFunctionClient(api_client=api_client)

# Example catalog and schema names
CATALOG = "my_catalog"
SCHEMA = "my_schema"
```

### Databricks-managed Unity Catalog

To use Databricks-managed Unity Catalog with this package, follow the [instructions](https://docs.databricks.com/en/dev-tools/cli/authentication.html#authentication-for-the-databricks-cli) to authenticate to your workspace and ensure that your access token has workspace-level privilege for managing UC functions.

#### Client setup

Initialize a client for managing UC functions in a Databricks workspace.

```python
from unitycatalog.ai.core.databricks import DatabricksFunctionClient
# Initialize the Databricks client
client = DatabricksFunctionClient(profile="<profile>")

CATALOG = "AICatalog"
SCHEMA = "AISchema"

```

### Creating a Unity Catalog Function

You can create a UC function by providing a Python callable. Below is an example (recommended) of using the `create_python_function` API that accepts a Python callable (function) as input.

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

## Using the Function as a Semantic Kernel Tool

### Create a UCFunctionToolkit instance

To begin, we will need an instance of the tool function interface from the `unitycatalog.ai.semantic_kernel.toolkit` module.

```python
from unitycatalog.ai.semantic_kernel.toolkit import UCFunctionToolkit

# Create an instance of the toolkit with the function that was created earlier.
toolkit = UCFunctionToolkit(
    function_names=[f"{CATALOG}.{SCHEMA}.add_numbers"],
    client=client
)
```

### Use the tools with Semantic Kernel

Now, let's use these Unity Catalog functions as plugins within a Semantic Kernel application:

```python
import os
from semantic_kernel import Kernel
from semantic_kernel.connectors.ai.function_choice_behavior import FunctionChoiceBehavior
from semantic_kernel.connectors.ai.open_ai import OpenAIChatCompletion
from semantic_kernel.connectors.ai.prompt_execution_settings import PromptExecutionSettings
from semantic_kernel.contents.chat_history import ChatHistory

# Initialize the kernel with an AI service
kernel = Kernel()

# Add chat completion service
chat_completion_service = OpenAIChatCompletion(
    ai_model_id="gpt-4", api_key=os.getenv("OPENAI_API_KEY")
)

# Set up execution settings
settings = PromptExecutionSettings(
    function_choice_behavior=FunctionChoiceBehavior.Auto(),
)

# Register Unity Catalog functions with the kernel
toolkit.register_with_kernel(kernel, plugin_name="calculator")

# Create a chat prompt
chat_prompt = """
You are a helpful calculator assistant. Use the calculator tools to answer questions about numbers.

Question: What is 49 + 82?
"""

# Register Unity Catalog functions with the kernel
toolkit.register_with_kernel(kernel, plugin_name="calculator")

# Create chat history
chat_history = ChatHistory()
chat_history.add_user_message(
    """You are a helpful calculator assistant. Use the calculator tools to answer questions about numbers.
    Question: What is 49 + 82?"""
)

# Process the chat interaction
# Note: This is an async operation
response = await chat_completion_service.get_chat_message_content(
    chat_history, settings, kernel=kernel
)

print("\nFinal Response:", response) 
```

### Showing Details of the Tool Call

You can review the conversation history and see how the LLM decided to call the function:

```python
print("\nChat History:") 
print("-" * 80)  
for message in chat_history.messages:
    print(f"Role: {message.role}") 
    print(f"Content: {message.content}")  
    if message.content == "":
        print(f"Details: {message.items}")  
    print("-" * 80)  
```

## Advanced Features

### Filtering Accessible Functions

The toolkit can filter functions based on the user's permissions:

```python
toolkit = UCFunctionToolkit(
    function_names=[f"{CATALOG}.{SCHEMA}.add_numbers"],
    client=client,
    filter_accessible_functions=True
)
```

### Working with Multiple Function Sets

You can create multiple plugins for different sets of Unity Catalog functions:

```python
# Calculator-related functions
calculator_toolkit = UCFunctionToolkit(
    function_names=[f"{CATALOG}.{SCHEMA}.add_numbers", f"{CATALOG}.{SCHEMA}.multiply_numbers"],
    client=client
)

# Register the function set with a specific plugin name
calculator_toolkit.register_with_kernel(kernel, plugin_name="calculator")
```

This approach organizes your Unity Catalog functions into logical groups that can be used contextually in your Semantic Kernel applications.

### Configurations for Databricks-only UC function execution

We provide configurations for the Databricks Client to control the function execution behaviors, check [function execution arguments section](../../core/README.md#function-execution-arguments-configuration).
