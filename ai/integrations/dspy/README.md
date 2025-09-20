# Using Unity Catalog AI with DSPy

You can use functions defined within Unity Catalog (UC) directly as tools within [DSPy](https://dspy.ai/) with this package. DSPy is a framework for building modular AI applications and optimize their performance. This integration allows you to seamlessly use Unity Catalog functions as tools in your DSPy workflows.

## Installation

### Client Library

To install the Unity Catalog function client SDK and the `DSPy` integration, simply install from PyPI:

```sh
pip install unitycatalog-dspy
```

If you are working with **Databricks Unity Catalog**, you can install the optional package:

```sh
pip install unitycatalog-dspy[databricks]
```

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

To use Databricks-managed UC with this package, follow the [instructions here](https://docs.databricks.com/en/dev-tools/cli/authentication.html#authentication-for-the-databricks-cli) to authenticate to your workspace and ensure that your access token has workspace-level privilege for managing UC functions.

#### Client setup

Initialize a client for managing UC functions in a Databricks workspace, and set it as the global client.

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
CATALOG = "catalog"
SCHEMA = "schema"

func_name = f"{CATALOG}.{SCHEMA}.python_exec"
# define the function body in UC SQL functions format
sql_body = f"""CREATE OR REPLACE FUNCTION {func_name}(code STRING COMMENT 'Python code to execute. Remember to print the final result to stdout.')
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'Executes Python code and returns its stdout.'
AS $$
    import sys
    from io import StringIO
    stdout = StringIO()
    sys.stdout = stdout
    exec(code)
    return stdout.getvalue()
$$
"""

client.create_function(sql_function_body=sql_body)
```

Now that the function exists within the Catalog and Schema that we defined, we can interface with it from DSPy using the `unitycatalog.ai.dspy` package.

## Using the Function as a GenAI Tool

### Create a UCFunctionToolkit instance

[DSPy Tools](https://dspy-docs.vercel.app/docs/reference/dspy.adapters.Tool) are callable external functions that GenAI applications (called by
an LLM), which are exposed with a UC interface through the use of the `unitycatalog.ai.dspy` package via the `UCFunctionToolkit` API.

```python
from unitycatalog.ai.dspy.toolkit import UCFunctionToolkit

# Pass the UC function name that we created to the constructor
toolkit = UCFunctionToolkit(function_names=[func_name])

# Get the DSPy-compatible tools definitions
tools = toolkit.tools
```

If you would like to validate that your tool is functional prior to proceeding to integrate it with DSPy, you can call the tool directly:

```python
my_tool_wrapper = tools[func_name]
my_tool = my_tool_wrapper.tool

# Call the tool directly
result = my_tool.func({"code":"print(1)"})
print(result)
```

### Utilize our function as a tool within a DSPy program

With our interface to our UC function defined as a DSPy tool collection, we can directly use it within a DSPy program application.
Below, we are going to create a simple DSPy program and verify that it properly calls our UC function.

```python
import dspy

class SimpleToolProgram(dspy.Module):
    """A simple DSPy program that uses Unity Catalog functions as tools."""
    
    def __init__(self, tool):
        super().__init__()
        self.tool = tool
    
    def forward(self, **kwargs):
        """Execute the tool with the given parameters."""
        try:
            result = self.tool.fn(**kwargs)
            return result
        except Exception as e:
            return f"Error executing tool: {e}"

# Create an instance of the program
program = SimpleToolProgram(my_tool)

# Execute the program
result = program(code="print('Hello from DSPy!')")
print(f"Program result: {result}")
```

### Advanced: Creating a DSPy Chain

You can create more complex DSPy programs that chain multiple operations together:

```python
class AdvancedToolChain(dspy.Module):
    """An advanced DSPy program that chains multiple tool operations."""
    
    def __init__(self, toolkit):
        super().__init__()
        self.toolkit = toolkit
    
    def forward(self, operation_type: str, **params):
        """Execute different operations based on the operation type."""
        
        if operation_type == "data_processing":
            # Use a specific tool for data processing
            tool_wrapper = self.toolkit.tools_dict.get(func_name)
            if tool_wrapper:
                return tool_wrapper.tool.fn(**params)
            else:
                return "Data processing tool not available"
        
        elif operation_type == "analysis":
            # Use a different tool for analysis
            # Add your analysis tool logic here
            return "Analysis operation completed"
        
        else:
            return f"Unknown operation type: {operation_type}"

# Create and use the advanced chain
advanced_chain = AdvancedToolChain(toolkit)
result = advanced_chain(operation_type="data_processing", code="print('Processing data...')")
print(result)
```

### Loading All Accessible Functions

You can also load all functions that your client has access to, which is useful for discovery and development:

```python
# Load all accessible functions
all_functions_toolkit = UCFunctionToolkit(
    function_names=[],  # Empty list to load all
    client=client,
    filter_accessible_functions=True
)

print(f"Loaded {len(all_functions_toolkit.tools_dict)} accessible functions")
for func_name, tool_wrapper in all_functions_toolkit.tools_dict.items():
    print(f"  - {func_name}: {tool_wrapper.tool.name}")
```

## Features

- **Seamless Integration**: Convert Unity Catalog functions to DSPy tools with minimal configuration
- **Automatic Schema Generation**: Automatically generates input parameter schemas from Unity Catalog function definitions
- **Permission Handling**: Built-in support for filtering functions based on user permissions
- **MLflow Tracing**: Optional MLflow tracing for function execution monitoring
- **Flexible Configuration**: Support for both individual functions and bulk function loading
- **Modern Pydantic**: Uses Pydantic v2 with ConfigDict for better type safety

## API Reference

### UnityCatalogDSPyToolWrapper

A Pydantic model representing a Unity Catalog function as a DSPy tool wrapper.

**Attributes:**
- `tool`: The underlying dspy.adapters.Tool instance
- `uc_function_name`: The full Unity Catalog function name
- `client_config`: Client configuration dictionary

**Methods:**
- `to_dict()`: Convert wrapper to dictionary representation

### UCFunctionToolkit

Main toolkit class for managing Unity Catalog functions as DSPy tools.

**Parameters:**
- `function_names`: List of function names to load (required)
- `client`: Unity Catalog client instance (optional, will use default if not provided)
- `filter_accessible_functions`: Whether to filter by permissions (default: False)
- `tools_dict`: Internal tools dictionary (auto-populated)

**Methods:**
- `tools_dict`: Property returning all available tools as a dictionary
- `get_tool(function_name)`: Get a specific tool by function name
- `add_function(function_name)`: Add a new function to the toolkit
- `remove_function(function_name)`: Remove a function from the toolkit

## Advanced Usage

### Custom Function Execution

```python
# Access the underlying function execution
tool_wrapper = toolkit.tools_dict[func_name]
tool = tool_wrapper.tool
result = tool.fn(param1="value1", param2="value2")
```

### Schema Inspection

```python
# Examine the generated schema for a tool
tool_wrapper = toolkit.tools_dict[func_name]
tool = tool_wrapper.tool
print(f"Tool name: {tool.name}")
print(f"Tool description: {tool.desc}")
print(f"Tool arguments: {tool.args}")
print(f"Tool argument types: {tool.arg_types}")
```

### Error Handling

```python
try:
    toolkit = UCFunctionToolkit(
        function_names=[func_name],
        client=client
    )
except PermissionError as e:
    print(f"Permission denied: {e}")
except ValueError as e:
    print(f"Invalid configuration: {e}")
```

### MLflow Integration

The toolkit automatically enables MLflow tracing when available:

```python
# MLflow tracing is automatically enabled if configured
toolkit = UCFunctionToolkit(
    function_names=[func_name],
    client=client
)
```

### Configurations for Databricks managed UC functions execution

We provide configurations for databricks client to control the function execution behaviors, check [function execution arguments section](../../README.md#function-execution-arguments-configuration).

## Examples

See the `examples/` directory and the `dspy_databricks_example.ipynb` notebook for complete working examples:

- Basic function execution
- DSPy program integration
- Error handling patterns
- Performance optimization
- Advanced tool chaining

## Development

### Setup Development Environment

```bash
git clone <repository>
cd ai/integrations/dspy
pip install -e ".[dev]"
```