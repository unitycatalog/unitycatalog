# Using Unity Catalog AI with DSPy

You can use functions defined within Unity Catalog (UC) directly as tools within [DSPy](https://dspy.ai/) with this package. DSPy is a framework for building modular AI applications and optimizing their performance. This integration allows you to seamlessly use Unity Catalog functions as tools in your DSPy workflows.

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
def get_weather(city: str) -> str:
    """Retrieve mock weather information for a given city.

    This function looks up predefined mock weather data for a set of cities.
    If the city is not found in the dataset, a default message is returned.

    Args:
        city (str): The name of the city to retrieve weather data for.

    Returns:
        str: A string describing the weather for the given city, or
        "Weather data not available" if the city is not in the dataset.

    Example:
        >>> get_weather("New York")
        'Sunny, 25°C'

        >>> get_weather("Boston")
        'Weather data not available'
    """
    mock_data = {
        "New York": "Sunny, 25°C",
        "Los Angeles": "Cloudy, 20°C",
        "Chicago": "Rainy, 15°C",
        "Houston": "Thunderstorms, 30°C",
        "Phoenix": "Sunny, 35°C",
    }
    return mock_data.get(city, "Weather data not available")

# Create the function within the Unity Catalog catalog and schema specified
function_info = uc_client.create_python_function(
    func=get_weather,
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
```

#### Create a UC function

To provide an executable function for your tool to use, you need to define and create the function within UC. To do this,
create a Python function that is wrapped within the SQL body format for UC and then utilize the `DatabricksFunctionClient` to store this in UC:

```python
# Replace with your own catalog and schema for where your function will be stored
CATALOG = "catalog"
SCHEMA = "schema"

func_name = f"{CATALOG}.{SCHEMA}.get_weather"
# define the function body in UC SQL functions format
sql_body = f"""CREATE OR REPLACE FUNCTION {func_name}(city STRING COMMENT 'The name of the city to retrieve weather data for.')
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'Retrieve mock weather information for a given city.'
AS $$
    mock_data = {
        "New York": "Sunny, 25°C",
        "Los Angeles": "Cloudy, 20°C",
        "Chicago": "Rainy, 15°C",
        "Houston": "Thunderstorms, 30°C",
        "Phoenix": "Sunny, 35°C"}
    return mock_data.get(city, "Weather data not available")
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
my_tool = toolkit.get_tool(func_name)
if my_tool:
    # Call the tool directly
    result = my_tool.func({"city": "New York"})
    print(result)
else:
    print("Tool not found")
```

### Utilize our function as a tool within a DSPy program

With our interface to our UC function defined as a DSPy tool collection, we can directly use it within a DSPy program application.
Below, we are going to create a ReAct agent and verify that it properly calls our UC function.

```python
import dspy

# Create a ReAct agent with our weather tool
react_agent = dspy.ReAct(
    signature="question -> answer",
    tools= toolkit.tools,
    max_iters=5)

# Example: Ask the agent to reason about weather
result = react_agent(question="What's the weather like in Tokyo?")
print(result.answer)
print("Tool calls made:", result.trajectory)
```


## Features

- **Seamless Integration**: Convert Unity Catalog functions to DSPy tools with minimal configuration
- **Automatic Schema Generation**: Automatically generates input parameter schemas from Unity Catalog function definitions
- **Permission Handling**: Built-in support for filtering functions based on user permissions
- **MLflow Tracing**: Optional MLflow tracing for function execution monitoring
- **Flexible Configuration**: Support for both individual functions and bulk function loading
- **Modern Pydantic**: Uses Pydantic v2 with ConfigDict for better type safety

## API Reference


### UCFunctionToolkit

Main toolkit class for managing Unity Catalog functions as DSPy tools.

**Parameters:**
- `function_names`: List of function names to load (required)
- `client`: Unity Catalog client instance (optional, will use default if not provided)
- `filter_accessible_functions`: Whether to filter by permissions (default: False)

**Properties:**
- `tools`: List of all underlying `dspy.Tool` instances

**Methods:**
- `get_tool(function_name)`: Get a specific underlying `dspy.Tool` by function name

## Advanced Usage

### Custom Function Execution

```python
tool = toolkit.get_tool(func_name)
if tool:
    result = tool.func(city="New York")
```

### Schema Inspection

```python
# Examine the generated schema for a tool
tool = toolkit.get_tool(func_name)
if tool:
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

See the `dspy_databricks_example.ipynb` notebook for complete working examples:

- Basic function execution
- DSPy program integration
- ReAct agent reasoning
- Error handling patterns
- Advanced tool chaining

## Development

### Setup Development Environment

```bash
git clone <repository>
cd ai/integrations/dspy
pip install -e ".[dev]"
```