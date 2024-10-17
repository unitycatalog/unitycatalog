# Welcome to the Unity Catalog AI documentation

## Defining a Client

To interact with Unity Catalog (UC) functions, you must define and initialize a client that manages communication between your application and the UC service. The `DatabricksFunctionClient` provided in the Unity Catalog AI Core Library is designed to facilitate this interaction seamlessly.

### Client initialization

Here is how to initialize a client for connecting to a managed Databricks Unity Catalog:

``` python
from ucai.core.databricks import DatabricksFunctionClient

client = DatabricksFunctionClient(warehouse_id="YOUR_WAREHOUSE_ID")

CATALOG = "my_catalog"
SCHEMA = "my_schema"
```

If you decide to run operations using serverless runtime on Databricks, you do not need to define a `warehouse_id` when creating the client.

#### Gotchas and Best Practices for the Client

- **Warehouse ID Accuracy**

    - Pitfall: Providing an incorrect warehouse_id can lead to authentication failures or inability to execute functions.
    - Solution: Double-check the warehouse_id obtained from your Databricks workspace. Ensure that the warehouse is active and supports serverless compute if required.

- **Serverless Compute Configuration**

    - Pitfall: Attempting to use serverless compute features with a warehouse that doesn't support them will raise errors.
    - Solution: Verify that your specified warehouse has serverless compute enabled. Refer to the Databricks Serverless Compute Guide for setup instructions.

- **Databricks SDK Installation**

    - Pitfall: Missing or outdated databricks-sdk can prevent the client from functioning correctly.
    - Solution: Ensure that you have installed the databricks-sdk as specified in the Prerequisites section. Keep it updated to the latest version to benefit from recent fixes and features.

- **Profile Configuration**

    - Pitfall: Incorrect profile settings can result in connection issues.
    - Solution: If you're using multiple profiles, specify the correct one during client initialization. Ensure that the profile has the necessary permissions to interact with the Unity Catalog.

- **Dependency Management**

    - Pitfall: Missing dependencies like pandas can cause runtime errors when executing functions that require them.
    - Solution: Ensure all necessary dependencies are installed in the environment where the client operates. Use virtual environments to manage dependencies cleanly.

- **Compatibility with Databricks Connect**

    - Pitfall: Using incompatible versions of databricks-connect can lead to execution failures, especially with serverless compute.
    - Solution: Install the specific version of databricks-connect that is supported (15.1.0 or higher) to ensure compatibility with serverless features.


#### Example Usage of the Client

``` python
from ucai.core.databricks import DatabricksFunctionClient

# Initialize the client using serverless
client = DatabricksFunctionClient()

# Define your catalog and schema
CATALOG = "my_catalog"
SCHEMA = "my_schema"

# Verify client setup by listing existing functions
functions = client.list_functions(catalog=CATALOG, schema=SCHEMA)
for func in functions.items:
    print(func.full_name)
```

## Creating Functions

### Using a Python Callable

#### How the `create_python_function` API Works

The `create_python_function` API in Unity Catalog AI provides a way to create Unity Catalog functions (UDFs) from Python functions, while ensuring compatibility with SQL environments. Here’s a comprehensive guide explaining how it works, what is required, and what is not allowed when using this API.

##### Key Concepts

- **Type Hint Requirements**
    - All Python functions passed to `create_python_function` must have explicit type hints for both parameters and return values. These type hints are used to determine the corresponding SQL data types for the UC function.
    - If type hints are missing, the function will not be accepted.
   
- **Supported Python to SQL Type Mapping**
    - `int` → `LONG`
    - `float` → `DOUBLE`
    - `str` → `STRING`
    - `bool` → `BOOLEAN`
    - `Decimal` → `DECIMAL(38,18)` (with default precision and scale)
    - `datetime.date` → `DATE`
    - `datetime.datetime` → `TIMESTAMP`
    - `datetime.timedelta` → `INTERVAL DAY TO SECOND`
    - `list`/`tuple` → `ARRAY`
    - `dict` → `MAP`
    - `bytes` → `BINARY`
    - `None` → `NULL`
   
For example, the following function is valid:

``` python
def add_numbers(x: int, y: float) -> float:
    """Adds two numbers and returns their sum"""
    return x + y
```

- **Docstring Requirement**
    - Functions must have a Google-style docstring to provide additional context for each parameter and the overall function.
    - The docstring should include descriptions for each parameter and the return value, which will be used as metadata in the UC function.

Example:

``` python
def add_numbers(x: int, y: float) -> float:
    """
    Adds two numbers together.

    Args:
        x (int): The first number.
        y (float): The second number.

    Returns:
        float: The sum of the two numbers.
    """
    return x + y
```

- **Variable arguments are not permitted**

Functions containing `*args` or `**kwargs` are not allowed. For example, the following function is invalid:

``` python
def invalid_func(*args):
    return sum(args)
```

- **SQL Function Body Generation**

    - The Python function is converted into a SQL function body behind the scenes. 
    - The `create_python_function` API extracts the function’s name, parameters, return type, and body to generate the corresponding SQL UDF in Unity Catalog.
    - The SQL body is created using a `CREATE FUNCTION` or `CREATE OR REPLACE FUNCTION` statement with the appropriate SQL types based on the Python type hints, depending on whether the `replace` argument is set to `True` or `False`(default)

- **Return Type**

    - The Python function **must** specify a valid return type. If no return type is provided, the function creation will fail.
    - Similar to input types, return types are also mapped from Python to SQL according to the compatibility matrix.

- **Function Replacement**

By default, if a function with the same name already exists in the target catalog and schema, the creation will fail. To replace an existing function, use the `replace=True` argument in the `create_python_function` API call.

- **External Dependencies**

The execution environment within a SQL Warehouse has limited access to Python libraries. Ensure that you have verified that your function is capable of being
executed prior to proceeding with using it in a GenAI solution to reduce the amount of troubleshooting you may have to do.



### Using a SQL body statement

An alternative to using the `create_python_function` API is to use the `create_function` API. This method requires you to submit the full SQL body for the function
creation manually.

For example:

``` python
sql_function_body = f"""CREATE FUNCTION my_catalog.my_schema.weather_func(location STRING COMMENT 'Retrieves the current weather from a provided location.')
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'Returns the current weather from a given location and returns the temperature in degrees Celsius.'
AS $$
    return "31.9 C"
$$
"""

function_info = client.create_function(sql_function_body=sql_function_body)
```

#### Gotchas and Best Practices for SQL body Function Creation

- **SQL Syntax Accuracy**
    - Pitfall: Incorrect SQL syntax can prevent the function from being created successfully.
    - Solution: Validate your SQL statements against Databricks' SQL syntax guidelines. Refer to the CREATE FUNCTION Syntax for proper structure.

- **Function Name Extraction**

    - Pitfall: Improperly formatted function names can lead to errors during extraction and execution.
    - Solution: Ensure that the function name follows the catalog.schema.function_name format without special characters that are not allowed.

- **Parameter and Return Types**

    - Pitfall: Using incompatible types between Python and SQL can cause execution failures.
    - Solution: Align your SQL function's parameter and return types with the supported types outlined in the Type Compatibility Matrix below.

- **Handling Reserved Keywords**

    - Pitfall: Using SQL reserved keywords as function or parameter names can result in syntax errors.
    - Solution: Avoid using reserved keywords or properly escape them using backticks if necessary.

- **Permissions and Access Control**

    - Pitfall: Insufficient permissions can prevent function creation or execution.
    - Solution: Ensure that the user account initializing the DatabricksFunctionClient has the necessary permissions to create and manage functions within the specified catalog and schema.

- **Function Overwriting**

    - Pitfall: Attempting to create a function that already exists without specifying the replace parameter can fail.
    - Solution: Include OR REPLACE in your SQL statement if you intend to overwrite an existing function.

``` python
sql_function_body = f"""CREATE OR REPLACE FUNCTION my_catalog.my_schema.weather_func(location STRING COMMENT 'Retrieves the current weather from a provided location.')
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'Returns the current weather from a given location and returns the temperature in degrees Fahrenheit.'
AS $$
    return "91.9 F"
$$
"""

function_info = client.create_function(sql_function_body=sql_function_body)
```

- **Dependency Management**

    - Pitfall: SQL functions relying on external dependencies or resources not available in the execution environment can fail.
    - Solution: Keep SQL functions self-contained and avoid dependencies on external libraries unless they are accessible within UC.


## Additional Guidance

To read more about the Clients available in this library, see [Client Documentation](client.md)


## Python to SQL Compatibility Matrix

The following table outlines how various Python types are mapped to their corresponding SQL types when using Unity Catalog AI.

| **Python Type**          | **SQL Type**                | **Details**                                                                 |
|--------------------------|-----------------------------|-----------------------------------------------------------------------------|
| `int`                    | `LONG`                      | Represents large integer values.                                            |
| `float`                  | `DOUBLE`                    | Represents double-precision floating-point values.                          |
| `str`                    | `STRING`                    | Represents variable-length string data.                                     |
| `bool`                   | `BOOLEAN`                   | Represents boolean values (`True`/`False`).                                 |
| `datetime.date`          | `DATE`                      | Represents date values (year, month, day).                                  |
| `datetime.datetime`      | `TIMESTAMP`                 | Represents timestamp values (date and time).                                |
| `datetime.timedelta`     | `INTERVAL DAY TO SECOND`    | Represents intervals (difference between two timestamps).                   |
| `decimal.Decimal`        | `DECIMAL(38, 18)`           | Represents fixed-point decimal values with default precision and scale.     |
| `list`, `tuple`          | `ARRAY`                     | Represents an array of elements (must specify the inner type).              |
| `dict`                   | `MAP`                       | Represents key-value pairs (map structure).                                 |
| `bytes`                  | `BINARY`                    | Represents binary data (e.g., files, byte streams).                         |
| `None`                   | `NULL`                      | Represents a null or undefined value.                                       |
