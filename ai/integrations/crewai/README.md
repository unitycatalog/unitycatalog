# Using Unity Catalog AI with CrewAI

You can use functions defined within Unity Catalog (UC) directly as tools within [CrewAI](https://www.crewai.com/) with this package.

## Installation

### Client Library

To install the Unity Catalog function client SDK and the `CrewAI` integration, simply install from PyPI:

```sh
pip install unitycatalog-crewai
```

If you are working with **Databricks Unity Catalog**, you can install the optional package:

```sh
pip install unitycatalog-crewai[databricks]
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
function_name = f"{CATALOG}.{SCHEMA}.make_uppercase"

def make_uppercase(s: str) -> str:
    """
    Convert the string to uppercase.
    """
    return s.upper()

response = client.create_python_function(func=make_uppercase, catalog=CATALOG, schema=SCHEMA)
```

Now that the function exists within the Catalog and Schema that we defined, we can interface with it from CrewAI using the `unitycatalog-crewai` package.

## Using the Function as a GenAI Tool

### Create a UCFunctionToolkit instance

[CrewAI Tools](https://docs.crewai.com/core-concepts/Tools/) are callable external functions that GenAI applications can use (called by
an LLM), which are exposed with a UC interface through the use of the `unitycatalog-crewai` package via the `UCFunctionToolkit` API.

```python
from unitycatalog.ai.crewai.toolkit import UCFunctionToolkit

# Pass the UC function name that we created to the constructor
toolkit = UCFunctionToolkit(function_names=[function_name])

# Get the CrewAI-compatible tools definitions
tools = toolkit.tools
```

If you would like to validate that your tool is functional prior to integrating it with CrewAI, you can call the tool directly:

```python
my_tool = tools[0]

my_tool.fn(**{"s": "lowercase string"})
```

### Utilize our function as a tool within a CrewAI `Crew`

With our interface to our UC function defined as a CrewAI tool collection, we can directly use it within a CrewAI `Crew`. 

```python
import os
from crewai import Agent, Task, Crew

# Set up API keys
os.environ["OPENAI_API_KEY"] = "your key"

# Create agents
coder = Agent(
    role="Simple coder",
    goal= "Create a program that prints Hello Unity Catalog!",
    backstory="likes long walks on the beach",
    expected_output="string",
    tools=tools,
    verbose=True
)

reviewer = Agent(
    role="reviewer",
    goal="Ensure the researcher calls a function and shows the answer",
    backstory="allergic to cats",
    expected_output="string",
    verbose=True
)

# Define tasks
research = Task(
    description="Call a tool",
    expected_output="string",
    agent=coder
)

review = Task(
    description="Review the tool call output. Once complete, stop.",
    expected_output="string",
    agent=reviewer,
)

# Assemble a crew with planning enabled
crew = Crew(
    agents=[coder, reviewer],
    tasks=[research, review],
    verbose=True,
    planning=True,  # Enable planning feature
)

# Execute tasks
crew.kickoff()
```

Output
```text
[2024-10-08 14:29:25][INFO]: Planning the crew execution

# Agent: Simple coder
## Task: Call a tool1. **Agent Identification**: Identify the agent responsible for this task, which is "Simple Coder".

2. **Agent Goal Confirmation**: Confirm the goal of the Simple Coder is to create a program that prints "Hello Unity Catalog!".

3. **Tool Selection**: The appropriate tool to use is the `UnityCatalogTool` named `main__default__make_uppercase`.

4. **Initial Setup**: Ensure the Simple Coder has access to the necessary environment to write and execute code.

5. **Code Implementation**: The Simple Coder will write the following code snippet:
   \`\`\`python
   def main():
       print("Hello Unity Catalog!")
   
   main()
   \`\`\`
   - The above code fulfills the requirement of printing "Hello Unity Catalog!" as expected.

6. **Using the Tool**: The Simple Coder will then use the `UnityCatalogTool` to convert the string "Hello Unity Catalog!" to uppercase:
   - Prepare the arguments schema for the tool:
     - `s: "Hello Unity Catalog!"`.
   - Call the tool function:
   \`\`\`python
   result = main__default__make_uppercase(s="Hello Unity Catalog!")
   \`\`\`

7. **Output Handling**: The Simple Coder will ensure that the result of the tool call is stored and can be reviewed.



# Agent: Simple coder
## Thought: I will print "Hello Unity Catalog!" and then convert that string to uppercase using the specified tool.
## Using tool: main__default__make_uppercase
## Tool Input: 
"{\"s\": \"Hello Unity Catalog!\"}"
## Tool Output: 
{"format": "SCALAR", "value": "HELLO UNITY CATALOG!", "truncated": false}


# Agent: Simple coder
## Final Answer: 
HELLO UNITY CATALOG!


# Agent: reviewer
## Task: Review the tool call output. Once complete, stop.1. **Agent Identification**: Identify the agent responsible for this task, which is "Reviewer".

2. **Expected Outcome Review**: The Reviewer’s goal is to ensure that the Simple Coder has called the function correctly and that it outputs the expected answer.

3. **Output Review Process**: The Reviewer will retrieve the output generated by the Simple Coder's tool call to `main__default__make_uppercase`.
   - The expected output should be the string "HELLO UNITY CATALOG!" since it is the uppercase conversion of the input.

4. **Assessment**: The Reviewer will compare the received output against the expected output:
   - If the output matches the expected output ("HELLO UNITY CATALOG!"), then the function call is verified as successful.
   - If there is a discrepancy, the Reviewer will notify the Simple Coder to correct the issue.

5. **Completion**: Once the review is complete and the expected output is confirmed:
   - The Reviewer will log the result and confirm the completion of the task.
   - The Reviewer will stop any further actions as the task is finished.


# Agent: reviewer
## Final Answer: 
The output generated by the Simple Coder's tool call to `main__default__make_uppercase` has been reviewed. The expected output is "HELLO UNITY CATALOG!" which matches the received output perfectly. Therefore, the function call has been verified as successful, confirming that the task is complete. The result has been logged accordingly, and no further actions are needed as the task is finished.
```

### Configurations for Databricks managed UC functions execution

We provide configurations for databricks client to control the function execution behaviors, check [function execution arguments section](../../README.md#function-execution-arguments-configuration).
