# Using Unity Catalog AI with CrewAI

Integrate Unity Catalog AI with the [CrewAI](https://www.crewai.com/) SDK to utilize functions defined in Unity Catalog (UC) as tools within CrewAI LLM calls. This guide covers installation, setup, caveats, environment variables, public APIs, and examples to help you get started.

---
  
## Installation

Install the Unity Catalog AI CrewAI integration from PyPI:

```sh
pip install unitycatalog-crewai
```

## Prerequisites

- **Python version**: Python 3.10 or higher is required.

### Unity Catalog

Ensure that you have a functional UC server set up and that you are able to access the catalog and schema where defined functions are stored.

### Databricks Unity Catalog

To interact with Databricks Unity Catalog, install the optional package dependency when installing the integration package:

```sh
pip install unitycatalog-crewai[databricks]
```

#### Authentication with Databricks Unity Catalog

To use Databricks-managed Unity Catalog with this package, follow the [Databricks CLI authentication instructions](https://docs.databricks.com/en/dev-tools/cli/authentication.html#authentication-for-the-databricks-cli) to authenticate to your workspace and ensure that your access token has workspace-level privileges for managing UC functions.

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

Initialize a client for managing Unity Catalog functions in a Databricks workspace and set it as the global client.

```python
from unitycatalog.ai.core.base import set_uc_function_client
from unitycatalog.ai.core.databricks import DatabricksFunctionClient

client = DatabricksFunctionClient()

# Set the default UC function client
set_uc_function_client(client)
```

### Create a UC function to use as a tool

Define and create Python functions that will be stored in Unity Catalog

```python
# Replace with your own catalog and schema where your function will be stored
CATALOG = "your_catalog"
SCHEMA = "your_schema"
function_name = f"{CATALOG}.{SCHEMA}.make_uppercase"

def make_uppercase(s: str) -> str:
    """
    Convert the string to uppercase.
    """
    return s.upper()

# Create UC function
response = client.create_python_function(
    func=make_uppercase,
    catalog=CATALOG,
    schema=SCHEMA
)
```

### Creating an Instance of a CrewAI-Compatible Tool

[CrewAI Tools](https://docs.crewai.com/core-concepts/Tools/) are callable external functions that GenAI applications can use (invoked by an LLM), which are exposed with a UC interface through the use of the `unitycatalog-crewai` package via the `UCFunctionToolkit` API.

```python
from unitycatalog.ai.crewai.toolkit import UCFunctionToolkit

# Pass the UC function name that we created to the constructor
toolkit = UCFunctionToolkit(function_names=[function_name])

# Get the CrewAI-compatible tools definitions
tools = toolkit.tools
```

### Validating the tool

Before integrating the tool with CrewAI, you can validate its functionality by calling it directly:

```python
my_tool = tools[0]

# Call the tool function with the necessary arguments
result = my_tool.fn(**{"s": "lowercase string"})
print(result)  # Output: "LOWERCASE STRING"
```

### Utilizing the Function as a Tool within a CrewAI 'Crew'

With the interface to your UC function defined as a CrewAI tool collection, you can directly use it within a CrewAI `Crew`. This involves setting up agents, defining tasks, and executing the crew to perform desired operations using the integrated tools.

```python
import os
from crewai import Agent, Task, Crew

# Set up API keys
os.environ["OPENAI_API_KEY"] = "your_openai_api_key"

# Create agents
coder = Agent(
    role="Simple Coder",
    goal="Create a program that prints 'Hello Unity Catalog!'",
    backstory="Enjoys solving coding challenges",
    expected_output="string",
    tools=tools,
    verbose=True
)

reviewer = Agent(
    role="Reviewer",
    goal="Ensure the coder calls the function and verifies the output",
    backstory="Meticulous and detail-oriented",
    expected_output="string",
    verbose=True
)

# Define tasks
research = Task(
    description="Call a tool to transform a string to uppercase",
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

An example output:

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
