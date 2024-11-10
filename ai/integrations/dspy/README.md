# Using Unity Catalog AI with the DSPy SDK

You can use the Unity Catalog AI package with the DSPy integration to utilize functions that are defined in Unity Catalog to be used as tools within [DSPy](https://dspy-docs.vercel.app/intro/) LM calls.

## Installation

```sh
# install from the source
pip install git+https://github.com/unitycatalog/unitycatalog.git#subdirectory=unitycatalog-ai/integrations/dspy
```

> [!NOTE]
> Once this package is published to PyPI, users can install via `pip install unitycatalog-dspy`

## Get started

### Databricks-managed UC

To use Databricks-managed Unity Catalog with this package, follow the [instructions](https://docs.databricks.com/en/dev-tools/cli/authentication.html#authentication-for-the-databricks-cli) to authenticate to your workspace and ensure that your access token has workspace-level privilege for managing UC functions.

#### Client setup

Initialize a client for managing UC functions in a Databricks workspace, and set it as the global client.

```python
from unitycatalog.ai.core.client import set_uc_function_client
from unitycatalog.ai.core.databricks import DatabricksFunctionClient

client = DatabricksFunctionClient(
    warehouse_id="..." # replace with the warehouse_id
)

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

def yes_or_no_answer(question: str) -> str:
    """
    Return the answer to a yes or no question.

    Args:
        question (str): Name of the yes or no question.

    Returns:
        A string of the answer.
    """
    import random
    return random.choice([
        "Yes", "No", "Maybe", "Definitely", "Absolutely not", "It is certain", "Very doubtful", 
        "Signs point to yes", "Cannot predict now", "You smell bad", "Don't count on it", 
        "Yes, in due time", "My sources say no", "You may rely on it", "Better not tell you now"
    ])

response = client.create_python_function(func=yes_or_no_answer, catalog=CATALOG, schema=SCHEMA)
```

Now that the function exists within the Catalog and Schema that we defined, we can interface with it from DSPy using the `unitycatalog-dspy` package.

#### Create an instance of a DSPy compatible tool

DSPy Tools are primarily used with [ReAct Agents](https://dspy-docs.vercel.app/deep-dive/modules/react/). In short, they are callable external functions that GenAI applications can be called by. Through the us of the `unitycatalog-dspy` package via the `UCFunctionToolkit` API, the below code snippet exposes UC functions as DSPy-compatible tools. 

```python
from unitycatalog.ai.dspy.toolkit import UCFunctionToolkit

# Pass the UC function name that we created to the constructor
toolkit = UCFunctionToolkit(function_names=[f"{CATALOG}.{SCHEMA}.yes_or_no_answer"])

# Get the DSPy-compatible tools definitions
tools = toolkit.tools
```

If you would like to validate that your tool is functional prior to integrating it with DSPy, you can call the tool directly:

```python
my_tool = tools[0]

my_tool.fn(**{"question": "Am I good at coding?"})
```

Output
```text
'{"format": "SCALAR", "value": "My sources say no", "truncated": false}'
```

#### Utilize our function as a tool within a DSPy Completion Call

With our interface to our UC function defined as a JSON tool collection, we can directly use it within a DSPy LM call.

```python
import dspy 

# Set up API keys
import dspy 

# Set up API keys
import os
os.environ["OPENAI_API_KEY"] = "your key"

# Configure our LLM in OpenAI
lm = dspy.LM('openai/gpt-4o-mini')
dspy.configure(lm=lm)

# Define a simple signature for basic question answering
class BasicQA(dspy.Signature):
    """Answer questions with short factoid answers."""
    question = dspy.InputField()
    answer = dspy.OutputField(desc="often between 1 and 5 words")

# Define the react agent
react_module = dspy.ReAct(BasicQA, tools=tools)

# Define your request 
question = "Does my cat actually love me? I worry that he just uses me for food and shelter."

# Show the response
response = react_module(question=question)

print("\nReAct Agent Response:\n", response)
```

Output
```text
ReAct Agent Response:
Prediction(
    trajectory={
        "thought_0": "Cats can show affection in various ways, such as purring, kneading, and following you around. It's possible that your cat loves you, even if it sometimes seems like they are just after food and shelter.",
        "tool_name_0": "main__default__yes_or_no_answer",
        "tool_args_0": {"question": "Does my cat actually love me?"},
        "observation_0": '{"format": "SCALAR", "value": "Maybe", "truncated": false}',
        "thought_1": 'While the answer is "Maybe," it\'s important to consider the signs of affection your cat shows. Cats can form strong bonds with their owners, and their behavior can indicate love.',
        "tool_name_1": "finish",
        "tool_args_1": {},
        "observation_1": "Completed.",
    },
    reasoning="Cats can show affection in various ways, such as purring, kneading, and following you around. While it may seem like they are just after food and shelter, these behaviors can indicate a bond and love for their owners. Therefore, it's possible that your cat does love you, even if it sometimes appears otherwise.",
    answer="Maybe",
)
```
