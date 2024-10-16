# 🦙 Using Unity Catalog AI with LlamaIndex

You can use functions defined within Unity Catalog (UC) directly as tools within [LlamaIndex](https://docs.llamaindex.ai/en/stable/) with this package.

## Installation

### From PyPI

```sh
pip install ucai-llamaindex
```

### From source

To get started with the latest version, you can directly install this package from source via:

```sh
pip install git+https://github.com/unitycatalog/unitycatalog.git#subdirectory=unitycatalog-ai/integrations/llama_index
```

## Getting started

### Databricks managed UC

To use Databricks-managed UC with this package, follow the [instructions here](https://docs.databricks.com/en/dev-tools/cli/authentication.html#authentication-for-the-databricks-cli) to authenticate to your workspace and ensure that your access token has workspace-level privilege for managing UC functions.

#### Client setup

Initialize a client for managing UC functions in a Databricks workspace, and set it as the global client.

```python
from ucai.core.client import set_uc_function_client
from ucai.core.databricks import DatabricksFunctionClient

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

Now that the function exists within the Catalog and Schema that we defined, we can interface with it from llamaindex using the ucai_llamaindex package.

#### Create an instance of a LlamaIndex compatible tool

[LlamaIndex Tools](https://docs.llamaindex.ai/en/stable/module_guides/deploying/agents/tools/) are callable external functions that GenAI applications (called by
an LLM), which are exposed with a UC interface through the use of the ucai_llamaindex package via the `UCFunctionToolkit` API.

```python
from ucai_llamaindex.toolkit import UCFunctionToolkit

# Pass the UC function name that we created to the constructor
toolkit = UCFunctionToolkit(function_names=[func_name])

# Get the LlamaIndex-compatible tools definitions
tools = toolkit.tools
```

If you would like to validate that your tool is functional prior to proceeding to integrate it with LlamaIndex, you can call the tool directly:

```python
my_tool = tools[0]

my_tool.fn(**{"code": "print(1)"})
```

#### Utilize our function as a tool within a ReActAgent in LlamaIndex

With our interface to our UC function defined as a LlamaIndex tool collection, we can directly use it within a LlamaIndex agent application.
Below, we are going to create a simple `ReActAgent` and verify that our agent properly calls our UC function.

```python
from llama_index.llms.openai import OpenAI
from llama_index.core.agent import ReActAgent

llm = OpenAI()

agent = ReActAgent.from_tools(tools, llm=llm, verbose=True)

agent.chat("Please call a python execution tool to evaluate the result of 42 + 97.")
```