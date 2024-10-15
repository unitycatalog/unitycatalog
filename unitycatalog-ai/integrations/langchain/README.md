# 🦜🔗 Using Unity Catalog AI with Langchain

Integrate Unity Catalog AI package with Langchain to allow seamless usage of UC functions as tools in agents application.

## Installation

```sh
# install from the source
pip install git+ssh://git@github.com/serena-ruan/unitycatalog-ai.git#subdirectory=integrations/langchain
```

> [!NOTE]
> Once this package is published to PyPI, users can install via `pip install ucai-langchain`

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

Now the function is created and stored in the corresponding catalog and schema.

#### Create an instance of a LangChain compatible tool

[Langchain tools](https://python.langchain.com/v0.2/docs/concepts/#tools) are utilities designed to be called by a model, and UCFunctionToolkit provides the ability to use UC functions as tools that are recognized natively by LangChain.

```python
from ucai_langchain.toolkit import UCFunctionToolkit

# create a UCFunctionToolkit that includes the above UC function
toolkit = UCFunctionToolkit(function_names=[f"{CATALOG}.{SCHEMA}.python_exec"])

# fetch the tools stored in the toolkit
tools = toolkit.tools
python_exec_tool = tools[0]

# execute the tool directly
python_exec_tool.invoke({"code": "print(1)"})
```

#### Use the tools in Langchain Agent

Now we create an agent and use the tools.

```python
from langchain_community.chat_models.databricks import ChatDatabricks
from langchain.agents import AgentExecutor, create_tool_calling_agent
from langchain_core.prompts import ChatPromptTemplate

# Use Databricks foundation models
llm = ChatDatabricks(endpoint="databricks-meta-llama-3-1-70b-instruct")
prompt = ChatPromptTemplate.from_messages(
    [
        (
            "system",
            "You are a helpful assistant. Make sure to use tool for information.",
        ),
        ("placeholder", "{chat_history}"),
        ("human", "{input}"),
        ("placeholder", "{agent_scratchpad}"),
    ]
)
agent = create_tool_calling_agent(llm, tools, prompt)

# Create the agent executor
agent_executor = AgentExecutor(agent=agent, tools=tools, verbose=True)
agent_executor.invoke({"input": "36939 * 8922.4"})
```
