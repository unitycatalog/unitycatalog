import json
import os
from unittest import mock

import pytest
from databricks.sdk.service.catalog import (
    FunctionInfo,
    FunctionParameterInfo,
    FunctionParameterInfos,
)
from langchain_core.messages import BaseMessage
from langchain_core.outputs import ChatGeneration, ChatResult
from langchain_core.runnables import RunnableGenerator
from langchain_databricks.chat_models import ChatDatabricks, ChatGeneration
from langgraph.prebuilt import create_react_agent
from ucai.core.client import (
    FunctionExecutionResult,
)
from ucai.core.utils.function_processing_utils import get_tool_name
from ucai.test_utils.client_utils import (
    USE_SERVERLESS,
    client,  # noqa: F401
    get_client,
    requires_databricks,
    set_default_client,
)
from ucai.test_utils.function_utils import (
    CATALOG,
    create_function_and_cleanup,
)

try:
    # v2
    from pydantic.v1.error_wrappers import ValidationError
except ImportError:
    # v1
    from pydantic.error_wrappers import ValidationError

from tests.helper_functions import wrap_output
from ucai_langchain.toolkit import UCFunctionToolkit

SCHEMA = os.environ.get("SCHEMA", "ucai_langchain_test")


@requires_databricks
@pytest.mark.parametrize("use_serverless", [True, False])
def test_toolkit_e2e(use_serverless, monkeypatch):
    monkeypatch.setenv(USE_SERVERLESS, str(use_serverless))
    client = get_client()
    with set_default_client(client), create_function_and_cleanup(client, schema=SCHEMA) as func_obj:
        toolkit = UCFunctionToolkit(function_names=[func_obj.full_function_name])
        tools = toolkit.tools
        assert len(tools) == 1
        tool = tools[0]
        assert tool.name == func_obj.tool_name
        assert tool.description == func_obj.comment
        assert tool.client_config == client.to_dict()
        tool.args_schema(**{"code": "print(1)"})
        result = json.loads(tool.func(code="print(1)"))["value"]
        assert result == "1\n"

        toolkit = UCFunctionToolkit(function_names=[f"{CATALOG}.{SCHEMA}.*"])
        assert len(toolkit.tools) >= 1
        assert func_obj.tool_name in [t.name for t in toolkit.tools]


@requires_databricks
@pytest.mark.parametrize("use_serverless", [True, False])
def test_toolkit_e2e_manually_passing_client(use_serverless, monkeypatch):
    monkeypatch.setenv(USE_SERVERLESS, str(use_serverless))
    client = get_client()
    with set_default_client(client), create_function_and_cleanup(client, schema=SCHEMA) as func_obj:
        toolkit = UCFunctionToolkit(function_names=[func_obj.full_function_name], client=client)
        tools = toolkit.tools
        assert len(tools) == 1
        tool = tools[0]
        assert tool.name == func_obj.tool_name
        assert tool.description == func_obj.comment
        assert tool.client_config == client.to_dict()
        tool.args_schema(**{"code": "print(1)"})
        result = json.loads(tool.func(code="print(1)"))["value"]
        assert result == "1\n"

        toolkit = UCFunctionToolkit(function_names=[f"{CATALOG}.{SCHEMA}.*"], client=client)
        assert len(toolkit.tools) >= 1
        assert func_obj.tool_name in [t.name for t in toolkit.tools]


@requires_databricks
@pytest.mark.parametrize("use_serverless", [True, False])
def test_toolkit_e2e_manually_passing_client(use_serverless, monkeypatch):
    monkeypatch.setenv(USE_SERVERLESS, str(use_serverless))
    client = get_client()
    with set_default_client(client), create_function_and_cleanup(client, schema=SCHEMA) as func_obj:
        toolkit = UCFunctionToolkit(function_names=[func_obj.full_function_name], client=client)
        tools = toolkit.tools
        assert len(tools) == 1
        tool = tools[0]
        assert tool.name == func_obj.tool_name
        assert tool.description == func_obj.comment
        assert tool.client_config == client.to_dict()
        tool.args_schema(**{"code": "print(1)"})
        result = json.loads(tool.func(code="print(1)"))["value"]
        assert result == "1\n"

        toolkit = UCFunctionToolkit(function_names=[f"{CATALOG}.{SCHEMA}.*"], client=client)
        assert len(toolkit.tools) >= 1
        assert func_obj.tool_name in [t.name for t in toolkit.tools]


@requires_databricks
@pytest.mark.parametrize("use_serverless", [True, False])
def test_multiple_toolkits(use_serverless, monkeypatch):
    monkeypatch.setenv(USE_SERVERLESS, str(use_serverless))
    client = get_client()
    with set_default_client(client), create_function_and_cleanup(client, schema=SCHEMA) as func_obj:
        toolkit1 = UCFunctionToolkit(function_names=[func_obj.full_function_name])
        toolkit2 = UCFunctionToolkit(function_names=[f"{CATALOG}.{SCHEMA}.*"])
        tool1 = toolkit1.tools[0]
        tool2 = [t for t in toolkit2.tools if t.name == func_obj.tool_name][0]
        input_args = {"code": "print(1)"}
        assert tool1.func(**input_args) == tool2.func(**input_args)


def test_toolkit_creation_errors():
    with pytest.raises(ValueError, match=r"No client provided"):
        UCFunctionToolkit(function_names=[])

    with pytest.raises(ValueError, match=r"instance of BaseFunctionClient expected"):
        UCFunctionToolkit(function_names=[], client="client")


def generate_function_info():
    parameters = [
        {
            "name": "x",
            "type_text": "string",
            "type_json": '{"name":"x","type":"string","nullable":true,"metadata":{"EXISTS_DEFAULT":"\\"123\\"","default":"\\"123\\"","CURRENT_DEFAULT":"\\"123\\""}}',
            "type_name": "STRING",
            "type_precision": 0,
            "type_scale": 0,
            "position": 17,
            "parameter_type": "PARAM",
            "parameter_default": '"123"',
        }
    ]
    return FunctionInfo(
        catalog_name="catalog",
        schema_name="schema",
        name="test",
        input_params=FunctionParameterInfos(
            parameters=[FunctionParameterInfo(**param) for param in parameters]
        ),
    )


def test_uc_function_to_langchain_tool():
    client = get_client()
    mock_function_info = generate_function_info()
    with (
        mock.patch(
            "ucai.core.databricks.DatabricksFunctionClient.get_function",
            return_value=mock_function_info,
        ),
        mock.patch(
            "ucai.core.databricks.DatabricksFunctionClient.execute_function",
            return_value=FunctionExecutionResult(format="SCALAR", value="some_string"),
        ),
    ):
        tool = UCFunctionToolkit.uc_function_to_langchain_tool(
            client=client, function_name=f"{CATALOG}.{SCHEMA}.test"
        )
        assert tool.name == get_tool_name(f"{CATALOG}.{SCHEMA}.test")
        assert json.loads(tool.func(x="some_string"))["value"] == "some_string"


@requires_databricks
@pytest.mark.parametrize("use_serverless", [True, False])
def test_langgraph_agents(monkeypatch, use_serverless):
    monkeypatch.setenv(USE_SERVERLESS, str(use_serverless))
    client = get_client()
    with create_function_and_cleanup(client, schema=SCHEMA) as func_obj:
        toolkit = UCFunctionToolkit(function_names=[func_obj.full_function_name], client=client)
        system_message = "You are a helpful assistant. Make sure to use tool for information."
        llm = ChatDatabricks(endpoint="databricks-meta-llama-3-1-70b-instruct")
        agent = create_react_agent(llm, toolkit.tools, state_modifier=system_message)
        chain = agent | RunnableGenerator(wrap_output)
        # we need this mock as the serving endpoint is not stable
        with mock.patch.object(
            llm,
            "_generate",
            return_value=ChatResult(
                generations=[
                    ChatGeneration(
                        text="1024",
                        type="ChatGeneration",
                        message=BaseMessage(content="1024", type="tool"),
                    )
                ]
            ),
        ):
            result = chain.invoke(
                {"messages": [{"role": "user", "content": "What is the result of 2**10?"}]}
            )
        assert "1024" in result


def test_toolkit_fields_validation(client):
    def test_tool():
        return "abc"

    with pytest.raises(ValidationError, match=r"str type expected"):
        UCFunctionToolkit(client=client, function_names=[test_tool])

    with pytest.raises(ValidationError, match=r"Invalid function name"):
        UCFunctionToolkit(client=client, function_names=["test_tool"])

    with pytest.raises(ValidationError, match=r"instance of BaseFunctionClient expected"):
        UCFunctionToolkit(client=test_tool, function_names=[])
