import json
import os
from unittest import mock

import pytest
import pytest_asyncio
from databricks.sdk.service.catalog import ColumnTypeName
from langchain_core.messages import BaseMessage
from langchain_core.outputs import ChatResult
from langchain_core.runnables import RunnableGenerator
from langchain_databricks.chat_models import ChatDatabricks
from langchain_databricks.chat_models import ChatGeneration as LangChainChatGeneration
from langgraph.prebuilt import create_react_agent

from tests.helper_functions import wrap_output
from unitycatalog.ai.core.base import (
    FunctionExecutionResult,
)
from unitycatalog.ai.core.client import (
    ExecutionMode,
    UnitycatalogFunctionClient,
)
from unitycatalog.ai.core.utils.function_processing_utils import get_tool_name
from unitycatalog.ai.langchain.toolkit import UCFunctionToolkit
from unitycatalog.ai.test_utils.function_utils import (
    RETRIEVER_OUTPUT_CSV,
    RETRIEVER_OUTPUT_SCALAR,
    RETRIEVER_TABLE_FULL_DATA_TYPE,
)
from unitycatalog.ai.test_utils.function_utils_oss import (
    CATALOG,
    RETRIEVER_TABLE_RETURN_PARAMS_OSS,
    create_function_and_cleanup_oss,
)
from unitycatalog.client import (
    ApiClient,
    Configuration,
)
from unitycatalog.client import (
    FunctionInfo as OSSFunctionInfo,
)
from unitycatalog.client import (
    FunctionParameterInfo as OSSFunctionParameterInfo,
)
from unitycatalog.client import (
    FunctionParameterInfos as OSSFunctionParameterInfos,
)

SCHEMA = os.environ.get("SCHEMA", "ucai_langchain_test")


@pytest.fixture(autouse=True)
def env_setup(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "fake-key")


@pytest_asyncio.fixture
async def uc_client():
    config = Configuration()
    config.host = "http://localhost:8080/api/2.1/unity-catalog"
    uc_api_client = ApiClient(configuration=config)

    uc_client = UnitycatalogFunctionClient(api_client=uc_api_client)
    uc_client.uc.create_catalog(name=CATALOG)
    uc_client.uc.create_schema(name=SCHEMA, catalog_name=CATALOG)

    yield uc_client

    await uc_client.close_async()
    await uc_api_client.close()


@pytest.mark.parametrize("execution_mode", ["local", "sandbox"])
@pytest.mark.asyncio
async def test_toolkit_e2e(uc_client, execution_mode):
    uc_client.execution_mode = ExecutionMode(execution_mode)
    with create_function_and_cleanup_oss(uc_client, schema=SCHEMA) as func_obj:
        toolkit = UCFunctionToolkit(function_names=[func_obj.full_function_name], client=uc_client)
        tools = toolkit.tools
        assert len(tools) == 1
        tool = tools[0]
        assert tool.name == func_obj.tool_name
        assert tool.description == func_obj.comment
        assert tool.client_config == uc_client.to_dict()
        tool.args_schema(**{"a": 1, "b": 2})
        result = json.loads(tool.func(**{"a": 1, "b": 2}))["value"]
        assert result == "3"

        toolkit = UCFunctionToolkit(function_names=[f"{CATALOG}.{SCHEMA}.*"], client=uc_client)
        assert len(toolkit.tools) >= 1
        assert func_obj.tool_name in [t.name for t in toolkit.tools]


@pytest.mark.parametrize("execution_mode", ["local", "sandbox"])
@pytest.mark.asyncio
async def test_toolkit_e2e_manually_passing_client(uc_client, execution_mode):
    uc_client.execution_mode = ExecutionMode(execution_mode)
    with create_function_and_cleanup_oss(uc_client, schema=SCHEMA) as func_obj:
        toolkit = UCFunctionToolkit(function_names=[func_obj.full_function_name], client=uc_client)
        tools = toolkit.tools
        assert len(tools) == 1
        tool = tools[0]
        assert tool.name == func_obj.tool_name
        assert tool.description == func_obj.comment
        assert tool.uc_function_name == func_obj.full_function_name
        assert tool.client_config == uc_client.to_dict()
        tool.args_schema(**{"a": 2, "b": 3})
        result = json.loads(tool.func(**{"a": 2, "b": 3}))["value"]
        assert result == "5"

        toolkit = UCFunctionToolkit(function_names=[f"{CATALOG}.{SCHEMA}.*"], client=uc_client)
        assert len(toolkit.tools) >= 1
        assert func_obj.tool_name in [t.name for t in toolkit.tools]


@pytest.mark.parametrize("execution_mode", ["local", "sandbox"])
@pytest.mark.asyncio
async def test_toolkit_e2e_tools_with_no_params(uc_client, execution_mode):
    uc_client.execution_mode = ExecutionMode(execution_mode)

    def get_weather() -> str:
        """
        Get the weather.
        """
        return "sunny"

    with create_function_and_cleanup_oss(
        uc_client, schema=SCHEMA, callable=get_weather
    ) as func_obj:
        toolkit = UCFunctionToolkit(function_names=[func_obj.full_function_name], client=uc_client)
        tools = toolkit.tools
        assert len(tools) == 1
        tool = tools[0]
        assert tool.name == func_obj.tool_name
        assert tool.description == func_obj.comment
        assert tool.uc_function_name == func_obj.full_function_name
        assert tool.client_config == uc_client.to_dict()
        tool.args_schema()
        result = json.loads(tool.func())["value"]
        assert result == "sunny"

        toolkit = UCFunctionToolkit(function_names=[f"{CATALOG}.{SCHEMA}.*"], client=uc_client)
        assert len(toolkit.tools) >= 1
        assert func_obj.tool_name in [t.name for t in toolkit.tools]


@pytest.mark.parametrize("execution_mode", ["local", "sandbox"])
@pytest.mark.asyncio
async def test_multiple_toolkits(uc_client, execution_mode):
    uc_client.execution_mode = ExecutionMode(execution_mode)
    with create_function_and_cleanup_oss(uc_client, schema=SCHEMA) as func_obj:
        toolkit1 = UCFunctionToolkit(function_names=[func_obj.full_function_name], client=uc_client)
        toolkit2 = UCFunctionToolkit(function_names=[f"{CATALOG}.{SCHEMA}.*"], client=uc_client)
        tool1 = toolkit1.tools[0]
        tool2 = [t for t in toolkit2.tools if t.name == func_obj.tool_name][0]
        input_args = {"a": 3, "b": 4}
        assert (
            json.loads(tool1.func(**input_args))["value"]
            == json.loads(tool2.func(**input_args))["value"]
        )


def test_toolkit_creation_errors_no_client(monkeypatch):
    monkeypatch.setattr("unitycatalog.ai.core.base._is_databricks_client_available", lambda: False)

    with pytest.raises(
        ValueError,
        match=r"No client provided, either set the client when creating a toolkit or set the default client",
    ):
        UCFunctionToolkit(function_names=["test.test.test"])


def test_toolkit_creation_errors(uc_client):
    with pytest.raises(ValueError, match=r"instance of BaseFunctionClient expected"):
        UCFunctionToolkit(function_names=[], client="client")


def generate_function_info(
    catalog="catalog",
    schema="schema",
    name="test",
    data_type=None,
    full_data_type=None,
    return_params=None,
):
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
    return OSSFunctionInfo(
        catalog_name=catalog,
        schema_name=schema,
        name=name,
        input_params=OSSFunctionParameterInfos(
            parameters=[OSSFunctionParameterInfo(**param) for param in parameters]
        ),
        full_name=f"{catalog}.{schema}.{name}",
        comment="Executes Python code and returns its stdout.",
        data_type=data_type,
        full_data_type=full_data_type,
        return_params=return_params,
    )


def test_uc_function_to_langchain_tool(uc_client):
    mock_function_info = generate_function_info()
    with (
        mock.patch(
            "unitycatalog.ai.core.client.UnitycatalogFunctionClient.get_function",
            return_value=mock_function_info,
        ),
        mock.patch(
            "unitycatalog.ai.core.client.UnitycatalogFunctionClient.execute_function",
            return_value=FunctionExecutionResult(format="SCALAR", value="some_string"),
        ),
    ):
        tool = UCFunctionToolkit.uc_function_to_langchain_tool(
            client=uc_client, function_name=f"{CATALOG}.{SCHEMA}.test"
        )
        assert tool.name == get_tool_name(f"{CATALOG}.{SCHEMA}.test")
        assert json.loads(tool.func(x="some_string"))["value"] == "some_string"


@pytest.mark.parametrize(
    "format,function_output",
    [
        ("SCALAR", RETRIEVER_OUTPUT_SCALAR),
        ("CSV", RETRIEVER_OUTPUT_CSV),
    ],
)
@pytest.mark.parametrize(
    "data_type,full_data_type,return_params",
    [
        (
            ColumnTypeName.TABLE_TYPE,
            RETRIEVER_TABLE_FULL_DATA_TYPE,
            RETRIEVER_TABLE_RETURN_PARAMS_OSS,
        ),
    ],
)
def test_langchain_tool_trace_as_retriever(
    uc_client, format, function_output, data_type, full_data_type, return_params
):
    mock_function_info = generate_function_info(
        name=f"test_{format}",
        data_type=data_type,
        full_data_type=full_data_type,
        return_params=return_params,
    )

    with (
        mock.patch(
            "unitycatalog.ai.core.client.UnitycatalogFunctionClient.get_function",
            return_value=mock_function_info,
        ),
        mock.patch(
            "unitycatalog.ai.core.client.UnitycatalogFunctionClient._execute_uc_function",
            return_value=FunctionExecutionResult(format=format, value=function_output),
        ),
        mock.patch("unitycatalog.ai.core.client.UnitycatalogFunctionClient.validate_input_params"),
    ):
        import mlflow

        mlflow.langchain.autolog()

        tool = UCFunctionToolkit.uc_function_to_langchain_tool(
            client=uc_client, function_name=mock_function_info.full_name
        )

        result = tool.func(x="some_string")
        assert json.loads(result)["value"] == function_output

        trace = mlflow.get_last_active_trace()
        assert trace is not None
        assert trace.data.spans[0].name == mock_function_info.full_name
        assert trace.info.execution_time_ms is not None
        assert trace.data.request == '{"x": "some_string"}'
        assert trace.data.response == RETRIEVER_OUTPUT_SCALAR

        mlflow.langchain.autolog(disable=True)


@pytest.mark.asyncio
async def test_langgraph_agents(uc_client):
    with create_function_and_cleanup_oss(uc_client, schema=SCHEMA) as func_obj:
        toolkit = UCFunctionToolkit(function_names=[func_obj.full_function_name], client=uc_client)
        system_message = "You are a helpful assistant. Make sure to use tool for information."
        llm = ChatDatabricks(endpoint="databricks-meta-llama-3-1-70b-instruct")
        agent = create_react_agent(llm, toolkit.tools, state_modifier=system_message)
        chain = agent | RunnableGenerator(wrap_output)

        with mock.patch.object(
            llm,
            "_generate",
            return_value=ChatResult(
                generations=[
                    LangChainChatGeneration(
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
