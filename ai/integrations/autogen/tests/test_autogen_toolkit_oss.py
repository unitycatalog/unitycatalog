import json
import os
from importlib import metadata
from unittest import mock

import pytest
import pytest_asyncio
from autogen_agentchat.agents import AssistantAgent
from autogen_agentchat.messages import TextMessage
from autogen_core import CancellationToken
from autogen_core.models import CreateResult, RequestUsage
from autogen_ext.models.openai import OpenAIChatCompletionClient
from databricks.sdk.service.catalog import ColumnTypeName
from packaging import version

from unitycatalog.ai.autogen.toolkit import UCFunctionToolkit
from unitycatalog.ai.core.base import FunctionExecutionResult
from unitycatalog.ai.core.client import ExecutionMode, UnitycatalogFunctionClient
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
    FunctionInfo,
    FunctionParameterInfo,
    FunctionParameterInfos,
)

try:
    # pydantic v2
    from pydantic_core._pydantic_core import ValidationError
except ImportError:
    # pydantic v1
    from pydantic.error_wrappers import ValidationError

SCHEMA = os.environ.get("SCHEMA", "ucai_autogen_test")

autogen_version = metadata.version("autogen_core")
skip_mlflow_test = version.parse(autogen_version) >= version.parse("0.4.0")


@pytest_asyncio.fixture
async def uc_client():
    config = Configuration()
    config.host = "http://localhost:8080/api/2.1/unity-catalog"
    uc_api_client = ApiClient(configuration=config)

    uc_client = UnitycatalogFunctionClient(api_client=uc_api_client)
    uc_client.uc.create_catalog(name=CATALOG)
    uc_client.uc.create_schema(name=SCHEMA, catalog_name=CATALOG)

    yield uc_client

    uc_client.close()
    await uc_api_client.close()


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
    return FunctionInfo(
        catalog_name=catalog,
        schema_name=schema,
        name=name,
        input_params=FunctionParameterInfos(
            parameters=[FunctionParameterInfo(**param) for param in parameters]
        ),
        full_name=f"{catalog}.{schema}.{name}",
        comment="Executes Python code and returns its stdout.",
        routine_body="EXTERNAL",
        routine_definition="print('hello')",
        data_type=data_type,
        full_data_type=full_data_type,
        return_params=return_params,
    )


@pytest.mark.parametrize("execution_mode", ["local", "sandbox"])
@pytest.mark.asyncio
async def test_toolkit_e2e(uc_client, execution_mode):
    uc_client.execution_mode = ExecutionMode(execution_mode)
    with create_function_and_cleanup_oss(uc_client, schema=SCHEMA) as func_obj:
        toolkit = UCFunctionToolkit(
            function_names=[func_obj.full_function_name],
            client=uc_client,
        )
        tools = toolkit.tools
        assert len(tools) == 1

        tool = tools[0]
        assert tool.description == (func_obj.comment or "")
        assert tool.name == func_obj.tool_name

        input_args = {"a": 1, "b": 2}
        result_str = await tool.run_json(input_args, CancellationToken())
        result = json.loads(result_str)["value"]
        assert result == "3"

        toolkit = UCFunctionToolkit(
            function_names=[f.full_name for f in uc_client.list_functions(CATALOG, SCHEMA)],
            client=uc_client,
        )
        assert len(toolkit.tools) >= 1
        assert func_obj.tool_name in [t.name for t in toolkit.tools]


@pytest.mark.parametrize("execution_mode", ["local", "sandbox"])
@pytest.mark.asyncio
async def test_toolkit_e2e_manually_passing_client(uc_client, execution_mode):
    uc_client.execution_mode = ExecutionMode(execution_mode)
    with create_function_and_cleanup_oss(uc_client, schema=SCHEMA) as func_obj:
        toolkit = UCFunctionToolkit(
            function_names=[func_obj.full_function_name],
            client=uc_client,
        )
        tools = toolkit.tools
        assert len(tools) == 1
        tool = tools[0]

        assert tool.name == func_obj.tool_name
        assert func_obj.comment in tool.description

        input_args = {"a": 1, "b": 4}
        # Pass a CancellationToken to run_json
        result_str = await tool.run_json(input_args, CancellationToken())
        result = json.loads(result_str)["value"]
        assert result == "5"


@pytest.mark.parametrize("execution_mode", ["local", "sandbox"])
@pytest.mark.asyncio
async def test_multiple_toolkits(uc_client, execution_mode):
    uc_client.execution_mode = ExecutionMode(execution_mode)
    with create_function_and_cleanup_oss(uc_client, schema=SCHEMA) as func_obj:
        toolkit1 = UCFunctionToolkit(
            function_names=[func_obj.full_function_name],
            client=uc_client,
        )
        toolkit2 = UCFunctionToolkit(
            function_names=[f.full_name for f in uc_client.list_functions(CATALOG, SCHEMA)],
            client=uc_client,
        )

        tool1 = toolkit1.tools[0]
        tool2 = [t for t in toolkit2.tools if t.name == func_obj.tool_name][0]

        input_args = {"a": 2, "b": 4}

        # Must pass a CancellationToken
        result1_str = await tool1.run_json(input_args, CancellationToken())
        result2_str = await tool2.run_json(input_args, CancellationToken())

        result1 = json.loads(result1_str)["value"]
        result2 = json.loads(result2_str)["value"]
        assert result1 == result2


def test_toolkit_creation_errors_no_client(monkeypatch):
    monkeypatch.setattr("unitycatalog.ai.core.base._is_databricks_client_available", lambda: False)

    with pytest.raises(
        ValidationError,
        match=r"No client provided, either set the client when creating a toolkit or set the default client",
    ):
        UCFunctionToolkit(function_names=["test.test.test"])


def test_toolkit_creation_errors_invalid_client(uc_client):
    with pytest.raises(ValidationError, match=r"Input should be an instance of BaseFunctionClient"):
        UCFunctionToolkit(function_names=[], client="client")


def test_toolkit_creation_errors_missing_function_names(uc_client):
    with pytest.raises(
        ValidationError,
        match=r"Cannot create tool instances without function_names being provided.",
    ):
        UCFunctionToolkit(function_names=[], client=uc_client)


def test_toolkit_function_argument_errors(uc_client):
    """
    Verify we still raise the appropriate validation errors if function_names is not provided.
    """
    with pytest.raises(
        ValidationError,
        match=r"1 validation error for UCFunctionToolkit\nfunction_names\n  Field required",
    ):
        UCFunctionToolkit(client=uc_client)


@pytest.mark.asyncio
async def test_uc_function_to_autogen_tool(uc_client):
    mock_function_info = generate_function_info()
    with (
        mock.patch.object(uc_client, "get_function", return_value=mock_function_info),
        mock.patch.object(
            uc_client,
            "execute_function",
            return_value=FunctionExecutionResult(format="SCALAR", value="some_string"),
        ),
        mock.patch(
            "unitycatalog.ai.core.utils.client_utils.validate_or_set_default_client",
            return_value=uc_client,
        ),
    ):
        tool = UCFunctionToolkit.uc_function_to_autogen_tool(
            function_name="catalog.schema.test", client=uc_client
        )
        result_str = await tool.run_json({"x": "some_string"}, CancellationToken())
        result = json.loads(result_str)["value"]
        assert result == "some_string"


@pytest.mark.skipif(
    skip_mlflow_test, reason="MLflow autologging is not supported for autogen_core 0.4.0 and above."
)
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
@pytest.mark.asyncio
async def test_autogen_tool_with_tracing_as_retriever(
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

        mlflow.autogen.autolog()

        tool = UCFunctionToolkit.uc_function_to_autogen_tool(
            function_name=mock_function_info.full_name, client=uc_client
        )
        result = tool.fn(x="some input")
        assert json.loads(result)["value"] == function_output

        import mlflow

        trace = mlflow.get_last_active_trace()
        assert trace is not None
        assert trace.data.spans[0].name == mock_function_info.full_name
        assert trace.info.execution_time_ms is not None
        assert trace.data.request == '{"x": "some input"}'
        assert trace.data.response == RETRIEVER_OUTPUT_SCALAR

        mlflow.autogen.autolog(disable=True)


@pytest.mark.asyncio
async def test_toolkit_with_invalid_function_input(uc_client):
    mock_function_info = generate_function_info()
    with (
        mock.patch(
            "unitycatalog.ai.core.utils.client_utils.validate_or_set_default_client",
            return_value=uc_client,
        ),
        mock.patch.object(uc_client, "get_function", return_value=mock_function_info),
    ):
        tool = UCFunctionToolkit.uc_function_to_autogen_tool(
            function_name="catalog.schema.test", client=uc_client
        )
        invalid_inputs = {"unexpected_key": "value"}
        with pytest.raises(ValueError, match="Extra inputs are not permitted"):
            await tool.run_json(invalid_inputs, CancellationToken())


@pytest.mark.asyncio
@mock.patch("autogen_ext.models.openai._openai_client.OpenAIChatCompletionClient.create")
async def test_tool_in_assistant_agent(mock_create, uc_client):
    mock_create.return_value = CreateResult(
        finish_reason="stop",
        content="Sure, here's the result: 1\n",
        usage=RequestUsage(prompt_tokens=0, completion_tokens=0),
        cached=False,
    )

    mock_function_info = generate_function_info()
    with (
        mock.patch.object(
            uc_client,
            "execute_function",
            return_value=FunctionExecutionResult(format="SCALAR", value="1\n"),
        ),
        mock.patch.object(uc_client, "get_function", return_value=mock_function_info),
    ):
        with create_function_and_cleanup_oss(uc_client, schema=SCHEMA) as func_obj:
            toolkit = UCFunctionToolkit(
                function_names=[func_obj.full_function_name], client=uc_client
            )
            my_tool = toolkit.tools[0]

            model_client = OpenAIChatCompletionClient(model="gpt-4", temperature=0, api_key="key")
            agent = AssistantAgent(
                name="assistant",
                system_message="You can call UC functions as needed.",
                model_client=model_client,
                tools=[my_tool],
                reflect_on_tool_use=True,
            )

            user_input = "Run the function with code='print(1)'"
            response = await agent.on_messages(
                [TextMessage(content=user_input, source="user")], CancellationToken()
            )

            assert "1\n" in response.chat_message.content

    mock_create.assert_called_once()
