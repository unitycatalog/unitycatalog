import json
import os
from unittest import mock

import pytest
import pytest_asyncio
from autogen import ConversableAgent

from unitycatalog.ai.autogen.toolkit import AutogenTool, UCFunctionToolkit
from unitycatalog.ai.core.base import FunctionExecutionResult
from unitycatalog.ai.core.client import UnitycatalogFunctionClient
from unitycatalog.ai.test_utils.function_utils import RETRIEVER_OUTPUT_CSV, RETRIEVER_OUTPUT_SCALAR
from unitycatalog.ai.test_utils.function_utils_oss import (
    CATALOG,
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
    # v2
    from pydantic_core._pydantic_core import ValidationError
except ImportError:
    # v1
    from pydantic.error_wrappers import ValidationError

SCHEMA = os.environ.get("SCHEMA", "ucai_autogen_test")


def mock_autogen_tool_response(function_name, input_data, message_id):
    input_data["code"] = 'print("Hello, World!")'

    return {
        "id": message_id,
        "type": "function_call",
        "name": function_name,
        "arguments": input_data,
    }


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
    uc_api_client.close()


@pytest.fixture
def sample_autogen_tool():
    fn = mock.MagicMock()
    name = "sample_function"
    description = "A sample function for testing."
    tool = {
        "type": "function",
        "function": {
            "name": name,
            "strict": True,
            "parameters": {
                "properties": {
                    "location": {
                        "anyOf": [{"type": "string"}, {"type": "null"}],
                        "description": "Retrieves the current weather from a provided location.",
                        "title": "Location",
                    }
                },
                "type": "object",
                "additionalProperties": False,
                "required": ["location"],
            },
            "description": description,
        },
    }

    return AutogenTool(fn=fn, name=name, description=description, tool=tool)


def test_autogen_tool_to_dict(sample_autogen_tool):
    expected_output = {
        "name": sample_autogen_tool.name,
        "description": sample_autogen_tool.description,
        "tool": sample_autogen_tool.tool,
    }
    assert sample_autogen_tool.to_dict() == expected_output


def test_autogen_tool_register_function(sample_autogen_tool):
    mock_caller = mock.MagicMock(spec=ConversableAgent)
    mock_executor = mock.MagicMock(spec=ConversableAgent)
    mock_executor._wrap_function.return_value = "wrapped_function"

    sample_autogen_tool.register_function(callers=mock_caller, executors=mock_executor)

    mock_caller.update_tool_signature.assert_called_once_with(
        sample_autogen_tool.tool, is_remove=False
    )
    mock_executor._wrap_function.assert_called_once_with(sample_autogen_tool.fn)
    mock_executor.register_function.assert_called_once_with(
        {sample_autogen_tool.name: "wrapped_function"}
    )


@pytest.mark.asyncio
async def test_toolkit_e2e(uc_client):
    with create_function_and_cleanup_oss(uc_client, schema=SCHEMA) as func_obj:
        toolkit = UCFunctionToolkit(function_names=[func_obj.full_function_name], client=uc_client)
        tools = toolkit.tools
        assert len(tools) == 1
        tool = tools[0]
        assert func_obj.comment in tool.description

        input_args = {"code": "print(1)"}
        result = json.loads(tool.fn(**input_args))["value"]
        assert result == "1\n"

        toolkit = UCFunctionToolkit(
            function_names=[f.full_name for f in uc_client.list_functions(CATALOG, SCHEMA)],
            client=uc_client,
        )
        assert len(toolkit.tools) >= 1
        assert func_obj.tool_name in [t.name for t in toolkit.tools]


@pytest.mark.asyncio
async def test_toolkit_e2e_manually_passing_client(uc_client):
    with create_function_and_cleanup_oss(uc_client, schema=SCHEMA) as func_obj:
        toolkit = UCFunctionToolkit(function_names=[func_obj.full_function_name], client=uc_client)
        tools = toolkit.tools
        assert len(tools) == 1
        tool = tools[0]
        assert tool.name == func_obj.tool_name
        assert func_obj.comment in tool.description
        input_args = {"code": "print(1)"}
        result = json.loads(tool.fn(**input_args))["value"]
        assert result == "1\n"

        toolkit = UCFunctionToolkit(
            function_names=[f.full_name for f in uc_client.list_functions(CATALOG, SCHEMA)],
            client=uc_client,
        )
        assert len(toolkit.tools) >= 1
        assert func_obj.tool_name in [t.name for t in toolkit.tools]


@pytest.mark.asyncio
async def test_multiple_toolkits(uc_client):
    with create_function_and_cleanup_oss(uc_client, schema=SCHEMA) as func_obj:
        toolkit1 = UCFunctionToolkit(function_names=[func_obj.full_function_name], client=uc_client)
        toolkit2 = UCFunctionToolkit(
            function_names=[f.full_name for f in uc_client.list_functions(CATALOG, SCHEMA)],
            client=uc_client,
        )
        tool1 = toolkit1.tools[0]
        tool2 = [t for t in toolkit2.tools if t.name == func_obj.tool_name][0]
        input_args = {"code": "print(1)"}
        result1 = json.loads(tool1.fn(**input_args))["value"]
        result2 = json.loads(tool2.fn(**input_args))["value"]
        assert result1 == result2


def test_toolkit_creation_errors_no_client():
    with pytest.raises(
        ValidationError,
        match=r"No client provided, either set the client when creating a toolkit or set the default client",
    ):
        UCFunctionToolkit(function_names=[])


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
    with pytest.raises(
        ValidationError,
        match=r"1 validation error for UCFunctionToolkit\nfunction_names\n  Field required",
    ):
        UCFunctionToolkit(client=uc_client)


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
        full_name=f"catalog.schema.test",
        comment="Executes Python code and returns its stdout.",
    )


@pytest.mark.asyncio
async def test_uc_function_to_autogen_tool(uc_client):
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
        tool = UCFunctionToolkit.uc_function_to_autogen_tool(
            function_name="catalog.schema.test", client=uc_client
        )
        result = json.loads(tool.fn(x="some_string"))["value"]
        assert result == "some_string"


@pytest.mark.parametrize(
    "format,function_output",
    [
        ("SCALAR", RETRIEVER_OUTPUT_SCALAR),
        ("CSV", RETRIEVER_OUTPUT_CSV),
    ],
)
@pytest.mark.asyncio
async def test_autogen_tool_with_tracing_as_retriever(uc_client, format: str, function_output: str):
    mock_function_info = generate_function_info()
    trace_response = '[{"page_content": "# Technology partners\\n## What is Databricks Partner Connect?\\n", "metadata": {"similarity_score": 0.010178182, "chunk_id": "0217a07ba2fec61865ce408043acf1cf"}}, {"page_content": "# Technology partners\\n## What is Databricks?\\n", "metadata": {"similarity_score": 0.010178183, "chunk_id": "0217a07ba2fec61865ce408043acf1cd"}}]'

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
            function_name=f"catalog.schema.test_{format}", client=uc_client
        )
        result = tool.fn(x="some input")
        assert json.loads(result)["value"] == function_output

        import mlflow

        trace = mlflow.get_last_active_trace()
        assert trace is not None
        assert trace.info.execution_time_ms is not None
        assert trace.data.request == '{"x": "some input"}'
        assert trace.data.response == trace_response
        assert trace.data.spans[0].name == f"catalog.schema.test_{format}"

        mlflow.autogen.autolog(disable=True)


@pytest.mark.asyncio
async def test_toolkit_with_invalid_function_input(uc_client):
    """Test toolkit with invalid input parameters for function conversion."""
    mock_function_info = generate_function_info()

    with (
        mock.patch(
            "unitycatalog.ai.core.utils.client_utils.validate_or_set_default_client",
            return_value=uc_client,
        ),
        mock.patch.object(uc_client, "get_function", return_value=mock_function_info),
    ):
        invalid_inputs = {"unexpected_key": "value"}
        tool = UCFunctionToolkit.uc_function_to_autogen_tool(
            function_name="catalog.schema.test", client=uc_client
        )

        with pytest.raises(ValueError, match="Extra parameters provided that are not defined"):
            tool.fn(**invalid_inputs)


def test_register_with_agents(uc_client):
    function_names = ["catalog.schema.function"]

    FunctionInfo(
        catalog_name="catalog",
        schema_name="schema",
        name="function",
        input_params=FunctionParameterInfos(parameters=[]),
        full_name="catalog.schema.function",
        comment="Executes Python code and returns its stdout.",
    )

    mock_autogen_tool = mock.create_autospec(AutogenTool)
    mock_autogen_tool.register_function = mock.MagicMock()

    with mock.patch.object(
        UCFunctionToolkit, "uc_function_to_autogen_tool", return_value=mock_autogen_tool
    ):
        toolkit = UCFunctionToolkit(function_names=function_names, client=uc_client)

        mock_callers = mock.MagicMock(spec=ConversableAgent)
        mock_executors = mock.MagicMock(spec=ConversableAgent)

        toolkit.register_with_agents(callers=mock_callers, executors=mock_executors)

        for tool in toolkit.tools:
            tool.register_function.assert_called_once_with(
                callers=mock_callers, executors=mock_executors
            )
