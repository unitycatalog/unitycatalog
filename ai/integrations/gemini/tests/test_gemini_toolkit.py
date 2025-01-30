import json
import os
from unittest import mock

import pytest
from databricks.sdk.service.catalog import (
    ColumnTypeName,
    FunctionInfo,
    FunctionParameterInfo,
    FunctionParameterInfos,
)
from google.generativeai.types import CallableFunctionDeclaration
from pydantic import ValidationError

from unitycatalog.ai.core.client import FunctionExecutionResult
from unitycatalog.ai.gemini.toolkit import GeminiTool, UCFunctionToolkit
from unitycatalog.ai.test_utils.client_utils import (
    USE_SERVERLESS,
    client,  # noqa: F401
    get_client,
    requires_databricks,
    set_default_client,
)
from unitycatalog.ai.test_utils.function_utils import (
    CATALOG,
    RETRIEVER_OUTPUT_CSV,
    RETRIEVER_OUTPUT_SCALAR,
    RETRIEVER_TABLE_FULL_DATA_TYPE,
    RETRIEVER_TABLE_RETURN_PARAMS,
    create_function_and_cleanup,
)
from unitycatalog.client.models.function_parameter_type import FunctionParameterType

SCHEMA = os.environ.get("SCHEMA", "ucai_gemini_test")


@pytest.fixture
def sample_gemini_tool():
    def dummy_function(**kwargs):
        return "dummy_result"

    name = "sample_function"
    description = "A sample function for testing."
    schema = {
        "name": name,
        "description": description,
        "parameters": {
            "properties": {
                "location": {
                    "type": "number",
                    "description": "Retrieves the current weather from a provided location.",
                    "nullable": True,
                }
            }
        },
        "type": "object",
        "required": ["location"],
    }

    return GeminiTool(fn=dummy_function, name=name, description=description, schema=schema)


def test_gemini_tool_to_dict(sample_gemini_tool):
    """Test the `to_dict` method of GeminiTool."""
    expected_output = {
        "name": sample_gemini_tool.name,
        "description": sample_gemini_tool.description,
        "schema": sample_gemini_tool.schema,
    }
    assert sample_gemini_tool.to_dict() == expected_output


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
        assert func_obj.comment in tool.description

        input_args = {"code": "print(1)"}
        result = json.loads(tool.fn(**input_args))["value"]
        assert result == "1\n"

        toolkit = UCFunctionToolkit(
            function_names=[f.full_name for f in client.list_functions(CATALOG, SCHEMA)]
        )
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
        assert func_obj.comment in tool.description
        input_args = {"code": "print(1)"}
        result = json.loads(tool.fn(**input_args))["value"]
        assert result == "1\n"

        toolkit = UCFunctionToolkit(
            function_names=[f.full_name for f in client.list_functions(CATALOG, SCHEMA)],
            client=client,
        )
        assert len(toolkit.tools) >= 1
        assert func_obj.tool_name in [t.name for t in toolkit.tools]


@requires_databricks
@pytest.mark.parametrize("use_serverless", [True, False])
def test_multiple_toolkits(use_serverless, monkeypatch):
    monkeypatch.setenv(USE_SERVERLESS, str(use_serverless))
    client = get_client()
    with set_default_client(client), create_function_and_cleanup(client, schema=SCHEMA) as func_obj:
        toolkit1 = UCFunctionToolkit(function_names=[func_obj.full_function_name])
        toolkit2 = UCFunctionToolkit(
            function_names=[f.full_name for f in client.list_functions(CATALOG, SCHEMA)]
        )
        tool1 = toolkit1.tools[0]
        tool2 = [t for t in toolkit2.tools if t.name == func_obj.tool_name][0]
        input_args = {"code": "print(1)"}
        result1 = json.loads(tool1.fn(**input_args))["value"]
        result2 = json.loads(tool2.fn(**input_args))["value"]
        assert result1 == result2


def test_toolkit_creation_errors():
    with pytest.raises(ValidationError, match=r"No client provided"):
        UCFunctionToolkit(function_names=[])

    with pytest.raises(ValidationError, match=r"Input should be an instance of BaseFunctionClient"):
        UCFunctionToolkit(function_names=[], client="client")


def test_toolkit_creation_errors(client):
    with pytest.raises(
        ValueError, match=r"Cannot create tool instances without function_names being provided."
    ):
        UCFunctionToolkit(function_names=[], client=client)


def test_toolkit_function_argument_errors(client):
    with pytest.raises(
        ValidationError,
        match=r"1 validation error for UCFunctionToolkit\nfunction_names\n  Field required",
    ):
        UCFunctionToolkit(client=client)


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
            "comment": "test comment",
            "name": "x",
            "type_text": "string",
            "type_json": '{"name":"x","type":"string","nullable":true,"metadata":{"EXISTS_DEFAULT":"\\"123\\"","default":"\\"123\\"","CURRENT_DEFAULT":"\\"123\\""}}',
            "type_name": "STRING",
            "type_precision": 0,
            "type_scale": 0,
            "position": 17,
            "parameter_type": FunctionParameterType.PARAM,
        }
    ]
    return FunctionInfo(
        catalog_name=catalog,
        schema_name=schema,
        name=name,
        full_name=f"{catalog}.{schema}.{name}",
        input_params=FunctionParameterInfos(
            parameters=[FunctionParameterInfo(**param) for param in parameters]
        ),
        comment="Executes Python code and returns its stdout.",
        data_type=data_type,
        full_data_type=full_data_type,
        return_params=return_params,
    )


def test_convert_to_gemini_schema_with_valid_function_info():
    """
    Test convert_to_gemini_schema with valid FunctionInfo input.
    """
    # Generate mock FunctionInfo using the provided generate_function_info
    function_info = generate_function_info()

    # Convert the FunctionInfo into Gemini-compatible schema
    result_schema = UCFunctionToolkit.convert_to_gemini_schema(function_info)

    # Expected output
    expected_schema = {
        "name": "test",
        "description": "Executes Python code and returns its stdout.",
        "parameters": {
            "properties": {
                "x": {"type": "string", "description": "test comment", "nullable": True},
            },
            "type": "object",
            "required": ["x"],
        },
    }

    assert (
        result_schema == expected_schema
    ), "The generated schema does not match the expected output."


def test_uc_function_to_gemini_tool(client):
    mock_function_info = generate_function_info()
    with (
        mock.patch(
            "unitycatalog.ai.core.databricks.DatabricksFunctionClient.get_function",
            return_value=mock_function_info,
        ),
        mock.patch(
            "unitycatalog.ai.core.databricks.DatabricksFunctionClient.execute_function",
            return_value=FunctionExecutionResult(format="SCALAR", value="some_string"),
        ),
    ):
        tool = UCFunctionToolkit.uc_function_to_gemini_tool(
            function_name=f"{CATALOG}.{SCHEMA}.test", client=client
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
@pytest.mark.parametrize(
    "data_type,full_data_type,return_params",
    [
        (ColumnTypeName.TABLE_TYPE, RETRIEVER_TABLE_FULL_DATA_TYPE, RETRIEVER_TABLE_RETURN_PARAMS),
    ],
)
@pytest.mark.parametrize("use_serverless", [True, False])
def test_crewai_tool_with_tracing_as_retriever(
    use_serverless, monkeypatch, format, function_output, data_type, full_data_type, return_params
):
    monkeypatch.setenv(USE_SERVERLESS, str(use_serverless))
    client = get_client()
    mock_function_info = generate_function_info(
        name=f"test_{format}",
        data_type=data_type,
        full_data_type=full_data_type,
        return_params=return_params,
    )

    with (
        mock.patch(
            "unitycatalog.ai.core.databricks.DatabricksFunctionClient.get_function",
            return_value=mock_function_info,
        ),
        mock.patch(
            "unitycatalog.ai.core.databricks.DatabricksFunctionClient._execute_uc_function",
            return_value=FunctionExecutionResult(format=format, value=function_output),
        ),
        mock.patch(
            "unitycatalog.ai.core.databricks.DatabricksFunctionClient.validate_input_params"
        ),
    ):
        import mlflow

        mlflow.gemini.autolog()

        tool = UCFunctionToolkit.uc_function_to_gemini_tool(
            function_name=mock_function_info.full_name, client=client
        )
        tool.fn(x="some_string")

        trace = mlflow.get_last_active_trace()
        assert trace is not None
        assert trace.data.spans[0].name == mock_function_info.full_name
        assert trace.info.execution_time_ms is not None
        assert trace.data.request == '{"x": "some_string"}'
        assert trace.data.response == RETRIEVER_OUTPUT_SCALAR

        mlflow.gemini.autolog(disable=True)


def test_toolkit_with_invalid_function_input(client):
    """Test toolkit with invalid input parameters for function conversion."""
    mock_function_info = generate_function_info()

    with (
        mock.patch(
            "unitycatalog.ai.core.utils.client_utils.validate_or_set_default_client",
            return_value=client,
        ),
        mock.patch.object(client, "get_function", return_value=mock_function_info),
    ):
        invalid_inputs = {"unexpected_key": "value"}
        tool = UCFunctionToolkit.uc_function_to_gemini_tool(
            function_name="catalog.schema.test", client=client
        )

        with pytest.raises(ValueError, match="Parameter x is required but not provided."):
            tool.fn(**invalid_inputs)


def test_generate_callable_tool_list(client):
    """
    Test the generate_callable_tool_list method of UCFunctionToolkit.
    """
    # Mock UCFunctionToolkit instance with a GeminiTool
    mock_function_info = generate_function_info()
    with (
        mock.patch(
            "unitycatalog.ai.core.databricks.DatabricksFunctionClient.get_function",
            return_value=mock_function_info,
        ),
        mock.patch(
            "unitycatalog.ai.core.databricks.DatabricksFunctionClient.execute_function",
            return_value=FunctionExecutionResult(format="SCALAR", value="some_string"),
        ),
    ):
        toolkit = UCFunctionToolkit(function_names=["catalog.schema.test_function"], client=client)

    # Generate callable tool list
    callable_tools = toolkit.generate_callable_tool_list()
    tools = toolkit.tools

    # Verify the output
    assert len(callable_tools) == 1, "The callable tool list should contain exactly one tool."

    gemini_tool = callable_tools[0]
    tool = tools[0]
    assert isinstance(
        gemini_tool, CallableFunctionDeclaration
    ), "The tool should be a CallableFunctionDeclaration."
    assert (
        tool.name == "catalog__schema__test_function"
    ), "The tool's name does not match the expected name."
    assert (
        tool.description == "Executes Python code and returns its stdout."
    ), "The tool's description does not match the expected description."
    assert "parameters" in tool.schema, "The tool's schema should include parameters."
    assert tool.schema["parameters"]["required"] == ["x"], "The required parameters do not match."
