import json
import os
from unittest import mock

import pytest
import pytest_asyncio
from databricks.sdk.service.catalog import (
    FunctionInfo,
    FunctionParameterInfo,
    FunctionParameterInfos,
)
from google.generativeai.types import CallableFunctionDeclaration
from pydantic import ValidationError

from unitycatalog.ai.core.base import FunctionExecutionResult
from unitycatalog.ai.core.client import (
    ExecutionMode,
    FunctionExecutionResult,
    UnitycatalogFunctionClient,
)
from unitycatalog.ai.gemini.toolkit import GeminiTool, UCFunctionToolkit
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
from unitycatalog.client.models.function_parameter_type import FunctionParameterType

try:
    # v2
    from pydantic_core._pydantic_core import ValidationError
except ImportError:
    # v1
    from pydantic.error_wrappers import ValidationError

SCHEMA = os.environ.get("SCHEMA", "ucai_gemini_test")


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
    await uc_api_client.close()


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


@pytest.mark.parametrize("execution_mode", ["local", "sandbox"])
@pytest.mark.asyncio
async def test_toolkit_e2e(uc_client, execution_mode):
    uc_client.execution_mode = ExecutionMode(execution_mode)
    with create_function_and_cleanup_oss(uc_client, schema=SCHEMA) as func_obj:
        toolkit = UCFunctionToolkit(function_names=[func_obj.full_function_name], client=uc_client)
        tools = toolkit.tools
        assert len(tools) == 1
        tool = tools[0]
        assert func_obj.comment in tool.description

        input_args = {"a": 5, "b": 6}
        result = json.loads(tool.fn(**input_args))["value"]
        assert result == "11"

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
        toolkit = UCFunctionToolkit(function_names=[func_obj.full_function_name], client=uc_client)
        tools = toolkit.tools
        assert len(tools) == 1
        tool = tools[0]
        assert tool.name == func_obj.tool_name
        assert func_obj.comment in tool.description
        input_args = {"a": 3, "b": 4}
        result = json.loads(tool.fn(**input_args))["value"]
        assert result == "7"

        toolkit = UCFunctionToolkit(
            function_names=[f.full_name for f in uc_client.list_functions(CATALOG, SCHEMA)],
            client=uc_client,
        )
        assert len(toolkit.tools) >= 1
        assert func_obj.tool_name in [t.name for t in toolkit.tools]


@pytest.mark.parametrize("execution_mode", ["local", "sandbox"])
@pytest.mark.asyncio
async def test_multiple_toolkits(uc_client, execution_mode):
    uc_client.execution_mode = ExecutionMode(execution_mode)
    with create_function_and_cleanup_oss(uc_client, schema=SCHEMA) as func_obj:
        toolkit1 = UCFunctionToolkit(function_names=[func_obj.full_function_name], client=uc_client)
        toolkit2 = UCFunctionToolkit(
            function_names=[f.full_name for f in uc_client.list_functions(CATALOG, SCHEMA)],
            client=uc_client,
        )
        tool1 = toolkit1.tools[0]
        tool2 = [t for t in toolkit2.tools if t.name == func_obj.tool_name][0]
        input_args = {"a": 2, "b": 3}
        result1 = json.loads(tool1.fn(**input_args))["value"]
        result2 = json.loads(tool2.fn(**input_args))["value"]
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
    with pytest.raises(
        ValidationError,
        match=r"1 validation error for UCFunctionToolkit\nfunction_names\n  Field required",
    ):
        UCFunctionToolkit(client=uc_client)


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

    assert result_schema == expected_schema, (
        "The generated schema does not match the expected output."
    )


@pytest.mark.asyncio
async def test_uc_function_to_gemini_tool(uc_client):
    mock_function_info = generate_function_info()
    with (
        mock.patch(
            "unitycatalog.ai.core.utils.client_utils.validate_or_set_default_client",
            return_value=uc_client,
        ),
        mock.patch.object(uc_client, "get_function", return_value=mock_function_info),
        mock.patch.object(
            uc_client,
            "execute_function",
            return_value=FunctionExecutionResult(format="SCALAR", value="some_string"),
        ),
    ):
        tool = UCFunctionToolkit.uc_function_to_gemini_tool(
            function_name="catalog.schema.test", client=uc_client
        )
        result = json.loads(tool.fn(x="some_string"))["value"]
        assert result == "some_string"


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
        tool = UCFunctionToolkit.uc_function_to_gemini_tool(
            function_name="catalog.schema.test", client=uc_client
        )

        with pytest.raises(ValueError, match="Parameter x is required but not provided."):
            tool.fn(**invalid_inputs)


def test_generate_callable_tool_list(uc_client):
    """
    Test the generate_callable_tool_list method of UCFunctionToolkit.
    """
    # Mock UCFunctionToolkit instance with a GeminiTool
    mock_function_info = generate_function_info()
    with (
        mock.patch(
            "unitycatalog.ai.core.utils.client_utils.validate_or_set_default_client",
            return_value=uc_client,
        ),
        mock.patch.object(uc_client, "get_function", return_value=mock_function_info),
        mock.patch.object(
            uc_client,
            "execute_function",
            return_value=FunctionExecutionResult(format="SCALAR", value="some_string"),
        ),
    ):
        toolkit = UCFunctionToolkit(
            function_names=["catalog.schema.test_function"], client=uc_client
        )

    # Generate callable tool list
    callable_tools = toolkit.generate_callable_tool_list()
    tools = toolkit.tools

    # Verify the output
    assert len(callable_tools) == 1, "The callable tool list should contain exactly one tool."

    gemini_tool = callable_tools[0]
    tool = tools[0]
    assert isinstance(gemini_tool, CallableFunctionDeclaration), (
        "The tool should be a CallableFunctionDeclaration."
    )
    assert tool.name == "catalog__schema__test_function", (
        "The tool's name does not match the expected name."
    )
    assert tool.description == "Executes Python code and returns its stdout.", (
        "The tool's description does not match the expected description."
    )
    assert "parameters" in tool.schema, "The tool's schema should include parameters."
    assert tool.schema["parameters"]["required"] == ["x"], "The required parameters do not match."
