import json
import os
import typing
from unittest import mock

import dspy
import pytest
import pytest_asyncio
from databricks.sdk.service.catalog import (
    FunctionInfo,
    FunctionParameterInfo,
    FunctionParameterInfos,
)
from pydantic import ValidationError

from unitycatalog.ai.core.base import FunctionExecutionResult
from unitycatalog.ai.core.client import (
    FunctionExecutionResult,
    UnitycatalogFunctionClient,
)
from unitycatalog.ai.core.utils.execution_utils import ExecutionMode
from unitycatalog.ai.dspy.toolkit import UCFunctionToolkit, UnityCatalogDSPyToolWrapper
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

SCHEMA = os.environ.get("SCHEMA", "ucai_dspy_test")


def mock_dspy_tool_response(function_name, input_data, message_id):
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
def sample_dspy_tool():
    def dummy_function(**kwargs):
        return "dummy_result"

    name = "test_test_test"
    description = "Simple function to test the tool adapter."
    args_dict = {
        "y": {"anyOf": [{"type": "string"}, {"type": "null"}]},
        "x": {"anyOf": [{"type": "number"}, {"type": "null"}]},
    }
    arg_types = {"x": float, "y": str}
    arg_desc = {"x": "A number parameter", "y": "A string parameter"}

    # Create a proper DSPy Tool instance instead of a mock
    dspy_tool = dspy.adapters.Tool(
        func=dummy_function,
        name=name,
        desc=description,
        args=args_dict,
        arg_types=arg_types,
        arg_desc=arg_desc,
    )

    return UnityCatalogDSPyToolWrapper(
        tool=dspy_tool, uc_function_name="test.test.test", client_config={"profile": None}
    )


def test_dspy_tool_wrapper_to_dict(sample_dspy_tool):
    """Test the `to_dict` method of UnityCatalogDSPyToolWrapper."""
    expected_output = {
        "uc_function_name": "test.test.test",
        "client_config": {"profile": None},
        "tool_info": {
            "name": "test_test_test",
            "description": "Simple function to test the tool adapter.",
            "args": {
                "y": {"anyOf": [{"type": "string"}, {"type": "null"}]},
                "x": {"anyOf": [{"type": "number"}, {"type": "null"}]},
            },
            "arg_types": {"x": float, "y": str},
            "arg_desc": {"x": "A number parameter", "y": "A string parameter"},
        },
    }
    assert sample_dspy_tool.to_dict() == expected_output


@pytest.mark.parametrize("execution_mode", ["local", "sandbox"])
@pytest.mark.asyncio
async def test_toolkit_e2e(uc_client, execution_mode):
    uc_client.execution_mode = ExecutionMode(execution_mode)
    with create_function_and_cleanup_oss(uc_client, schema=SCHEMA) as func_obj:
        toolkit = UCFunctionToolkit(function_names=[func_obj.full_function_name], client=uc_client)
        tools = toolkit.tools
        assert len(tools) == 1
        tool = tools[0]
        assert func_obj.comment in tool.desc

        input_args = {"a": 5, "b": 6}
        result = json.loads(tool.func(**input_args))["value"]
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
        assert func_obj.comment in tool.desc
        input_args = {"a": 3, "b": 4}
        result = json.loads(tool.func(**input_args))["value"]
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
        result1 = json.loads(tool1.func(**input_args))["value"]
        result2 = json.loads(tool2.func(**input_args))["value"]
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


def test_convert_to_dspy_schema_with_valid_function_info():
    """
    Test convert_to_dspy_schema with valid FunctionInfo input.
    """
    # Generate mock FunctionInfo using the provided generate_function_info
    function_info = generate_function_info()

    # Convert the FunctionInfo into DSPy-compatible schema
    result_schema = UCFunctionToolkit.convert_to_dspy_schema(function_info)

    # Expected output
    expected_schema = {
        "args_dict": {"x": {"anyOf": [{"type": "string"}, {"type": "null"}]}},
        "args_desc": {"x": "test comment"},
        "args_type": {"x": typing.Optional[str]},
    }

    assert result_schema == expected_schema, (
        "The generated schema does not match the expected output."
    )


@pytest.mark.asyncio
async def test_uc_function_to_dspy_tool(uc_client):
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
        tool_wrapper = UCFunctionToolkit.uc_function_to_dspy_tool(
            function_name="catalog.schema.test", client=uc_client
        )

        # Validate tool wrapper attributes
        assert tool_wrapper.uc_function_name == "catalog.schema.test"
        assert tool_wrapper.client_config == uc_client.to_dict()

        # Validate underlying DSPy tool
        tool = tool_wrapper.tool
        assert tool.name == "catalog__schema__test"
        assert tool.desc == "Executes Python code and returns its stdout."

        result = json.loads(tool.func(x="some_string"))["value"]
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
        tool_wrapper = UCFunctionToolkit.uc_function_to_dspy_tool(
            function_name="catalog.schema.test", client=uc_client
        )

        with pytest.raises(ValueError, match="Parameter x is required but not provided."):
            tool_wrapper.tool.func(**invalid_inputs)


def test_generate_dspy_tool_list(uc_client):
    """
    Test the generate_dspy_tool_list method of UCFunctionToolkit.
    """
    # Mock UCFunctionToolkit instance with a DSPy tool
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

    # Generate DSPy tool list
    dspy_tools = toolkit.tools
    tools = toolkit.tools

    # Verify the output
    assert len(dspy_tools) == 1, "The DSPy tool list should contain exactly one tool."

    dspy_tool = dspy_tools[0]
    tool = tools[0]
    assert hasattr(dspy_tool, "name"), "The tool should have a name attribute."
    assert tool.name == "catalog__schema__test_function", (
        "The tool's name does not match the expected name."
    )
    assert tool.desc == "Executes Python code and returns its stdout.", (
        "The tool's description does not match the expected description."
    )
    assert hasattr(tool, "args"), "The tool should have args attribute."
    assert "x" in tool.args, "The tool's args should include the parameter x."


def test_toolkit_convert_to_dspy_schema_no_parameters():
    """Test schema conversion with no parameters."""
    mock_function_info = generate_function_info()
    mock_function_info.input_params.parameters = []

    schema = UCFunctionToolkit.convert_to_dspy_schema(mock_function_info)

    assert schema["args_dict"] == {}
    assert schema["args_desc"] == {}
    assert schema["args_type"] == {}


def test_toolkit_convert_to_dspy_schema_none_parameters():
    """Test schema conversion with None parameters raises error."""
    mock_function_info = generate_function_info()
    mock_function_info.input_params.parameters = None

    with pytest.raises(ValueError, match="Function input parameters are None"):
        UCFunctionToolkit.convert_to_dspy_schema(mock_function_info)


def test_toolkit_get_tool_methods(uc_client):
    """Test the get_tool and get_tool_wrapper methods."""
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

        # Test get_tool
        tool = toolkit.get_tool("catalog.schema.test_function")
        assert tool is not None
        assert tool.name == "catalog__schema__test_function"


def test_toolkit_with_wildcard_function_names(uc_client):
    """Test toolkit with wildcard function names."""
    with create_function_and_cleanup_oss(uc_client, schema=SCHEMA) as func_obj:
        toolkit = UCFunctionToolkit(function_names=[f"{CATALOG}.{SCHEMA}.*"], client=uc_client)

        # Should find at least the function we created
        assert len(toolkit.tools) >= 1
        assert func_obj.tool_name in [t.name for t in toolkit.tools]


def test_toolkit_error_handling(uc_client):
    """Test toolkit error handling for invalid function names."""
    # Test with incorrect function names
    with pytest.raises(
        ValueError,
        match="Could not find function info for the given function name or function info.",
    ):
        UCFunctionToolkit(function_names=["invalid.function.name"], client=uc_client)


def test_toolkit_validation_error_handling():
    """Test that toolkit properly handles validation errors."""
    # Test with empty function names
    with pytest.raises(ValueError, match="1 validation error for UCFunctionToolkit"):
        UCFunctionToolkit(function_names=[])

    # Test with None function names
    with pytest.raises(
        ValueError, match="1 validation error for UCFunctionToolkit\nfunction_names\n"
    ):
        UCFunctionToolkit(function_names=None)


def test_toolkit_to_dict_serialization(uc_client):
    """Test that toolkit can be properly serialized."""
    with create_function_and_cleanup_oss(uc_client, schema=SCHEMA) as func_obj:
        toolkit = UCFunctionToolkit(function_names=[func_obj.full_function_name], client=uc_client)

        # Test basic properties
        assert hasattr(toolkit, "function_names")
        assert hasattr(toolkit, "tools_dict")
        assert hasattr(toolkit, "client")
        assert hasattr(toolkit, "filter_accessible_functions")

        # Test that toolkit is a valid Pydantic model
        toolkit_dict = toolkit.model_dump()
        assert isinstance(toolkit_dict, dict)
        assert "function_names" in toolkit_dict


def test_toolkit_filter_accessible_functions(uc_client):
    """Test toolkit with accessible function filtering."""
    with create_function_and_cleanup_oss(uc_client, schema=SCHEMA) as func_obj:
        toolkit = UCFunctionToolkit(
            function_names=[func_obj.full_function_name],
            client=uc_client,
            filter_accessible_functions=True,
        )

        # Should still work with accessible functions
        assert len(toolkit.tools) == 1
        assert toolkit.get_tool(func_obj.full_function_name) is not None


def test_toolkit_convert_to_dspy_schema_strict_mode():
    """Test schema conversion with strict mode enabled."""
    mock_function_info = generate_function_info()

    schema = UCFunctionToolkit.convert_to_dspy_schema(mock_function_info, strict=True)

    assert "args_dict" in schema
    assert "args_desc" in schema
    assert "args_type" in schema

    # Check that the schema contains the expected parameter
    assert "x" in schema["args_dict"]
    assert "x" in schema["args_type"]
    assert schema["args_type"]["x"] == typing.Optional[str]


def test_toolkit_convert_to_dspy_schema_non_strict_mode():
    """Test schema conversion with strict mode disabled."""
    mock_function_info = generate_function_info()

    schema = UCFunctionToolkit.convert_to_dspy_schema(mock_function_info, strict=False)

    assert "args_dict" in schema
    assert "args_desc" in schema
    assert "args_type" in schema

    # Should still work but with potentially different type handling
    assert "x" in schema["args_dict"]
    assert "x" in schema["args_type"]
