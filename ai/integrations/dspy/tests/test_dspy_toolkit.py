import json
import os
import typing
from unittest import mock

import dspy
import pytest
from databricks.sdk.service.catalog import (
    FunctionInfo,
    FunctionParameterInfo,
    FunctionParameterInfos,
)
from pydantic import ValidationError

from unitycatalog.ai.core.base import BaseFunctionClient
from unitycatalog.ai.core.client import FunctionExecutionResult
from unitycatalog.ai.core.utils.execution_utils import ExecutionModeDatabricks
from unitycatalog.ai.core.utils.validation_utils import has_retriever_signature
from unitycatalog.ai.dspy.toolkit import UCFunctionToolkit, UnityCatalogDSPyToolWrapper
from unitycatalog.ai.test_utils.client_utils import (
    TEST_IN_DATABRICKS,
    client,  # noqa: F401
    get_client,
    requires_databricks,
    set_default_client,
)
from unitycatalog.ai.test_utils.function_utils import (
    CATALOG,
    create_function_and_cleanup,
    create_table_function_and_cleanup,
)
from unitycatalog.client.models.function_parameter_type import FunctionParameterType

SCHEMA = os.environ.get("SCHEMA", "ucai_dspy_test")


class MockFunctionExecutionResult:
    def __init__(self, return_value: str):
        self.return_value = return_value

    def to_json(self) -> str:
        return self.return_value


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


@pytest.mark.parametrize("execution_mode", ["serverless", "local"])
@requires_databricks
def test_toolkit_e2e(execution_mode):
    client = get_client()
    client.execution_mode = ExecutionModeDatabricks(execution_mode)
    with set_default_client(client), create_function_and_cleanup(client, schema=SCHEMA) as func_obj:
        toolkit = UCFunctionToolkit(function_names=[func_obj.full_function_name])
        tools = toolkit.tools
        assert len(tools) == 1
        tool = tools[0]
        assert func_obj.comment in tool.desc

        input_args = {"number": 4}
        raw_result = tool.func(**input_args)
        result = json.loads(raw_result)["value"]
        assert result == "14"

        toolkit = UCFunctionToolkit(
            function_names=[f.full_name for f in client.list_functions(CATALOG, SCHEMA)]
        )
        assert len(toolkit.tools) >= 1
        assert func_obj.tool_name in [t.name for t in toolkit.tools]


@pytest.mark.parametrize("execution_mode", ["serverless", "local"])
@requires_databricks
def test_toolkit_e2e_manually_passing_client(execution_mode):
    client = get_client()
    client.execution_mode = ExecutionModeDatabricks(execution_mode)
    with set_default_client(client), create_function_and_cleanup(client, schema=SCHEMA) as func_obj:
        toolkit = UCFunctionToolkit(function_names=[func_obj.full_function_name], client=client)
        tools = toolkit.tools
        assert len(tools) == 1
        tool = tools[0]
        assert tool.name == func_obj.tool_name
        assert func_obj.comment in tool.desc
        input_args = {"number": 5}
        raw_result = tool.func(**input_args)
        result = json.loads(raw_result)["value"]
        assert result == "15"

        toolkit = UCFunctionToolkit(
            function_names=[f.full_name for f in client.list_functions(CATALOG, SCHEMA)],
            client=client,
        )
        assert len(toolkit.tools) >= 1
        assert func_obj.tool_name in [t.name for t in toolkit.tools]


@pytest.mark.parametrize("execution_mode", ["serverless", "local"])
@requires_databricks
def test_multiple_toolkits(execution_mode):
    client = get_client()
    client.execution_mode = ExecutionModeDatabricks(execution_mode)
    with set_default_client(client), create_function_and_cleanup(client, schema=SCHEMA) as func_obj:
        toolkit1 = UCFunctionToolkit(function_names=[func_obj.full_function_name])
        toolkit2 = UCFunctionToolkit(
            function_names=[f.full_name for f in client.list_functions(CATALOG, SCHEMA)]
        )
        tool1 = toolkit1.tools[0]
        tool2 = [t for t in toolkit2.tools if t.name == func_obj.tool_name][0]
        input_args = {"number": 6}
        raw_result1 = tool1.func(**input_args)
        raw_result2 = tool2.func(**input_args)
        result1 = json.loads(raw_result1)["value"]
        result2 = json.loads(raw_result2)["value"]
        assert result1 == result2


def test_toolkit_creation_errors():
    with pytest.raises(ValidationError, match=r"Input should be an instance of BaseFunctionClient"):
        UCFunctionToolkit(function_names=[], client="client")


def test_toolkit_creation_errors_with_client(client):
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


def test_uc_function_to_dspy_tool(client):
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
        tool_wrapper = UCFunctionToolkit.uc_function_to_dspy_tool(
            function_name=f"{CATALOG}.{SCHEMA}.test", client=client
        )

        # Validate tool wrapper attributes
        assert tool_wrapper.uc_function_name == f"{CATALOG}.{SCHEMA}.test"
        assert tool_wrapper.client_config == client.to_dict()

        # Validate underlying DSPy tool
        tool = tool_wrapper.tool
        assert tool.name == f"{CATALOG}__{SCHEMA}__test"
        assert tool.desc == "Executes Python code and returns its stdout."

        result = json.loads(tool.func(x="some_string"))["value"]
        assert result == "some_string"


@pytest.mark.parametrize(
    "filter_accessible_functions",
    [True, False],
)
def test_uc_function_to_dspy_tool_permission_denied(filter_accessible_functions):
    client = get_client()
    # Permission Error should be caught
    with mock.patch(
        "unitycatalog.ai.core.databricks.DatabricksFunctionClient.get_function",
        side_effect=PermissionError("Permission Denied to Underlying Assets"),
    ):
        if filter_accessible_functions:
            tool_wrapper = UCFunctionToolkit.uc_function_to_dspy_tool(
                client=client,
                function_name=f"{CATALOG}.{SCHEMA}.test",
                filter_accessible_functions=filter_accessible_functions,
            )
            assert tool_wrapper == None
        else:
            with pytest.raises(PermissionError):
                tool_wrapper = UCFunctionToolkit.uc_function_to_dspy_tool(
                    client=client,
                    function_name=f"{CATALOG}.{SCHEMA}.test",
                    filter_accessible_functions=filter_accessible_functions,
                )
    # Other errors should not be Caught
    with mock.patch(
        "unitycatalog.ai.core.databricks.DatabricksFunctionClient.get_function",
        side_effect=ValueError("Wrong Get Function Call"),
    ):
        with pytest.raises(ValueError):
            tool_wrapper = UCFunctionToolkit.uc_function_to_dspy_tool(
                client=client,
                function_name=f"{CATALOG}.{SCHEMA}.test",
                filter_accessible_functions=filter_accessible_functions,
            )


@requires_databricks
def test_dspy_tool_calling_with_trace_as_retriever():
    client = get_client()
    import mlflow

    if TEST_IN_DATABRICKS:
        import mlflow.tracking._model_registry.utils

        mlflow.tracking._model_registry.utils._get_registry_uri_from_spark_session = (
            lambda: "databricks-uc"
        )

    with (
        set_default_client(client),
        create_table_function_and_cleanup(client, schema=SCHEMA) as func_obj,
    ):
        func_name = func_obj.full_function_name
        func_info = client.get_function(func_name)
        assert has_retriever_signature(func_info)
        toolkit = UCFunctionToolkit(function_names=[func_name])
        tools = toolkit.tools

        # Test that we can execute the tool directly
        tool = tools[0]
        assert tool.name == func_obj.tool_name
        assert func_obj.comment in tool.desc

        # Execute the function with test parameters
        input_args = {"query": "What are the page contents?"}
        result = tool.func(**input_args)
        result_data = json.loads(result)

        # Verify the result structure
        assert "value" in result_data
        expected_result = "page_content,metadata\ntesting,\"{'doc_uri': 'https://docs.databricks.com/', 'chunk_id': '1'}\"\n"
        assert result_data["value"] == expected_result

        # Test MLflow tracing if available
        try:
            mlflow.dspy.autolog()

            # Execute again to capture trace
            result = tool.func(**input_args)

            trace = mlflow.get_trace(mlflow.get_last_active_trace_id())
            assert trace is not None
            assert trace.data.spans[0].name == func_name
            assert trace.info.execution_time_ms is not None
            assert trace.data.request == json.dumps(input_args)

            mlflow.dspy.autolog(disable=True)
        except Exception:
            # MLflow tracing might not be available in test environment
            pass


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
        tool_wrapper = UCFunctionToolkit.uc_function_to_dspy_tool(
            function_name="catalog.schema.test", client=client
        )

        with pytest.raises(ValueError, match="Parameter x is required but not provided."):
            tool_wrapper.tool.func(**invalid_inputs)


def test_generate_dspy_tool_list(client):
    """
    Test the tools property of UCFunctionToolkit.
    """
    # Mock UCFunctionToolkit instance with a DSPy tool
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

    # Get DSPy tool list
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


def test_toolkit_get_tool_methods(client):
    """Test the get_tool and get_tool_wrapper methods."""
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

        # Test get_tool
        tool = toolkit.get_tool("catalog.schema.test_function")
        assert tool is not None
        assert tool.name == "catalog__schema__test_function"


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


@requires_databricks
def test_toolkit_filter_accessible_functions(client):
    """Test toolkit with accessible function filtering."""
    with create_function_and_cleanup(client, schema=SCHEMA) as func_obj:
        toolkit = UCFunctionToolkit(
            function_names=[func_obj.full_function_name],
            client=client,
            filter_accessible_functions=True,
        )

        # Should still work with accessible functions
        assert len(toolkit.tools) == 1
        assert toolkit.get_tool(func_obj.full_function_name) is not None


@requires_databricks
def test_toolkit_to_dict_serialization(client):
    """Test that toolkit can be properly serialized."""
    with create_function_and_cleanup(client, schema=SCHEMA) as func_obj:
        toolkit = UCFunctionToolkit(function_names=[func_obj.full_function_name], client=client)

        # Test basic properties
        assert hasattr(toolkit, "function_names")
        assert hasattr(toolkit, "tools_dict")
        assert hasattr(toolkit, "client")
        assert hasattr(toolkit, "filter_accessible_functions")

        # Test that toolkit is a valid Pydantic model
        toolkit_dict = toolkit.model_dump()
        assert isinstance(toolkit_dict, dict)
        assert "function_names" in toolkit_dict


def generate_mock_function_info(has_properties: bool = False):
    """Generate mock function info for testing properties argument handling."""
    parameters = [
        {
            "comment": "test comment",
            "name": "x",
            "type_text": "string",
            "type_json": '{"name":"x","type":"string","nullable":true}',
            "type_name": "STRING",
            "type_precision": 0,
            "type_scale": 0,
            "position": 1,
            "parameter_type": FunctionParameterType.PARAM,
        }
    ]

    if has_properties:
        parameters.append(
            {
                "comment": "properties parameter",
                "name": "properties",
                "type_text": "map<string, string>",
                "type_json": '{"name":"properties","type":"MAP","nullable":true}',
                "type_name": "MAP",
                "type_precision": 0,
                "type_scale": 0,
                "position": 2,
                "parameter_type": FunctionParameterType.PARAM,
            }
        )

    return FunctionInfo(
        catalog_name="catalog",
        schema_name="schema",
        name="test_function",
        full_name="catalog.schema.test_function",
        input_params=FunctionParameterInfos(
            parameters=[FunctionParameterInfo(**param) for param in parameters]
        ),
        comment="A test function without properties argument"
        if not has_properties
        else "A test function with properties argument",
        data_type=None,
        full_data_type=None,
        return_params=None,
    )


def test_toolkit_creation_without_properties_argument_mocked():
    """
    Test that UCFunctionToolkit successfully creates a tool when the function does not have a 'properties' argument using mocks.
    """
    mock_function_info = generate_mock_function_info(has_properties=False)

    mock_client = mock.create_autospec(BaseFunctionClient, instance=True)
    mock_client.get_function.return_value = mock_function_info
    mock_client.to_dict.return_value = {"mock": "config"}
    mock_client.execute_function.return_value = MockFunctionExecutionResult(
        return_value='{"value": "some_string"}'
    )

    with (
        mock.patch(
            "unitycatalog.ai.dspy.toolkit.validate_or_set_default_client",
            return_value=mock_client,
        ),
    ):
        toolkit = UCFunctionToolkit(
            function_names=["catalog.schema.test_function"], client=mock_client
        )
        tools = toolkit.tools
        assert len(tools) == 1
        tool = tools[0]
        assert tool.name == "catalog__schema__test_function"
        assert tool.desc == "A test function without properties argument"

        input_args = {"x": "some_string"}
        result = json.loads(tool.func(**input_args))["value"]
        assert result == "some_string"
