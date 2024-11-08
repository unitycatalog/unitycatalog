import json
import os
from unittest import mock

import pytest
from autogen import ConversableAgent
from databricks.sdk.service.catalog import (
    FunctionInfo,
    FunctionParameterInfo,
    FunctionParameterInfos,
)
from pydantic import ValidationError

from unitycatalog.ai.autogen.toolkit import AutogenTool, UCFunctionToolkit
from unitycatalog.ai.core.client import FunctionExecutionResult
from unitycatalog.ai.test_utils.client_utils import (
    USE_SERVERLESS,
    client,  # noqa: F401
    get_client,
    requires_databricks,
    set_default_client,
)
from unitycatalog.ai.test_utils.function_utils import CATALOG, create_function_and_cleanup

SCHEMA = os.environ.get("SCHEMA", "ucai_autogen_test")


@pytest.fixture
def sample_autogen_tool():
    # Sample data for AutogenTool
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
    # Test the to_dict method
    expected_output = {
        "name": sample_autogen_tool.name,
        "description": sample_autogen_tool.description,
        "tool": sample_autogen_tool.tool,
    }
    assert sample_autogen_tool.to_dict() == expected_output


def test_autogen_tool_register_function(sample_autogen_tool):
    # Mock caller and executor
    mock_caller = mock.MagicMock(spec=ConversableAgent)
    mock_executor = mock.MagicMock(spec=ConversableAgent)
    mock_executor._wrap_function.return_value = "wrapped_function"

    # Invoke register_function with keyword arguments
    sample_autogen_tool.register_function(callers=mock_caller, executors=mock_executor)

    # Assertions
    mock_caller.update_tool_signature.assert_called_once_with(
        sample_autogen_tool.tool, is_remove=False
    )
    mock_executor._wrap_function.assert_called_once_with(sample_autogen_tool.fn)
    mock_executor.register_function.assert_called_once_with(
        {sample_autogen_tool.name: "wrapped_function"}
    )


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


def test_uc_function_to_autogen_tool(client):
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
        tool = UCFunctionToolkit.uc_function_to_autogen_tool(
            function_name=f"{CATALOG}.{SCHEMA}.test", client=client
        )
        result = json.loads(tool.fn(x="some_string"))["value"]
        assert result == "some_string"


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
        # Test with invalid input params that are not matching expected schema
        invalid_inputs = {"unexpected_key": "value"}
        tool = UCFunctionToolkit.uc_function_to_autogen_tool(
            function_name="catalog.schema.test", client=client
        )

        with pytest.raises(ValueError, match="Extra parameters provided that are not defined"):
            tool.fn(**invalid_inputs)


def test_register_with_agents(client):
    # Create a sample UCFunctionToolkit with mock function names
    function_names = ["catalog.schema.function"]

    # Create a realistic FunctionInfo object
    function_info = FunctionInfo(
        catalog_name="catalog",
        schema_name="schema",
        name="function",
        input_params=FunctionParameterInfos(parameters=[]),
        # Add other necessary attributes if required
    )

    # Create a mock AutogenTool with a mocked register_function method
    mock_autogen_tool = mock.create_autospec(AutogenTool)
    # Ensure that register_function is a Mock
    mock_autogen_tool.register_function = mock.MagicMock()

    with mock.patch.object(
        UCFunctionToolkit, "uc_function_to_autogen_tool", return_value=mock_autogen_tool
    ):
        toolkit = UCFunctionToolkit(function_names=function_names, client=client)

        # Mock agents
        mock_callers = mock.MagicMock(spec=ConversableAgent)
        mock_executors = mock.MagicMock(spec=ConversableAgent)

        # Call register_with_agents
        toolkit.register_with_agents(callers=mock_callers, executors=mock_executors)

        # Assert that register_function was called on the tool with the correct parameters
        for tool in toolkit.tools:
            tool.register_function.assert_called_once_with(
                callers=mock_callers, executors=mock_executors
            )
