import os
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest
from databricks.sdk.service.catalog import (
    FunctionInfo,
    FunctionParameterInfo,
    FunctionParameterInfos,
)
from pydantic import ValidationError

from unitycatalog.ai.core.base import FunctionExecutionResult
from unitycatalog.ai.core.databricks import ExecutionMode
from unitycatalog.ai.litellm.toolkit import UCFunctionToolkit
from unitycatalog.ai.test_utils.client_utils import (
    client,  # noqa: F401
    get_client,
    requires_databricks,
    set_default_client,
)
from unitycatalog.ai.test_utils.function_utils import CATALOG, create_function_and_cleanup

SCHEMA = os.environ.get("SCHEMA", "ucai_litellm_test")


@pytest.fixture
def tools_mock():
    """Fixture that returns a mock list of tool call responses."""
    return [
        MagicMock(
            function=MagicMock(
                arguments='{"location": "San Francisco, CA", "unit": "celsius"}',
                name="get_current_weather",
            )
        ),
        MagicMock(
            function=MagicMock(
                arguments='{"location": "Tokyo, Japan", "unit": "celsius"}',
                name="get_current_weather",
            )
        ),
        MagicMock(
            function=MagicMock(
                arguments='{"location": "Paris, France", "unit": "celsius"}',
                name="get_current_weather",
            )
        ),
    ]


@contextmanager
def mock_litellm_completion(tools_mock):
    """Context manager to patch litellm.completion with a mock response."""
    mock_response = MagicMock()
    mock_response.choices[0].message.tool_calls = tools_mock
    with patch("litellm.completion", return_value=mock_response):
        yield mock_response


@pytest.fixture
def function_info():
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


def test_uc_function_to_litellm_tool(function_info, client):
    with (
        patch(
            "unitycatalog.ai.core.databricks.DatabricksFunctionClient.get_function",
            return_value=function_info,
        ),
        patch(
            "unitycatalog.ai.core.databricks.DatabricksFunctionClient.execute_function",
            return_value=FunctionExecutionResult(format="SCALAR", value="some_string"),
        ),
    ):
        function_name = f"{CATALOG}.{SCHEMA}.test"
        tool = UCFunctionToolkit.uc_function_to_litellm_tool(
            function_name=function_name, client=client
        )
        assert tool["name"] == function_name.replace(".", "__")


@pytest.mark.parametrize(
    "filter_accessible_functions",
    [True, False],
)
def uc_function_to_litellm_tool_permission_denied(filter_accessible_functions):
    client = get_client()
    # Permission Error should be caught
    with patch(
        "unitycatalog.ai.core.databricks.DatabricksFunctionClient.get_function",
        side_effect=PermissionError("Permission Denied to Underlying Assets"),
    ):
        if filter_accessible_functions:
            tool = UCFunctionToolkit.uc_function_to_litellm_tool(
                client=client,
                function_name=f"{CATALOG}.{SCHEMA}.test",
                filter_accessible_functions=filter_accessible_functions,
            )
            assert tool == None
        else:
            with pytest.raises(PermissionError):
                tool = UCFunctionToolkit.uc_function_to_litellm_tool(
                    client=client,
                    function_name=f"{CATALOG}.{SCHEMA}.test",
                    filter_accessible_functions=filter_accessible_functions,
                )
    # Other errors should not be Caught
    with patch(
        "unitycatalog.ai.core.databricks.DatabricksFunctionClient.get_function",
        side_effect=ValueError("Wrong Get Function Call"),
    ):
        with pytest.raises(ValueError):
            tool = UCFunctionToolkit.uc_function_to_litellm_tool(
                client=client,
                function_name=f"{CATALOG}.{SCHEMA}.test",
                filter_accessible_functions=filter_accessible_functions,
            )


@pytest.mark.parametrize("execution_mode", ["serverless", "local"])
@requires_databricks
def test_toolkit_e2e(execution_mode):
    client = get_client()
    client.execution_mode = ExecutionMode(execution_mode)
    with set_default_client(client), create_function_and_cleanup(client, schema=SCHEMA) as func_obj:
        toolkit = UCFunctionToolkit(
            function_names=[func_obj.full_function_name], return_direct=True
        )
        tool = toolkit.tools[0]
        assert tool["name"] == func_obj.tool_name
        assert tool["description"] == func_obj.comment

        toolkit = UCFunctionToolkit(function_names=[f"{CATALOG}.{SCHEMA}.*"])
        assert func_obj.tool_name in [t["name"] for t in toolkit.tools]


@pytest.mark.parametrize("execution_mode", ["serverless", "local"])
@requires_databricks
def test_toolkit_e2e_with_client(execution_mode):
    client = get_client()
    client.execution_mode = ExecutionMode(execution_mode)
    with set_default_client(client), create_function_and_cleanup(client, schema=SCHEMA) as func_obj:
        toolkit = UCFunctionToolkit(
            function_names=[func_obj.full_function_name],
            return_direct=True,
            client=client,
        )
        tool = toolkit.tools[0]
        assert tool["name"] == func_obj.tool_name
        assert tool["description"] == func_obj.comment

        toolkit = UCFunctionToolkit(function_names=[f"{CATALOG}.{SCHEMA}.*"])
        assert func_obj.tool_name in [t["name"] for t in toolkit.tools]


@requires_databricks
def test_multiple_toolkits():
    client = get_client()
    with set_default_client(client), create_function_and_cleanup(client, schema=SCHEMA) as func_obj:
        toolkit1 = UCFunctionToolkit(function_names=[func_obj.full_function_name])
        toolkit2 = UCFunctionToolkit(function_names=[f"{CATALOG}.{SCHEMA}.*"])
        toolkits = [toolkit1, toolkit2]

        assert all(isinstance(toolkit.tools[0], dict) for toolkit in toolkits)


def test_toolkit_creation_errors():
    with pytest.raises(ValidationError, match="Input should be an instance of BaseFunctionClient"):
        UCFunctionToolkit(function_names=[], client="client")


def test_toolkit_creation_errors_with_client(client):
    with pytest.raises(
        ValueError,
        match="Cannot create tool instances without function_names being provided.",
    ):
        UCFunctionToolkit(function_names=[], client=client)
