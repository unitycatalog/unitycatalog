from unittest.mock import MagicMock

import pytest

from unitycatalog.ai.bedrock.utils import (
    execute_tool_calls,
    extract_response_details,
    extract_tool_calls,
    generate_tool_call_session_state,
)


# Fixtures
@pytest.fixture
def mock_client():
    """Fixture to provide a mocked client."""
    return MagicMock()


@pytest.fixture
def test_context():
    """Fixture to provide common test context."""
    return {
        "catalog_name": "test_catalog",
        "schema_name": "test_schema",
        "function_name": "test_function",
    }


def test_extract_response_details():
    """Test the extract_response_details function."""
    response = {
        "completion": [
            {"chunk": {"bytes": b"chunk1"}},
            {"chunk": {"bytes": b"chunk2"}},
            {"returnControl": {"key": "value"}},
        ]
    }
    result = extract_response_details(response)
    assert result == {"chunks": "chunk1chunk2", "tool_calls": []}


def test_extract_tool_calls(mock_client, test_context):
    """Test the extract_tool_calls function."""
    tool_calls = {
        "completion": [
            {
                "returnControl": {
                    "invocationId": "12345",
                    "invocationInputs": [
                        {
                            "functionInvocationInput": {
                                "actionGroup": "example_action_group",
                                "function": "example_function",
                                "parameters": [{"name": "param1", "value": "value1"}],
                            }
                        }
                    ],
                }
            }
        ]
    }
    result = extract_tool_calls(tool_calls)
    assert result == [
        {
            "action_group": "example_action_group",
            "function": "example_function",
            "function_name": "example_action_group__example_function",
            "parameters": {"param1": "value1"},
            "invocation_id": "12345",
        }
    ]


def test_generate_tool_call_session_state(mock_client, test_context):
    """Test the generate_tool_call_session_state function."""
    tool_result = {"result": "success", "invocation_id": "12345"}  # Added "invocation_id"
    tool_call = {
        "toolCall": {"name": "example_tool"},
        "action_group": "example_action_group",
        "function": "example_function",
    }
    session_state = generate_tool_call_session_state(tool_result, tool_call)
    assert session_state == {
        "invocationId": "12345",
        "returnControlInvocationResults": [
            {
                "functionResult": {
                    "actionGroup": "example_action_group",
                    "function": "example_function",
                    "confirmationState": "CONFIRM",
                    "responseBody": {"TEXT": {"body": "success"}},
                }
            }
        ],
    }


def test_execute_tool_calls(mock_client, test_context):
    """Test the execute_tool_calls function."""
    tool_calls = [
        {"name": "example_tool", "parameters": {"param1": "value1"}, "invocation_id": "12345"}
    ]
    mock_client.execute_function.return_value = MagicMock(value="success")
    results = execute_tool_calls(
        tool_calls,
        mock_client,
        catalog_name=test_context["catalog_name"],
        schema_name=test_context["schema_name"],
        function_name=test_context["function_name"],
    )
    assert results == [{"invocation_id": "12345", "result": "success"}]
    mock_client.execute_function.assert_called_once_with(
        "test_catalog.test_schema.test_function", {"param1": "value1"}
    )
