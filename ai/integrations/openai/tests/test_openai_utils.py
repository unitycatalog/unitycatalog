import json
from unittest import mock

import pytest
from openai.types.chat.chat_completion_message_tool_call import Function

from tests.helper_functions import mock_chat_completion_response, mock_choice
from unitycatalog.ai.core.client import FunctionExecutionResult
from unitycatalog.ai.core.databricks import DatabricksFunctionClient
from unitycatalog.ai.openai.utils import generate_tool_call_messages


@pytest.fixture
def client() -> DatabricksFunctionClient:
    with mock.patch(
        "unitycatalog.ai.core.databricks.get_default_databricks_workspace_client",
        return_value=mock.Mock(),
    ):
        return DatabricksFunctionClient()


def test_generate_tool_call_messages(client: DatabricksFunctionClient):
    response = mock_chat_completion_response(
        function=Function(
            name="ml__test__test_func",
            arguments='{"arg1": "value1"}',
        ),
    )
    with mock.patch.object(
        client,
        "execute_function",
        return_value=FunctionExecutionResult(format="SCALAR", value="result"),
    ):
        messages = generate_tool_call_messages(response=response, client=client)
        assert len(messages) == 2
        assert messages[0]["role"] == "assistant"
        assert messages[-1] == {
            "role": "tool",
            "content": json.dumps({"content": "result"}),
            "tool_call_id": "call_mock",
        }


def test_generate_tool_call_messages_multiple_choices(client: DatabricksFunctionClient):
    response = mock_chat_completion_response(
        choices=[
            mock_choice(
                Function(
                    name="ml__test__test_func1",
                    arguments='{"arg1": "value1"}',
                )
            ),
            mock_choice(
                Function(
                    name="ml__test__test_func2",
                    arguments='{"arg1": "value2"}',
                )
            ),
        ]
    )

    def mock_execute_function(func_name, arguments):
        return FunctionExecutionResult(format="SCALAR", value=arguments["arg1"])

    with mock.patch.object(client, "execute_function", side_effect=mock_execute_function):
        messages = generate_tool_call_messages(response=response, client=client, choice_index=0)
        assert len(messages) == 2
        assert messages[0]["role"] == "assistant"
        assert messages[1] == {
            "role": "tool",
            "content": json.dumps({"content": "value1"}),
            "tool_call_id": "call_mock",
        }

        messages = generate_tool_call_messages(response=response, client=client, choice_index=1)
        assert len(messages) == 2
        assert messages[0]["role"] == "assistant"
        assert messages[1] == {
            "role": "tool",
            "content": json.dumps({"content": "value2"}),
            "tool_call_id": "call_mock",
        }
