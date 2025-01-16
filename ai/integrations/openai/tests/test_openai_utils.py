import json
from unittest import mock

import pytest
from openai.types.chat.chat_completion_message_tool_call import Function

from tests.helper_functions import mock_chat_completion_response, mock_choice
from unitycatalog.ai.core.base import FunctionExecutionResult
from unitycatalog.ai.core.databricks import DatabricksFunctionClient
from unitycatalog.ai.openai.utils import generate_tool_call_messages
from unitycatalog.ai.test_utils.constants import RETRIEVER_OUTPUT_CSV, RETRIEVER_OUTPUT_SCALAR


@pytest.fixture
def client() -> DatabricksFunctionClient:
    with mock.patch(
        "unitycatalog.ai.core.databricks.get_default_databricks_workspace_client",
        return_value=mock.Mock(),
    ):
        return DatabricksFunctionClient()


@pytest.mark.parametrize(
    "format, function_output",
    [
        ("SCALAR", RETRIEVER_OUTPUT_SCALAR),
        ("CSV", RETRIEVER_OUTPUT_CSV),
    ],
)
def test_generate_tool_call_messages_with_tracing(
    client: DatabricksFunctionClient, format: str, function_output: str
):
    function_name = f"ml__test__test_func_{format}"
    function_input = '{"query": "What is Databricks Partner Connect?"}'
    trace_response = '[{"page_content": "# Technology partners\\n## What is Databricks Partner Connect?\\n", "metadata": {"similarity_score": 0.010178182, "chunk_id": "0217a07ba2fec61865ce408043acf1cf"}}, {"page_content": "# Technology partners\\n## What is Databricks?\\n", "metadata": {"similarity_score": 0.010178183, "chunk_id": "0217a07ba2fec61865ce408043acf1cd"}}]'

    response = mock_chat_completion_response(
        function=Function(name=function_name, arguments=function_input),
    )

    function_mock = mock.MagicMock()
    function_mock.name = function_name

    with (
        mock.patch.object(client, "get_function", return_value=function_mock),
        mock.patch.object(client, "validate_input_params"),
        mock.patch.object(
            client,
            "_execute_uc_function",
            return_value=FunctionExecutionResult(format=format, value=function_output),
        ),
    ):
        import mlflow

        mlflow.openai.autolog()

        generate_tool_call_messages(response=response, client=client)

        trace = mlflow.get_last_active_trace()
        assert trace is not None
        assert trace.info.execution_time_ms is not None
        assert trace.data.request == function_input
        assert trace.data.response == trace_response
        assert trace.data.spans[0].name == f"ml.test.test_func_{format}"

        mlflow.openai.autolog(disable=True)


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

    def mock_execute_function(func_name, arguments, enable_trace_as_retriever=False):
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
