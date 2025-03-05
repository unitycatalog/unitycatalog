import json
from unittest import mock
from unittest.mock import Mock

import pytest
from databricks.sdk.service.catalog import ColumnTypeName, FunctionInfo
from openai.types.chat.chat_completion_message_tool_call import Function

from tests.helper_functions import mock_chat_completion_response, mock_choice
from unitycatalog.ai.core.base import FunctionExecutionResult
from unitycatalog.ai.core.databricks import DatabricksFunctionClient
from unitycatalog.ai.openai.utils import generate_tool_call_messages
from unitycatalog.ai.test_utils.client_utils import TEST_IN_DATABRICKS
from unitycatalog.ai.test_utils.function_utils import (
    RETRIEVER_OUTPUT_CSV,
    RETRIEVER_OUTPUT_SCALAR,
    RETRIEVER_TABLE_FULL_DATA_TYPE,
    RETRIEVER_TABLE_RETURN_PARAMS,
)


@pytest.fixture
def client() -> DatabricksFunctionClient:
    with (
        mock.patch(
            "unitycatalog.ai.core.databricks.get_default_databricks_workspace_client",
            return_value=mock.Mock(),
        ),
        mock.patch(
            "unitycatalog.ai.core.databricks._validate_databricks_connect_available",
            return_value=True,
        ),
        mock.patch(
            "unitycatalog.ai.core.databricks.DatabricksFunctionClient.initialize_spark_session",
            return_value=None,
        ),
    ):
        return DatabricksFunctionClient()


@pytest.mark.parametrize(
    "format, function_output",
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
def test_generate_tool_call_messages_with_tracing(
    client: DatabricksFunctionClient,
    format,
    function_output,
    data_type,
    full_data_type,
    return_params,
):
    function_name = f"ml__test__test_func_{format}"
    function_input = '{"query": "What is Databricks Partner Connect?"}'

    response = mock_chat_completion_response(
        function=Function(name=function_name, arguments=function_input),
    )

    function_mock = Mock(
        spec=FunctionInfo,
        name=f"test_func_{format}",
        full_name=f"ml.test.test_func_{format}",
        data_type=data_type,
        full_data_type=full_data_type,
        return_params=return_params,
        autospec=True,
    )

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

        if TEST_IN_DATABRICKS:
            import mlflow.tracking._model_registry.utils

            mlflow.tracking._model_registry.utils._get_registry_uri_from_spark_session = (
                lambda: "databricks-uc"
            )

        mlflow.openai.autolog()

        generate_tool_call_messages(response=response, client=client)

        trace = mlflow.get_last_active_trace()
        assert trace is not None
        assert trace.data.spans[0].name == function_mock.full_name
        assert trace.info.execution_time_ms is not None
        assert trace.data.request == function_input
        assert trace.data.response == RETRIEVER_OUTPUT_SCALAR

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

    def mock_execute_function(func_name, arguments, enable_retriever_tracing=False):
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
