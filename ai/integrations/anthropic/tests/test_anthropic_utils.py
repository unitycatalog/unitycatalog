from contextlib import nullcontext
from unittest import mock
from unittest.mock import Mock

import pytest
from anthropic.types import TextBlock, ToolUseBlock
from anthropic.types.message import Message
from databricks.sdk.service.catalog import ColumnTypeName, FunctionInfo

from unitycatalog.ai.anthropic.utils import (
    ToolCallData,
    extract_tool_call_data,
    generate_tool_call_messages,
)
from unitycatalog.ai.core.base import BaseFunctionClient
from unitycatalog.ai.core.databricks import DatabricksFunctionClient
from unitycatalog.ai.test_utils.client_utils import TEST_IN_DATABRICKS
from unitycatalog.ai.test_utils.function_utils import (
    RETRIEVER_OUTPUT_CSV,
    RETRIEVER_OUTPUT_SCALAR,
    RETRIEVER_TABLE_FULL_DATA_TYPE,
    RETRIEVER_TABLE_RETURN_PARAMS,
)


@pytest.fixture
def mock_client():
    client = Mock(spec=BaseFunctionClient)
    client.execute_function = Mock(return_value=Mock(value="65 degrees"))
    return client


@pytest.fixture
def mock_message_single_tool():
    tool_use_block = ToolUseBlock(
        id="toolu_01A09q90qw90lq917835lq9",
        name="catalog__schema__get_weather",
        input={"location": "San Francisco, CA", "unit": "celsius"},
        type="tool_use",
    )
    response = Mock(spec=Message)
    response.stop_reason = "tool_use"
    response.content = [tool_use_block]
    response.role = "assistant"
    return response


@pytest.fixture
def mock_message_multiple_tools():
    tool_use_block_1 = ToolUseBlock(
        id="toolu_01A09q90qw90lq917835lq9",
        name="catalog__schema__get_weather",
        input={"location": "San Francisco, CA", "unit": "celsius"},
        type="tool_use",
    )
    tool_use_block_2 = ToolUseBlock(
        id="toolu_02B24q89qw70lq986423lq6",
        name="catalog__schema__get_weather",
        input={"location": "New York, NY", "unit": "fahrenheit"},
        type="tool_use",
    )
    response = Mock(spec=Message)
    response.stop_reason = "tool_use"
    response.content = [tool_use_block_1, tool_use_block_2]
    response.role = "assistant"
    return response


@pytest.fixture
def dummy_history():
    return {
        "role": "user",
        "content": [
            {
                "citations": None,
                "type": "text",
                "text": "What's the weather like in San Francisco?",
            }
        ],
    }


def test_extract_tool_call_data_single_tool(mock_message_single_tool):
    result = extract_tool_call_data(response=mock_message_single_tool)

    assert len(result) == 1
    tool_call = result[0]

    assert isinstance(tool_call, ToolCallData)
    assert tool_call.function_name == "catalog.schema.get_weather"
    assert tool_call.arguments == {"location": "San Francisco, CA", "unit": "celsius"}
    assert tool_call.tool_use_id == "toolu_01A09q90qw90lq917835lq9"


def test_extract_tool_call_data_multiple_tools(mock_message_multiple_tools):
    result = extract_tool_call_data(response=mock_message_multiple_tools)

    assert len(result) == 2
    tool_call_1, tool_call_2 = result

    assert tool_call_1.function_name == "catalog.schema.get_weather"
    assert tool_call_1.arguments == {"location": "San Francisco, CA", "unit": "celsius"}
    assert tool_call_1.tool_use_id == "toolu_01A09q90qw90lq917835lq9"

    assert tool_call_2.function_name == "catalog.schema.get_weather"
    assert tool_call_2.arguments == {"location": "New York, NY", "unit": "fahrenheit"}
    assert tool_call_2.tool_use_id == "toolu_02B24q89qw70lq986423lq6"


def test_tool_call_data_execute(mock_client):
    tool_call = ToolCallData(
        function_name="catalog__schema__get_weather",
        arguments={"location": "San Francisco, CA", "unit": "celsius"},
        tool_use_id="toolu_01A09q90qw90lq917835lq9",
    )
    result = tool_call.execute(mock_client)

    mock_client.execute_function.assert_called_once_with(
        "catalog__schema__get_weather",
        {"location": "San Francisco, CA", "unit": "celsius"},
        enable_retriever_tracing=False,
    )
    assert result == "65 degrees"


def test_generate_tool_call_messages_single_tool(
    mock_message_single_tool, mock_client, dummy_history
):
    result = generate_tool_call_messages(
        response=mock_message_single_tool, conversation_history=dummy_history, client=mock_client
    )

    assert len(result) == 3
    conversational_history, assistant_message, tool_response_message = result

    assert conversational_history == dummy_history
    assert isinstance(assistant_message, dict)

    assert tool_response_message["role"] == "user"
    tool_message_content = tool_response_message["content"][0]
    assert tool_message_content["type"] == "tool_result"
    assert tool_message_content["tool_use_id"] == "toolu_01A09q90qw90lq917835lq9"
    assert tool_message_content["content"] == "65 degrees"


def test_generate_tool_call_messages_multiple_tools(
    mock_message_multiple_tools, mock_client, dummy_history
):
    result = generate_tool_call_messages(
        response=mock_message_multiple_tools, conversation_history=dummy_history, client=mock_client
    )

    assert len(result) == 3
    conversational_history, assistant_message, tool_response_message = result

    assert conversational_history == dummy_history
    assert isinstance(assistant_message, dict)

    assert tool_response_message["role"] == "user"
    tool_message_content = tool_response_message["content"]

    assert len(tool_message_content) == 2

    assert tool_message_content[0]["type"] == "tool_result"
    assert tool_message_content[0]["tool_use_id"] == "toolu_01A09q90qw90lq917835lq9"
    assert tool_message_content[0]["content"] == "65 degrees"

    assert tool_message_content[1]["type"] == "tool_result"
    assert tool_message_content[1]["tool_use_id"] == "toolu_02B24q89qw70lq986423lq6"
    assert tool_message_content[1]["content"] == "65 degrees"


def test_generate_tool_call_messages_no_tool_use(mock_client, dummy_history):
    response = Mock(spec=Message)
    response.stop_reason = "stop"
    response.content = []
    response.role = "assistant"

    result = generate_tool_call_messages(
        response=response, conversation_history=dummy_history, client=mock_client
    )
    assert len(result) == 2

    history, assistant_message = result

    assert history == dummy_history
    assert assistant_message == {"role": "assistant", "content": []}


def test_generate_tool_call_messages_empty_content(mock_client, dummy_history):
    response = Mock(spec=Message)
    response.stop_reason = "tool_use"
    response.content = []
    response.role = "assistant"

    result = generate_tool_call_messages(
        response=response, conversation_history=dummy_history, client=mock_client
    )

    assert len(result) == 2

    history, assistant_message = result

    assert history == dummy_history

    assert assistant_message == {"role": "assistant", "content": []}


def test_generate_tool_call_messages_validate_default_client(
    mock_message_single_tool, dummy_history
):
    client = None
    with (
        mock.patch(
            "unitycatalog.ai.core.base._is_databricks_client_available",
            return_value=False,
        ),
        pytest.raises(ValueError, match="No client provided"),
    ):
        generate_tool_call_messages(
            response=mock_message_single_tool, conversation_history=dummy_history, client=client
        )


def test_generate_tool_call_messages_with_text_block(mock_client, dummy_history):
    text_block = TextBlock(text="Fetching weather data for San Francisco, CA...", type="text")
    tool_use_block = ToolUseBlock(
        id="toolu_01A09q90qw90lq917835lq9",
        name="catalog__schema__get_weather",
        input={"location": "San Francisco, CA", "unit": "celsius"},
        type="tool_use",
    )
    response = Mock(spec=Message)
    response.stop_reason = "tool_use"
    response.content = [text_block, tool_use_block]
    response.role = "assistant"

    result = generate_tool_call_messages(
        response=response, conversation_history=dummy_history, client=mock_client
    )

    assert len(result) == 3
    conversational_history, assistant_message, tool_response_message = result

    assert conversational_history == dummy_history

    assert assistant_message["content"][0]["type"] == "text"
    assert (
        assistant_message["content"][0]["text"] == "Fetching weather data for San Francisco, CA..."
    )

    assert tool_response_message["content"][0]["type"] == "tool_result"
    assert tool_response_message["content"][0]["tool_use_id"] == "toolu_01A09q90qw90lq917835lq9"
    assert tool_response_message["content"][0]["content"] == "65 degrees"


def test_generate_tool_call_messages_with_invalid_tool_use_block(mock_client, dummy_history):
    tool_use_block = Mock(spec=ToolUseBlock)
    tool_use_block.id = None
    tool_use_block.name = "catalog__schema__get_weather"
    tool_use_block.input = {"location": "San Francisco, CA", "unit": "celsius"}
    tool_use_block.type = "tool_use"

    response = Mock(spec=Message)
    response.stop_reason = "tool_use"
    response.content = [tool_use_block]
    response.role = "assistant"

    with pytest.raises(ValueError, match="Tool use block is missing an ID"):
        generate_tool_call_messages(
            response=response, conversation_history=dummy_history, client=mock_client
        )


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
def test_generate_tool_call_messages_with_tracing(
    dummy_history, format, function_output, data_type, full_data_type, return_params
):
    if not TEST_IN_DATABRICKS:
        patch_registry = mock.patch(
            "mlflow.tracking._model_registry.utils._get_registry_uri_from_spark_session",
            return_value="databricks-uc",
        )
    else:
        patch_registry = nullcontext()

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
        mock.patch(
            "unitycatalog.ai.core.base._is_databricks_client_available",
            return_value=False,
        ),
        patch_registry,
    ):
        function_mock = Mock(
            spec=FunctionInfo,
            name=f"catalog.schema.retriever_tool_{format}",
            full_name=f"catalog.schema.retriever_tool_{format}",
            data_type=data_type,
            full_data_type=full_data_type,
            return_params=return_params,
            autospec=True,
        )

        mock_client = DatabricksFunctionClient()
        mock_client._execute_uc_function = Mock(
            return_value=Mock(format=format, value=function_output, error=None)
        )
        mock_client.validate_input_params = Mock()
        mock_client.get_function = Mock(return_value=function_mock)

        text_block = TextBlock(text="Fetching documents...", type="text")
        tool_use_block = ToolUseBlock(
            id="toolu_01A09q90qw90lq917835lq9",
            name=f"catalog__schema__retriever_tool_{format}",
            input={"query": "What is Databricks Partner Connect?"},
            type="tool_use",
        )
        response = Mock(spec=Message)
        response.stop_reason = "tool_use"
        response.content = [text_block, tool_use_block]
        response.role = "assistant"

        import mlflow

        if TEST_IN_DATABRICKS:
            import mlflow.tracking._model_registry.utils

            mlflow.tracking._model_registry.utils._get_registry_uri_from_spark_session = (
                lambda: "databricks-uc"
            )

        mlflow.anthropic.autolog()

        generate_tool_call_messages(
            response=response, conversation_history=dummy_history, client=mock_client
        )

        trace = mlflow.get_last_active_trace()
        assert trace is not None
        assert trace.data.spans[0].name == function_mock.full_name
        assert trace.info.execution_time_ms is not None
        assert trace.data.request == '{"query": "What is Databricks Partner Connect?"}'
        assert trace.data.response == RETRIEVER_OUTPUT_SCALAR

        mlflow.anthropic.autolog(disable=True)
