import copy
from unittest.mock import Mock

import pytest
from litellm.types.utils import (
    ChatCompletionMessageToolCall,
    Choices,
    Function,
    Message,
    ModelResponse,
)

from unitycatalog.ai.core.client import BaseFunctionClient
from unitycatalog.ai.litellm.utils import (
    ToolCallData,
    extract_tool_call_data,
    generate_tool_call_messages,
)


@pytest.fixture
def mock_client():
    client = Mock(spec=BaseFunctionClient)
    client.execute_function = Mock(return_value=Mock(value="65 degrees"))
    return client


@pytest.fixture
def mock_single_tool() -> list[ChatCompletionMessageToolCall]:
    return [
        ChatCompletionMessageToolCall(
            function=Function(
                arguments='{"location": "San Francisco"}',
                name="main__default__get_current_weather",
            ),
            id="call_HSQTsZTvFfLySGY250051VQz",
            type="function",
        )
    ]


@pytest.fixture
def mock_multiple_tools() -> list[ChatCompletionMessageToolCall]:
    return [
        ChatCompletionMessageToolCall(
            function=Function(
                arguments='{"location": "San Francisco"}',
                name="main__default__get_current_weather",
            ),
            id="call_HSQTsZTvFfLySGY250051VQz",
            type="function",
        ),
        ChatCompletionMessageToolCall(
            function=Function(
                arguments='{"location": "Tokyo"}',
                name="main__default__get_current_weather",
            ),
            id="call_Ozd6L5vXIuKPlsmomMVPsHFM",
            type="function",
        ),
        ChatCompletionMessageToolCall(
            function=Function(
                arguments='{"location": "Paris"}',
                name="main__default__get_current_weather",
            ),
            id="call_MKQQrCiQhKXM6ZWtGTsASIXd",
            type="function",
        ),
    ]


@pytest.fixture
def mock_message_single_tool(mock_single_tool):
    mock = Mock(spec=ModelResponse)

    mock_choice = Mock(spec=Choices)
    mock_choice.finish_reason = "tool_calls"
    mock_choice.function_call = None

    mock_choice.message = Mock(spec=Message)
    mock_choice.message.content = None
    mock_choice.message.role = "assistant"
    mock_choice.message.tool_calls = mock_single_tool
    mock_choice.message.to_dict.return_value = {
        "content": None,
        "role": "assistant",
        "tool_calls": [
            {
                "function": {
                    "arguments": '{"location": "San Francisco"}',
                    "name": "main__default__get_current_weather",
                },
                "id": "call_1aW2jpx0R85gFlOp8cGZv3La",
                "type": "function",
            }
        ],
        "function_call": None,
    }

    mock.choices = [mock_choice]
    return mock


@pytest.fixture
def mock_message_multiple_tools(mock_multiple_tools):
    mock = Mock(spec=ModelResponse)

    mock_choice = Mock(spec=Choices)
    mock_choice.finish_reason = "tool_calls"
    mock_choice.function_call = None

    mock_choice.message = Mock(spec=Message)
    mock_choice.message.content = None
    mock_choice.message.role = "assistant"
    mock_choice.message.tool_calls = mock_multiple_tools
    mock_choice.message.to_dict.return_value = {
        "content": None,
        "role": "assistant",
        "tool_calls": [
            {
                "function": {
                    "arguments": '{"location": "San Francisco"}',
                    "name": "main__default__get_current_weather",
                },
                "id": "call_1aW2jpx0R85gFlOp8cGZv3La",
                "type": "function",
            }
        ],
        "function_call": None,
    }

    mock.choices = [mock_choice]
    return mock


@pytest.fixture
def mock_message_multiple_choices_multiple_tools(mock_multiple_tools):
    mock = Mock(spec=ModelResponse)

    mock_choice = Mock(spec=Choices)
    mock_choice.finish_reason = "tool_calls"
    mock_choice.function_call = None

    mock_choice.message = Mock(spec=Message)
    mock_choice.message.content = None
    mock_choice.message.role = "assistant"
    mock_choice.message.tool_calls = mock_multiple_tools

    mock.choices = [mock_choice] * 2
    return mock


@pytest.fixture
def dummy_history():
    return {"role": "user", "content": "What's the weather like in San Fransisco?"}


def test_extract_tool_call_data_single_tool(mock_message_single_tool):
    result = extract_tool_call_data(response=mock_message_single_tool)

    assert len(result) == 1
    tool_call = result[0][0]

    assert isinstance(tool_call, ToolCallData)
    assert tool_call.function_name == "main.default.get_current_weather"
    assert tool_call.arguments == {"location": "San Francisco"}
    assert tool_call.tool_call_id == "call_HSQTsZTvFfLySGY250051VQz"


def test_extract_tool_call_data_multiple_tools(mock_message_multiple_tools):
    result = extract_tool_call_data(response=mock_message_multiple_tools)

    assert len(result) == 1
    assert len(result[0]) == 3
    tool_calls = result[0]

    expected_values = [
        {
            "function_name": "main.default.get_current_weather",
            "arguments": {"location": "San Francisco"},
            "tool_call_id": "call_HSQTsZTvFfLySGY250051VQz",
        },
        {
            "function_name": "main.default.get_current_weather",
            "arguments": {"location": "Tokyo"},
            "tool_call_id": "call_Ozd6L5vXIuKPlsmomMVPsHFM",
        },
        {
            "function_name": "main.default.get_current_weather",
            "arguments": {"location": "Paris"},
            "tool_call_id": "call_MKQQrCiQhKXM6ZWtGTsASIXd",
        },
    ]

    assert len(tool_calls) == len(expected_values)
    for tool_call, expected in zip(tool_calls, expected_values):
        assert isinstance(tool_call, ToolCallData)
        assert tool_call.function_name == expected["function_name"]
        assert tool_call.arguments == expected["arguments"]
        assert tool_call.tool_call_id == expected["tool_call_id"]


def test_extract_tool_call_data_multiple_choices_multiple_tools(
    mock_message_multiple_choices_multiple_tools,
):
    result = extract_tool_call_data(response=mock_message_multiple_choices_multiple_tools)

    assert len(result) == 2
    assert len(result[0]) == 3

    expected_values = [
        {
            "function_name": "main.default.get_current_weather",
            "arguments": {"location": "San Francisco"},
            "tool_call_id": "call_HSQTsZTvFfLySGY250051VQz",
        },
        {
            "function_name": "main.default.get_current_weather",
            "arguments": {"location": "Tokyo"},
            "tool_call_id": "call_Ozd6L5vXIuKPlsmomMVPsHFM",
        },
        {
            "function_name": "main.default.get_current_weather",
            "arguments": {"location": "Paris"},
            "tool_call_id": "call_MKQQrCiQhKXM6ZWtGTsASIXd",
        },
    ]

    for choice in result:
        for tool_call, expected in zip(choice, expected_values):
            assert isinstance(tool_call, ToolCallData)
            assert tool_call.function_name == expected["function_name"]
            assert tool_call.arguments == expected["arguments"]
            assert tool_call.tool_call_id == expected["tool_call_id"]


def test_tool_call_data_execute(mock_client):
    tool_call = ToolCallData(
        function_name="catalog__schema__get_weather",
        arguments={"location": "San Francisco, CA", "unit": "celsius"},
        tool_call_id="toolu_01A09q90qw90lq917835lq9",
    )
    result = tool_call.execute(mock_client)

    mock_client.execute_function.assert_called_once_with(
        "catalog__schema__get_weather",
        {"location": "San Francisco, CA", "unit": "celsius"},
    )
    assert result == "65 degrees"


def test_generate_tool_call_messages_single_tool(
    mock_message_single_tool, mock_client, dummy_history
):
    result = generate_tool_call_messages(
        response=mock_message_single_tool,
        conversation_history=dummy_history,
        client=mock_client,
    )

    assert len(result) == 3
    conversational_history, assistant_message, tool_response_message = result

    assert conversational_history == dummy_history
    assert isinstance(assistant_message, dict)

    assert tool_response_message["role"] == "tool"
    assert tool_response_message["tool_call_id"] == "call_HSQTsZTvFfLySGY250051VQz"
    assert tool_response_message["content"] == "65 degrees"


def test_generate_tool_call_messages_multiple_tools(
    mock_message_multiple_tools, mock_client, dummy_history
):
    result = generate_tool_call_messages(
        response=mock_message_multiple_tools,
        conversation_history=dummy_history,
        client=mock_client,
    )

    assert len(result) == 5
    conversational_history, assistant_message, *tool_response_message = result

    assert conversational_history == dummy_history
    assert isinstance(assistant_message, dict)

    assert len(tool_response_message) == 3

    assert tool_response_message[0]["role"] == "tool"
    assert tool_response_message[0]["tool_call_id"] == "call_HSQTsZTvFfLySGY250051VQz"
    assert tool_response_message[0]["content"] == "65 degrees"

    assert tool_response_message[1]["role"] == "tool"
    assert tool_response_message[1]["tool_call_id"] == "call_Ozd6L5vXIuKPlsmomMVPsHFM"
    assert tool_response_message[1]["content"] == "65 degrees"

    assert tool_response_message[2]["role"] == "tool"
    assert tool_response_message[2]["tool_call_id"] == "call_MKQQrCiQhKXM6ZWtGTsASIXd"
    assert tool_response_message[2]["content"] == "65 degrees"


def test_generate_tool_call_messages_no_tool_use(
    mock_message_single_tool, mock_client, dummy_history
):
    response = copy.copy(mock_message_single_tool)
    response.choices[0].finish_reason = "stop"
    response.choices[0].message.tool_calls = None

    result = generate_tool_call_messages(
        response=response, conversation_history=dummy_history, client=mock_client
    )
    assert len(result) == 2

    history, assistant_message = result

    assert history == dummy_history
    assert assistant_message == {
        "content": None,
        "role": "assistant",
        "tool_calls": [
            {
                "function": {
                    "arguments": '{"location": "San Francisco"}',
                    "name": "main__default__get_current_weather",
                },
                "id": "call_1aW2jpx0R85gFlOp8cGZv3La",
                "type": "function",
            }
        ],
        "function_call": None,
    }


def test_generate_tool_call_messages_validate_default_client(
    mock_message_single_tool, dummy_history, monkeypatch
):
    monkeypatch.setattr(
        "unitycatalog.ai.core.base._is_databricks_client_available",
        lambda: False,
    )
    client = None

    with pytest.raises(ValueError, match="No client provided"):
        generate_tool_call_messages(
            response=mock_message_single_tool,
            conversation_history=dummy_history,
            client=client,
        )
