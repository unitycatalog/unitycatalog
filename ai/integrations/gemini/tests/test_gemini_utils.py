from unittest.mock import MagicMock, patch

import pytest
from google.generativeai import GenerativeModel, protos
from google.generativeai.types import GenerateContentResponse, content_types
from google.generativeai.types.content_types import to_content, to_part

from unitycatalog.ai.gemini.utils import generate_tool_call_messages, get_function_calls


@pytest.fixture
def example_function_response_with_mutliple_parts():
    response = MagicMock(spec=GenerateContentResponse)
    candidate = MagicMock(spec=protos.Candidate)
    dict_1 = {"name": "get_temperature", "args": {"location": "New York"}}

    dict_2 = {"name": "convert_to_celsius", "args": {"celsius": "New 36.5"}}

    content = to_content(
        [to_part(protos.FunctionCall(dict_1)), to_part(protos.FunctionCall(dict_2))]
    )
    content.role = "model"
    candidate.content = content

    response.candidates = [candidate]
    return response


@pytest.fixture
def example_function_response_with_single_part():
    response = MagicMock(spec=GenerateContentResponse)
    candidate = MagicMock(spec=protos.Candidate)
    dict_1 = {"name": "get_temperature", "args": {"location": "Tokyo"}}

    content = to_content([to_part(protos.FunctionCall(dict_1))])
    content.role = "model"
    candidate.content = content

    response.candidates = [candidate]
    return response


@pytest.fixture
def example_string_response():
    response = MagicMock(spec=GenerateContentResponse)
    candidate = MagicMock(spec=protos.Candidate)

    content = content_types.to_content("This is a dummy output Response")
    content.role = "model"
    candidate.content = content

    response.candidates = [candidate]
    return response


@pytest.fixture
def example_conversation_history():
    content = content_types.to_content("What is the temperature in Tokyo?")
    content.role = "user"
    return [content]


@pytest.fixture
def mock_model():
    """Fixture to create a mock model object."""
    model = MagicMock(spec=GenerativeModel)
    model._tools = None
    return model


def test_get_function_calls_with_function_calls(example_function_response_with_mutliple_parts):
    result = get_function_calls(example_function_response_with_mutliple_parts)
    assert len(result) == 2
    assert "get_temperature" in result[0].name
    assert "convert_to_celsius" in result[1].name


def test_get_function_calls_with_no_function_calls(example_string_response):
    result = get_function_calls(example_string_response)
    assert len(result) == 0


def test_generate_tool_call_messages_no_tools(mock_model):
    # If the model has no tools, it should return conversation_history unchanged and None
    mock_response = MagicMock()
    mock_response.candidates = [MagicMock()]
    mock_response.candidates[0].content.parts = []
    empty_conversation = []

    updated_history, function_response_parts = generate_tool_call_messages(
        model=mock_model,
        response=mock_response,
        conversation_history=empty_conversation,
    )
    assert updated_history == empty_conversation
    assert function_response_parts is None


@patch("google.generativeai.types.content_types.to_function_library")
def test_generate_tool_call_messages_with_function_calls(
    mock_model, example_conversation_history, example_function_response_with_single_part
):
    from unitycatalog.ai.gemini.utils import generate_tool_call_messages

    # Create a mock tool library function that returns a predefined Part
    def mock_tool_lib(fc):
        response_dict = {
            "name": "get_temperature",
            "response": {"result": '{"format": "SCALAR", "value": "31.9 C", "truncated": false}'},
        }
        return to_part(protos.FunctionResponse(response_dict))

        # Patch inside the test using a context manager

    with patch(
        "google.generativeai.types.content_types.to_function_library", return_value=mock_tool_lib
    ):
        # Mock model and response to test generate_tool_call_messages
        mock_model._tools = object()  # pretend tools are present

        assert len(example_conversation_history) == 1

        updated_history, function_response_parts = generate_tool_call_messages(
            model=mock_model,
            response=example_function_response_with_single_part,
            conversation_history=example_conversation_history,
        )

        assert function_response_parts is not None
        assert len(updated_history) == len(example_conversation_history) + 2
        assert updated_history[-1].parts == function_response_parts
        assert function_response_parts[0].function_response.name == "get_temperature"
        p = function_response_parts[0].function_response
        function_response_dict = type(p).to_dict(p)
        assert (
            function_response_dict["response"]["result"]
            == '{"format": "SCALAR", "value": "31.9 C", "truncated": false}'
        )


def test_generate_tool_call_messages_no_function_calls_in_response(
    mock_model, example_conversation_history, example_string_response
):
    from unitycatalog.ai.gemini.utils import generate_tool_call_messages

    # Create a mock tool library function that returns a predefined Part
    def mock_tool_lib(fc):
        response_dict = {
            "name": "get_temperature",
            "response": {"result": '{"format": "SCALAR", "value": "31.9 C", "truncated": false}'},
        }
        return to_part(protos.FunctionResponse(response_dict))

        # Patch inside the test using a context manager

    with patch(
        "google.generativeai.types.content_types.to_function_library", return_value=mock_tool_lib
    ):
        # Mock model and response to test generate_tool_call_messages
        mock_model._tools = object()  # pretend tools are present

        assert len(example_conversation_history) == 1

        updated_history, function_response_parts = generate_tool_call_messages(
            model=mock_model,
            response=example_string_response,
            conversation_history=example_conversation_history,
        )

        # Since no function calls, just the candidate content is appended
        assert function_response_parts is None
        assert len(updated_history) == len(example_conversation_history) + 1
