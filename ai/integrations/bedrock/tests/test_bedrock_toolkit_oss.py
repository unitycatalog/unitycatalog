from unittest.mock import MagicMock

import pytest

from unitycatalog.ai.bedrock.toolkit import BedrockTool, BedrockToolResponse
from unitycatalog.ai.core.utils.function_processing_utils import get_tool_name
from unitycatalog.client import FunctionInfo


@pytest.fixture
def raw_response_with_chunks():
    """Fixture to provide a raw response with chunks."""
    return {
        "completion": [
            {"chunk": {"bytes": b"Hello"}},
            {"chunk": {"bytes": b" World"}},
        ]
    }


@pytest.fixture
def raw_response_with_tool_calls():
    """Fixture to provide a raw response with tool calls."""
    return {
        "completion": [
            {"returnControl": True},
            {"toolCall": {"name": "example_tool"}},
        ]
    }


@pytest.fixture
def empty_raw_response():
    """Fixture to provide an empty raw response."""
    return {"completion": []}


@pytest.fixture
def mock_function_info():
    """Fixture to provide a mocked function info."""
    return FunctionInfo(
        name="test_catalog.test_schema.test_function",
        full_name="test_catalog.test_schema.test_function",
        catalog_name="test_catalog",
        schema_name="test_schema",
        comment="This is a test function",
    )


@pytest.fixture
def mock_fn_schema():
    """Fixture to provide a mocked function schema."""
    mock_schema = MagicMock()
    mock_schema.pydantic_model.model_json_schema.return_value = {
        "properties": {"param1": {"type": "string"}},
        "required": ["param1"],
    }
    return mock_schema


def test_bedrock_tool_response_final_response(raw_response_with_chunks):
    """Test the final_response property of BedrockToolResponse."""
    response = BedrockToolResponse(raw_response=raw_response_with_chunks)
    assert response.final_response == "Hello"


def test_bedrock_tool_response_is_streaming(raw_response_with_chunks):
    """Test the is_streaming property of BedrockToolResponse."""
    response = BedrockToolResponse(raw_response=raw_response_with_chunks)
    assert response.is_streaming is True


def test_bedrock_tool_response_requires_tool_execution(raw_response_with_tool_calls):
    """Test the requires_tool_execution property of BedrockToolResponse."""
    response = BedrockToolResponse(raw_response=raw_response_with_tool_calls)
    assert response.requires_tool_execution is True


def test_bedrock_tool_response_empty_response(empty_raw_response):
    """Test BedrockToolResponse with an empty raw response."""
    response = BedrockToolResponse(raw_response=empty_raw_response)
    assert response.final_response is None
    assert response.is_streaming is False
    assert response.requires_tool_execution is False


def test_bedrock_tool_creation(mock_function_info, mock_fn_schema):
    """Test the creation of a BedrockTool."""

    tool = BedrockTool(
        name=get_tool_name(mock_function_info.name),
        description=mock_function_info.comment,
        parameters={
            "properties": mock_fn_schema.pydantic_model.model_json_schema().get("properties", {}),
            "required": mock_fn_schema.pydantic_model.model_json_schema().get("required", []),
        },
    )

    assert tool.name == "test_catalog__test_schema__test_function"
    assert tool.description == "This is a test function"
    assert tool.parameters["properties"] == {"param1": {"type": "string"}}
    assert tool.parameters["required"] == ["param1"]


def test_bedrock_tool_response_streaming_chunks(raw_response_with_chunks):
    """Test the get_stream method of BedrockToolResponse."""
    response = BedrockToolResponse(raw_response=raw_response_with_chunks)
    chunks = list(response.get_stream())
    assert chunks == ["Hello", " World"]


def test_bedrock_tool_response_invalid_chunk_structure():
    """Test BedrockToolResponse with invalid chunk structure."""
    raw_response = {
        "completion": [
            {"chunk": {"bytes": b"Hello"}},
            {"chunk": {}},  # Missing "bytes" key
        ]
    }
    response = BedrockToolResponse(raw_response=raw_response)
    chunks = list(response.get_stream())
    assert chunks == ["Hello"]


def test_bedrock_tool_response_no_chunks(empty_raw_response):
    """Test BedrockToolResponse when no chunks are present."""
    response = BedrockToolResponse(raw_response=empty_raw_response)
    chunks = list(response.get_stream())
    assert chunks == []


def test_bedrock_tool_response_tool_calls(raw_response_with_tool_calls):
    """Test BedrockToolResponse with tool calls."""
    response = BedrockToolResponse(raw_response=raw_response_with_tool_calls)
    assert response.requires_tool_execution is True
