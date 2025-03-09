from unittest.mock import MagicMock, patch

import pytest
from databricks.sdk.service.catalog import FunctionInfo  # Import the correct class

from unitycatalog.ai.bedrock.toolkit import BedrockToolResponse, UCFunctionToolkit
from unitycatalog.ai.core.client import UnitycatalogFunctionClient


@pytest.fixture
def mock_client():
    """Fixture for setting up a mocked UnityCatalogFunctionClient"""
    client = MagicMock(spec=UnitycatalogFunctionClient)
    function_name = "test_catalog.test_schema.test_function"

    # ✅ Mock function info with only valid parameters
    mock_function_info = FunctionInfo(
        name=function_name,
        full_name=function_name,
        catalog_name="test_catalog",
        schema_name="test_schema",
        comment="Mock function for testing"  # Only include valid params
    )

    client.get_function.return_value = mock_function_info
    return client


@pytest.fixture
def mock_boto_client():
    """Fixture for setting up a mocked boto3 client"""
    with patch("unitycatalog.ai.bedrock.toolkit.boto3.client") as mock:
        mock.return_value.invoke_agent.return_value = {
            "completion": [{"chunk": {"bytes": b"response_chunk"}}]
        }
        yield mock


@pytest.fixture
def toolkit(mock_client):
    """Fixture for setting up the UCFunctionToolkit with a mock client"""
    return UCFunctionToolkit(
        function_names=["test_catalog.test_schema.test_function"],
        client=mock_client
    )


class TestToolkit:
    def test_bedrock_tool_response_requires_tool_execution(self):
        response = {"completion": [{"returnControl": {}}]}
        tool_response = BedrockToolResponse(raw_response=response)
        assert tool_response.requires_tool_execution is True

    def test_bedrock_tool_response_final_response(self):
        response = {"completion": [{"chunk": {"bytes": b"final_response"}}]}
        tool_response = BedrockToolResponse(raw_response=response)
        assert tool_response.final_response == b"final_response".decode("utf-8")

    def test_bedrock_tool_response_is_streaming(self):
        response = {"completion": [{"chunk": {"bytes": b"streaming_response"}}]}
        tool_response = BedrockToolResponse(raw_response=response)
        assert tool_response.is_streaming is True

    def test_bedrock_tool_response_get_stream(self):
        response = {
            "completion": [
                {"chunk": {"bytes": b"chunk1"}},
                {"chunk": {"bytes": b"chunk2"}}
            ]
        }
        tool_response = BedrockToolResponse(raw_response=response)
        stream = list(tool_response.get_stream())
        assert stream == [b"chunk1".decode("utf-8"), b"chunk2".decode("utf-8")]
