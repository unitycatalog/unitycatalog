import unittest
from unittest.mock import MagicMock, patch
import pytest
from databricks.sdk.service.catalog import FunctionInfo  # Import the correct class
from unitycatalog.ai.core.client import UnitycatalogFunctionClient
from unitycatalog.ai.bedrock.toolkit import (
    BedrockToolResponse,
    BedrockSession,
    UCFunctionToolkit
)


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

    def test_bedrock_session_invoke_agent(self, mock_boto_client):
        session = BedrockSession(
            agent_id="test_agent_id",
            agent_alias_id="test_agent_alias_id",
            catalog_name="test_catalog",
            schema_name="test_schema"
        )
        response = session.invoke_agent("test_input")

        assert response.raw_response["completion"][0]["chunk"]["bytes"] == b"response_chunk"

    def test_ucfunctiontoolkit_initialization(self, toolkit, mock_client):
        """Validate that UCFunctionToolkit initializes correctly"""
        assert toolkit.client == mock_client
        assert len(toolkit.function_names) == 1
        assert toolkit.function_names[0] == "test_catalog.test_schema.test_function"

    def test_ucfunctiontoolkit_invalid_function_name(self, mock_client):
        """Test invalid function name formatting"""
        with pytest.raises(ValueError, match="Invalid function name: .* expecting format <catalog_name>.<schema_name>.<function_name>"):
            UCFunctionToolkit(
                function_names=["invalid_function_name"],  # Missing catalog & schema
                client=mock_client
            )


    def test_ucfunctiontoolkit_calls_get_function(self, toolkit, mock_client):
        """Ensure `get_function` is called on the Unity Catalog client"""
        toolkit.validate_toolkit()
        mock_client.get_function.assert_called_once_with("test_catalog.test_schema.test_function")
