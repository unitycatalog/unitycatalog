from unittest.mock import MagicMock

import pytest


@pytest.fixture
def mock_client():
    return MagicMock()


@pytest.fixture
def bedrock_client():
    client = MagicMock()
    # Mock any necessary methods and properties of the client
    client.is_connected.return_value = True
    return client


@pytest.mark.bedrock
def test_bedrock_client_creation(bedrock_client):
    client = bedrock_client
    assert client is not None
    assert client.is_connected()


@pytest.mark.bedrock
def test_bedrock_client_function_execution(bedrock_client):
    client = bedrock_client
    client.execute_function.return_value = MagicMock(value="execution_result")

    result = client.execute_function("test_function", {"param1": "value1"})
    assert result.value == "execution_result"
    client.execute_function.assert_called_once_with("test_function", {"param1": "value1"})