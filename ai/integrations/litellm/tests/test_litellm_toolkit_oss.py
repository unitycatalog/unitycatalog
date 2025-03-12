import os
from unittest import mock

import pytest
import pytest_asyncio
from pydantic import ValidationError

from unitycatalog.ai.core.base import FunctionExecutionResult
from unitycatalog.ai.core.client import ExecutionMode, UnitycatalogFunctionClient
from unitycatalog.ai.litellm.toolkit import UCFunctionToolkit
from unitycatalog.ai.test_utils.function_utils_oss import (
    CATALOG,
    create_function_and_cleanup_oss,
)
from unitycatalog.client import (
    ApiClient,
    Configuration,
    FunctionInfo,
    FunctionParameterInfo,
    FunctionParameterInfos,
)

SCHEMA = os.environ.get("SCHEMA", "ucai_litellm_test")


@pytest_asyncio.fixture
async def uc_client():
    """Fixture to create and yield an OSS Unity Catalog client."""
    config = Configuration()
    config.host = "http://localhost:8080/api/2.1/unity-catalog"
    uc_api_client = ApiClient(configuration=config)

    uc_client = UnitycatalogFunctionClient(api_client=uc_api_client)
    uc_client.uc.create_catalog(name=CATALOG)
    uc_client.uc.create_schema(name=SCHEMA, catalog_name=CATALOG)

    yield uc_client

    uc_client.close()
    uc_api_client.close()


@pytest.mark.parametrize("execution_mode", ["local", "sandbox"])
@pytest.mark.asyncio
async def test_toolkit_e2e(uc_client, execution_mode):
    uc_client.execution_mode = ExecutionMode(execution_mode)
    with create_function_and_cleanup_oss(uc_client, schema=SCHEMA) as func_obj:
        toolkit = UCFunctionToolkit(function_names=[func_obj.full_function_name], client=uc_client)
        tools = toolkit.tools
        assert len(tools) == 1
        tool = tools[0]
        assert func_obj.comment in tool["description"]

        toolkit = UCFunctionToolkit(
            function_names=[f.full_name for f in uc_client.list_functions(CATALOG, SCHEMA)],
            client=uc_client,
        )
        assert len(toolkit.tools) >= 1
        assert func_obj.tool_name in [t["name"] for t in toolkit.tools]


@pytest.mark.parametrize("execution_mode", ["local", "sandbox"])
@pytest.mark.asyncio
async def test_toolkit_e2e_manually_passing_client(uc_client, execution_mode):
    uc_client.execution_mode = ExecutionMode(execution_mode)
    with create_function_and_cleanup_oss(uc_client, schema=SCHEMA) as func_obj:
        toolkit = UCFunctionToolkit(function_names=[func_obj.full_function_name], client=uc_client)
        tools = toolkit.tools
        assert len(tools) == 1
        tool = tools[0]
        assert tool["name"] == func_obj.tool_name
        assert func_obj.comment in tool["description"]

        toolkit = UCFunctionToolkit(
            function_names=[f"{CATALOG}.{SCHEMA}.*"],
            client=uc_client,
        )
        assert len(toolkit.tools) >= 1
        assert func_obj.tool_name in [t["name"] for t in toolkit.tools]


@pytest.mark.parametrize("execution_mode", ["local", "sandbox"])
@pytest.mark.asyncio
async def test_multiple_toolkits(uc_client, execution_mode):
    uc_client.execution_mode = ExecutionMode(execution_mode)
    with create_function_and_cleanup_oss(uc_client, schema=SCHEMA) as func_obj:
        toolkit1 = UCFunctionToolkit(function_names=[func_obj.full_function_name], client=uc_client)
        toolkit2 = UCFunctionToolkit(function_names=[f"{CATALOG}.{SCHEMA}.*"], client=uc_client)
        toolkits = [toolkit1, toolkit2]

        assert all(isinstance(toolkit.tools[0], dict) for toolkit in toolkits)


def test_toolkit_creation_errors_no_client(monkeypatch):
    monkeypatch.setattr("unitycatalog.ai.core.base._is_databricks_client_available", lambda: False)

    with pytest.raises(
        ValidationError,
        match=r"No client provided, either set the client when creating a toolkit or set the default client",
    ):
        UCFunctionToolkit(function_names=["test.test.test"])


def test_toolkit_creation_errors_invalid_client(uc_client):
    """Test that UCFunctionToolkit raises ValidationError when an invalid client is provided."""
    with pytest.raises(
        ValidationError,
        match=r"Input should be an instance of BaseFunctionClient",
    ):
        UCFunctionToolkit(function_names=[], client="client")


def test_toolkit_creation_errors_missing_function_names(uc_client):
    """Test that UCFunctionToolkit raises ValidationError when function_names are missing."""
    with pytest.raises(
        ValidationError,
        match=r"Cannot create tool instances without function_names being provided.",
    ):
        UCFunctionToolkit(function_names=[], client=uc_client)


def test_toolkit_function_argument_errors(uc_client):
    """Test that UCFunctionToolkit raises ValidationError when function_names are not provided."""
    with pytest.raises(
        ValidationError,
        match=r"Cannot create tool instances without function_names being provided.",
    ):
        UCFunctionToolkit(client=uc_client)


def generate_function_info():
    """Generate a mock FunctionInfo object for testing."""
    parameters = [
        {
            "name": "x",
            "type_text": "string",
            "type_json": '{"name":"x","type":"string","nullable":true,"metadata":{"EXISTS_DEFAULT":"\\"123\\"","default":"\\"123\\"","CURRENT_DEFAULT":"\\"123\\""}}',
            "type_name": "STRING",
            "type_precision": 0,
            "type_scale": 0,
            "position": 17,
            "parameter_type": "PARAM",
            "parameter_default": '"123"',
        }
    ]
    return FunctionInfo(
        catalog_name="catalog",
        schema_name="schema",
        name="test",
        input_params=FunctionParameterInfos(
            parameters=[FunctionParameterInfo(**param) for param in parameters]
        ),
        full_name="catalog.schema.test",
        comment="Executes Python code and returns its stdout.",
    )


@pytest.mark.asyncio
async def test_uc_function_to_litellm_tool(uc_client):
    """Test conversion of a UC function to a LiteLLM tool."""
    mock_function_info = generate_function_info()
    with (
        mock.patch(
            "unitycatalog.ai.core.client.UnitycatalogFunctionClient.get_function",
            return_value=mock_function_info,
        ),
        mock.patch(
            "unitycatalog.ai.core.client.UnitycatalogFunctionClient.execute_function",
            return_value=FunctionExecutionResult(format="SCALAR", value="some_string"),
        ),
    ):
        tool = UCFunctionToolkit.uc_function_to_litellm_tool(
            function_name="catalog.schema.test", client=uc_client
        )
        assert tool["name"] == "catalog__schema__test"


@pytest.mark.asyncio
async def test_toolkit_litellm_kwarg_passthrough(uc_client):
    """Test toolkit with keyword arguments for LitelLLM tool conversion."""
    mock_function_info = generate_function_info()

    with (
        mock.patch(
            "unitycatalog.ai.core.utils.client_utils.validate_or_set_default_client",
            return_value=uc_client,
        ),
        mock.patch.object(uc_client, "get_function", return_value=mock_function_info),
    ):
        tool = UCFunctionToolkit.uc_function_to_litellm_tool(
            function_name="catalog.schema.test",
            client=uc_client,
        )

        assert tool is not None
