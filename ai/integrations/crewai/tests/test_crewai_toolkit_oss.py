import json
import os
from unittest import mock

import pytest
import pytest_asyncio
from pydantic import ValidationError

from unitycatalog.ai.core.base import FunctionExecutionResult
from unitycatalog.ai.core.client import ExecutionMode, UnitycatalogFunctionClient
from unitycatalog.ai.crewai.toolkit import UCFunctionToolkit
from unitycatalog.ai.test_utils.function_utils import (
    RETRIEVER_OUTPUT_CSV,
    RETRIEVER_OUTPUT_SCALAR,
    RETRIEVER_TABLE_FULL_DATA_TYPE,
)
from unitycatalog.ai.test_utils.function_utils_oss import (
    CATALOG,
    RETRIEVER_TABLE_RETURN_PARAMS_OSS,
    create_function_and_cleanup_oss,
)
from unitycatalog.client import (
    ApiClient,
    ColumnTypeName,
    Configuration,
    FunctionInfo,
    FunctionParameterInfo,
    FunctionParameterInfos,
)

SCHEMA = os.environ.get("SCHEMA", "ucai_crewai_test")


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
    await uc_api_client.close()


@pytest.mark.parametrize("execution_mode", ["local", "sandbox"])
@pytest.mark.asyncio
async def test_toolkit_e2e(uc_client, execution_mode):
    uc_client.execution_mode = ExecutionMode(execution_mode)
    with create_function_and_cleanup_oss(uc_client, schema=SCHEMA) as func_obj:
        toolkit = UCFunctionToolkit(function_names=[func_obj.full_function_name], client=uc_client)
        tools = toolkit.tools
        assert len(tools) == 1
        tool = tools[0]
        assert func_obj.full_function_name.replace(".", "__") in tool.description
        assert func_obj.comment in tool.description
        assert tool.client_config == uc_client.to_dict()

        input_args = {"a": 2, "b": 3}
        result = json.loads(tool.fn(**input_args))["value"]
        assert result == "5"

        toolkit = UCFunctionToolkit(
            function_names=[f.full_name for f in uc_client.list_functions(CATALOG, SCHEMA)],
            client=uc_client,
        )
        assert len(toolkit.tools) >= 1
        assert func_obj.tool_name in [t.name for t in toolkit.tools]


@pytest.mark.parametrize("execution_mode", ["local", "sandbox"])
@pytest.mark.asyncio
async def test_toolkit_e2e_manually_passing_client(uc_client, execution_mode):
    uc_client.execution_mode = ExecutionMode(execution_mode)
    with create_function_and_cleanup_oss(uc_client, schema=SCHEMA) as func_obj:
        toolkit = UCFunctionToolkit(function_names=[func_obj.full_function_name], client=uc_client)
        tools = toolkit.tools
        assert len(tools) == 1
        tool = tools[0]
        assert tool.name == func_obj.tool_name
        assert func_obj.full_function_name.replace(".", "__") in tool.description
        assert func_obj.comment in tool.description
        assert tool.client_config == uc_client.to_dict()
        input_args = {"a": 4, "b": 8}
        result = json.loads(tool.fn(**input_args))["value"]
        assert result == "12"

        toolkit = UCFunctionToolkit(
            function_names=[f"{CATALOG}.{SCHEMA}.*"],
            client=uc_client,
        )
        assert len(toolkit.tools) >= 1
        assert func_obj.tool_name in [t.name for t in toolkit.tools]


@pytest.mark.parametrize("execution_mode", ["local", "sandbox"])
@pytest.mark.asyncio
async def test_multiple_toolkits(uc_client, execution_mode):
    uc_client.execution_mode = ExecutionMode(execution_mode)
    with create_function_and_cleanup_oss(uc_client, schema=SCHEMA) as func_obj:
        toolkit1 = UCFunctionToolkit(function_names=[func_obj.full_function_name], client=uc_client)
        toolkit2 = UCFunctionToolkit(function_names=[f"{CATALOG}.{SCHEMA}.*"], client=uc_client)
        tool1 = toolkit1.tools[0]
        tool2 = [t for t in toolkit2.tools if t.name == func_obj.tool_name][0]
        input_args = {"a": 4, "b": 8}
        result1 = json.loads(tool1.fn(**input_args))["value"]
        result2 = json.loads(tool2.fn(**input_args))["value"]
        assert result1 == result2


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


def generate_function_info(
    catalog="catalog",
    schema="schema",
    name="test",
    data_type=None,
    full_data_type=None,
    return_params=None,
):
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
        catalog_name=catalog,
        schema_name=schema,
        name=name,
        input_params=FunctionParameterInfos(
            parameters=[FunctionParameterInfo(**param) for param in parameters]
        ),
        full_name=f"{catalog}.{schema}.{name}",
        comment="Executes Python code and returns its stdout.",
        data_type=data_type,
        full_data_type=full_data_type,
        return_params=return_params,
    )


@pytest.mark.asyncio
async def test_uc_function_to_crewai_tool(uc_client):
    """Test conversion of a UC function to a CrewaAI tool."""
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
        tool = UCFunctionToolkit.uc_function_to_crewai_tool(
            function_name="catalog.schema.test", client=uc_client
        )
        result = json.loads(tool.fn(x="some_string"))["value"]
        assert result == "some_string"

        # Check defaults for parameters not defined by UC
        assert tool.cache_function(1, {1: 1})
        assert not tool.result_as_answer
        assert not tool.description_updated


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
        (
            ColumnTypeName.TABLE_TYPE,
            RETRIEVER_TABLE_FULL_DATA_TYPE,
            RETRIEVER_TABLE_RETURN_PARAMS_OSS,
        ),
    ],
)
@pytest.mark.asyncio
async def test_crewai_tool_with_tracing_as_retriever(
    uc_client, format, function_output, data_type, full_data_type, return_params
):
    mock_function_info = generate_function_info(
        name=f"test_{format}",
        data_type=data_type,
        full_data_type=full_data_type,
        return_params=return_params,
    )

    with (
        mock.patch(
            "unitycatalog.ai.core.client.UnitycatalogFunctionClient.get_function",
            return_value=mock_function_info,
        ),
        mock.patch(
            "unitycatalog.ai.core.client.UnitycatalogFunctionClient._execute_uc_function",
            return_value=FunctionExecutionResult(format=format, value=function_output),
        ),
        mock.patch("unitycatalog.ai.core.client.UnitycatalogFunctionClient.validate_input_params"),
    ):
        import mlflow

        mlflow.crewai.autolog()

        tool = UCFunctionToolkit.uc_function_to_crewai_tool(
            function_name=mock_function_info.full_name, client=uc_client
        )
        result = tool.fn(x="some input")
        assert json.loads(result)["value"] == function_output

        trace = mlflow.get_last_active_trace()
        assert trace is not None
        assert trace.data.spans[0].name == mock_function_info.full_name
        assert trace.info.execution_time_ms is not None
        assert trace.data.request == '{"x": "some input"}'
        assert trace.data.response == RETRIEVER_OUTPUT_SCALAR

        mlflow.crewai.autolog(disable=True)


@pytest.mark.asyncio
async def test_toolkit_with_invalid_function_input(uc_client):
    """Test toolkit with invalid input parameters for function conversion."""
    mock_function_info = generate_function_info()

    with (
        mock.patch(
            "unitycatalog.ai.core.utils.client_utils.validate_or_set_default_client",
            return_value=uc_client,
        ),
        mock.patch.object(uc_client, "get_function", return_value=mock_function_info),
    ):
        # Test with invalid input params that are not matching expected schema
        invalid_inputs = {"unexpected_key": "value"}
        tool = UCFunctionToolkit.uc_function_to_crewai_tool(
            function_name="catalog.schema.test", client=uc_client
        )

        with pytest.raises(ValueError, match="Extra parameters provided that are not defined"):
            tool.fn(**invalid_inputs)


@pytest.mark.asyncio
async def test_toolkit_crewai_kwarg_passthrough(uc_client):
    """Test toolkit with keyword arguments for CrewaAI tool conversion."""
    mock_function_info = generate_function_info()

    with (
        mock.patch(
            "unitycatalog.ai.core.utils.client_utils.validate_or_set_default_client",
            return_value=uc_client,
        ),
        mock.patch.object(uc_client, "get_function", return_value=mock_function_info),
    ):
        tool = UCFunctionToolkit.uc_function_to_crewai_tool(
            function_name="catalog.schema.test",
            client=uc_client,
            cache_function=lambda: True,
            result_as_answer=True,
            description_updated=True,
        )

        assert tool.cache_function()
        assert tool.result_as_answer
        assert tool.description_updated
