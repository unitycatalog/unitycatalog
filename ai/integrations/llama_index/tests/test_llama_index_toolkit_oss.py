import json
import os
from typing import Any, Dict
from unittest import mock

import pytest
import pytest_asyncio
from databricks.sdk.service.catalog import ColumnTypeName
from pydantic import BaseModel, ValidationError

from unitycatalog.ai.core.base import (
    BaseFunctionClient,
    FunctionExecutionResult,
)
from unitycatalog.ai.core.client import (
    ExecutionMode,
    UnitycatalogFunctionClient,
)
from unitycatalog.ai.llama_index.toolkit import UCFunctionToolkit
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
    Configuration,
    FunctionInfo,
    FunctionParameterInfo,
    FunctionParameterInfos,
)

SCHEMA = os.environ.get("SCHEMA", "ucai_llama_index_test")


@pytest.fixture(autouse=True)
def env_setup(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "fake-key")


@pytest_asyncio.fixture
async def uc_client():
    config = Configuration()
    config.host = "http://localhost:8080/api/2.1/unity-catalog"
    uc_api_client = ApiClient(configuration=config)

    uc_client = UnitycatalogFunctionClient(api_client=uc_api_client)
    uc_client.uc.create_catalog(name=CATALOG)
    uc_client.uc.create_schema(name=SCHEMA, catalog_name=CATALOG)

    yield uc_client

    await uc_client.close_async()
    await uc_api_client.close()


class MockFnSchemaWithProperties(BaseModel):
    x: str
    properties: Dict[str, Any]


class MockFnSchemaWithoutProperties(BaseModel):
    x: str


class MockFunctionExecutionResult:
    def __init__(self, return_value: str):
        self.return_value = return_value

    def to_json(self) -> str:
        return self.return_value


def generate_mock_function_info(has_properties: bool = False) -> FunctionInfo:
    parameters = [
        FunctionParameterInfo(
            name="x",
            type_text="string",
            type_json='{"name":"x","type":"STRING","nullable":true}',
            type_name="STRING",
            type_precision=0,
            type_scale=0,
            position=1,
            parameter_type="PARAM",
            parameter_default='"default_x"',
        )
    ]

    if has_properties:
        parameters.append(
            FunctionParameterInfo(
                name="properties",
                type_text="map<string, string>",
                type_json='{"name":"properties","type":"MAP","nullable":true}',
                type_name="MAP",
                type_precision=0,
                type_scale=0,
                position=2,
                parameter_type="PARAM",
                parameter_default=None,
            )
        )

    return FunctionInfo(
        catalog_name="catalog",
        schema_name="schema",
        name="test_function",
        input_params=FunctionParameterInfos(parameters=parameters),
        comment="A test function with properties argument"
        if has_properties
        else "A test function without properties argument",
    )


def generate_mock_execution_result(return_value: str = "result") -> FunctionExecutionResult:
    return FunctionExecutionResult(format="SCALAR", value=return_value)


def test_toolkit_creation_errors_no_client(monkeypatch):
    monkeypatch.setattr("unitycatalog.ai.core.base._is_databricks_client_available", lambda: False)

    with pytest.raises(
        ValidationError,
        match=r"No client provided, either set the client when creating a toolkit or set the default client",
    ):
        UCFunctionToolkit(function_names=["test.test.test"])


@pytest.mark.asyncio
async def test_toolkit_creation_with_properties_argument_mocked():
    """
    Test that UCFunctionToolkit raises a ValueError when the function has a 'properties' argument using mocks.
    """
    mock_function_info = generate_mock_function_info(has_properties=True)

    mock_client = mock.create_autospec(BaseFunctionClient, instance=True)
    mock_client.get_function.return_value = mock_function_info
    mock_client.to_dict.return_value = {"mock": "config"}

    mock_fn_schema = mock.Mock()
    mock_fn_schema.pydantic_model = MockFnSchemaWithProperties

    with (
        mock.patch(
            "unitycatalog.ai.core.utils.function_processing_utils.generate_function_input_params_schema",
            return_value=mock_fn_schema,
        ),
        mock.patch(
            "unitycatalog.ai.llama_index.toolkit.validate_or_set_default_client",
            return_value=mock_client,
        ),
    ):
        with pytest.raises(ValueError, match="has a 'properties' key in its input schema"):
            UCFunctionToolkit(function_names=["catalog.schema.test_function"], client=mock_client)


@pytest.mark.parametrize("execution_mode", ["local", "sandbox"])
@pytest.mark.asyncio
async def test_toolkit_e2e(uc_client, execution_mode):
    uc_client.execution_mode = ExecutionMode(execution_mode)
    with create_function_and_cleanup_oss(uc_client, schema=SCHEMA) as func_obj:
        toolkit = UCFunctionToolkit(
            function_names=[func_obj.full_function_name], client=uc_client, return_direct=True
        )
        tools = toolkit.tools
        assert len(tools) == 1
        tool = tools[0]
        assert tool.metadata.name == func_obj.tool_name
        assert tool.metadata.return_direct
        assert tool.metadata.description == func_obj.comment
        assert tool.uc_function_name == func_obj.full_function_name
        assert tool.client_config == uc_client.to_dict()

        input_args = {"a": 1, "b": 2}
        result = json.loads(tool.fn(**input_args))["value"]
        assert result == "3"

        toolkit = UCFunctionToolkit(function_names=[f"{CATALOG}.{SCHEMA}.*"], client=uc_client)
        assert len(toolkit.tools) >= 1
        assert func_obj.tool_name in [t.metadata.name for t in toolkit.tools]


@pytest.mark.parametrize("execution_mode", ["local", "sandbox"])
@pytest.mark.asyncio
async def test_toolkit_e2e_manually_passing_client(uc_client, execution_mode):
    uc_client.execution_mode = ExecutionMode(execution_mode)
    with create_function_and_cleanup_oss(uc_client, schema=SCHEMA) as func_obj:
        toolkit = UCFunctionToolkit(
            function_names=[func_obj.full_function_name], client=uc_client, return_direct=True
        )
        tools = toolkit.tools
        assert len(tools) == 1
        tool = tools[0]
        assert tool.metadata.name == func_obj.tool_name
        assert tool.metadata.return_direct
        assert tool.metadata.description == func_obj.comment
        assert tool.uc_function_name == func_obj.full_function_name
        assert tool.client_config == uc_client.to_dict()
        input_args = {"a": 1, "b": 2}
        result = json.loads(tool.fn(**input_args))["value"]
        assert result == "3"

        toolkit = UCFunctionToolkit(function_names=[f"{CATALOG}.{SCHEMA}.*"], client=uc_client)
        assert len(toolkit.tools) >= 1
        assert func_obj.tool_name in [t.metadata.name for t in toolkit.tools]


@pytest.mark.parametrize("execution_mode", ["local", "sandbox"])
@pytest.mark.asyncio
async def test_multiple_toolkits(uc_client, execution_mode):
    uc_client.execution_mode = ExecutionMode(execution_mode)
    with create_function_and_cleanup_oss(uc_client, schema=SCHEMA) as func_obj:
        toolkit1 = UCFunctionToolkit(function_names=[func_obj.full_function_name], client=uc_client)
        toolkit2 = UCFunctionToolkit(function_names=[f"{CATALOG}.{SCHEMA}.*"], client=uc_client)
        tool1 = toolkit1.tools[0]
        tool2 = [t for t in toolkit2.tools if t.metadata.name == func_obj.tool_name][0]
        input_args = {"a": 2, "b": 3}
        result1 = json.loads(tool1.fn(**input_args))["value"]
        result2 = json.loads(tool2.fn(**input_args))["value"]
        assert result1 == result2


def generate_function_info(
    catalog: str = "catalog",
    schema: str = "schema",
    name: str = "test_function",
    data_type: ColumnTypeName = None,
    full_data_type: str = None,
    return_params: FunctionParameterInfos = None,
):
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
        full_name=f"{catalog}.{schema}.{name}",
        data_type=data_type,
        full_data_type=full_data_type,
        return_params=return_params,
        input_params=FunctionParameterInfos(
            parameters=[FunctionParameterInfo(**param) for param in parameters]
        ),
    )


def test_uc_function_to_llama_tool(uc_client):
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
        tool = UCFunctionToolkit.uc_function_to_llama_tool(
            function_name=f"{CATALOG}.{SCHEMA}.test", client=uc_client, return_direct=True
        )
        # Validate passthrough of LlamaIndex argument
        assert tool.metadata.return_direct

        # Validate tool repr
        assert str(tool).startswith(
            f"UnityCatalogTool(description='', name='{CATALOG}__{SCHEMA}__test',"
        )

        result = json.loads(tool.fn(x="some_string"))["value"]
        assert result == "some_string"


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
def test_toolkit_with_tracing_as_retriever(
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

        mlflow.llama_index.autolog()

        tool = UCFunctionToolkit.uc_function_to_llama_tool(
            function_name=mock_function_info.full_name, client=uc_client, return_direct=True
        )
        result = tool.fn(x="some input")
        assert json.loads(result)["value"] == function_output

        import mlflow

        trace = mlflow.get_last_active_trace()
        assert trace is not None
        assert trace.data.spans[0].name == mock_function_info.full_name
        assert trace.info.execution_time_ms is not None
        assert trace.data.request == '{"x": "some input"}'
        assert trace.data.response == RETRIEVER_OUTPUT_SCALAR

        mlflow.llama_index.autolog(disable=True)
