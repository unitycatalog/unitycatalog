import json
import os
from importlib import metadata
from unittest import mock

import pytest
from autogen_core import CancellationToken
from databricks.sdk.service.catalog import (
    ColumnTypeName,
    FunctionInfo,
    FunctionParameterInfo,
    FunctionParameterInfos,
)
from packaging import version
from pydantic import ValidationError

from unitycatalog.ai.autogen.toolkit import UCFunctionToolkit
from unitycatalog.ai.core.client import FunctionExecutionResult
from unitycatalog.ai.core.databricks import ExecutionMode
from unitycatalog.ai.test_utils.client_utils import (
    TEST_IN_DATABRICKS,
    get_client,
    requires_databricks,
    set_default_client,
)
from unitycatalog.ai.test_utils.function_utils import (
    CATALOG,
    RETRIEVER_OUTPUT_CSV,
    RETRIEVER_OUTPUT_SCALAR,
    RETRIEVER_TABLE_FULL_DATA_TYPE,
    RETRIEVER_TABLE_RETURN_PARAMS,
    create_function_and_cleanup,
)

SCHEMA = os.environ.get("SCHEMA", "ucai_autogen_test")

autogen_version = metadata.version("autogen_core")
skip_mlflow_test = version.parse(autogen_version) >= version.parse("0.4.0")


@pytest.fixture
def dbx_client():
    return get_client()


def generate_function_info(
    catalog="catalog",
    schema="schema",
    name="test",
    data_type=None,
    full_data_type=None,
    return_params=None,
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
        input_params=FunctionParameterInfos(
            parameters=[FunctionParameterInfo(**param) for param in parameters]
        ),
        full_name=f"{catalog}.{schema}.{name}",
        comment="Executes Python code and returns its stdout.",
        routine_body="EXTERNAL",
        routine_definition="print('hello')",
        data_type=data_type,
        full_data_type=full_data_type,
        return_params=return_params,
    )


@pytest.mark.parametrize("execution_mode", ["serverless", "local"])
@requires_databricks
@pytest.mark.asyncio
async def test_toolkit_e2e(dbx_client, execution_mode):
    dbx_client.execution_mode = ExecutionMode(execution_mode)
    with (
        set_default_client(dbx_client),
        create_function_and_cleanup(dbx_client, schema=SCHEMA) as func_obj,
    ):
        # 1) Build a toolkit
        toolkit = UCFunctionToolkit(function_names=[func_obj.full_function_name], client=dbx_client)
        tools = toolkit.tools
        assert len(tools) == 1
        tool = tools[0]

        assert func_obj.comment in tool.description

        input_args = {"number": 5}
        result_str = await tool.run_json(input_args, CancellationToken())
        result = json.loads(result_str)["value"]
        assert result == "15"

        toolkit2 = UCFunctionToolkit(
            function_names=[f.full_name for f in dbx_client.list_functions(CATALOG, SCHEMA)],
            client=dbx_client,
        )
        assert len(toolkit2.tools) >= 1
        assert func_obj.tool_name in [t.name for t in toolkit2.tools]


@pytest.mark.parametrize("execution_mode", ["serverless", "local"])
@requires_databricks
@pytest.mark.asyncio
async def test_toolkit_e2e_manually_passing_client(dbx_client, execution_mode):
    dbx_client.execution_mode = ExecutionMode(execution_mode)
    with (
        set_default_client(dbx_client),
        create_function_and_cleanup(dbx_client, schema=SCHEMA) as func_obj,
    ):
        toolkit = UCFunctionToolkit(function_names=[func_obj.full_function_name], client=dbx_client)
        tools = toolkit.tools
        assert len(tools) == 1
        tool = tools[0]
        assert tool.name == func_obj.tool_name
        assert func_obj.comment in tool.description

        input_args = {"number": 2}
        result_str = await tool.run_json(input_args, CancellationToken())
        result = json.loads(result_str)["value"]
        assert result == "12"

        toolkit2 = UCFunctionToolkit(
            function_names=[f.full_name for f in dbx_client.list_functions(CATALOG, SCHEMA)],
            client=dbx_client,
        )
        assert len(toolkit2.tools) >= 1
        assert func_obj.tool_name in [t.name for t in toolkit2.tools]


@pytest.mark.parametrize("execution_mode", ["serverless", "local"])
@requires_databricks
@pytest.mark.asyncio
async def test_multiple_toolkits(dbx_client, execution_mode):
    dbx_client.execution_mode = ExecutionMode(execution_mode)
    with (
        set_default_client(dbx_client),
        create_function_and_cleanup(dbx_client, schema=SCHEMA) as func_obj,
    ):
        toolkit1 = UCFunctionToolkit(
            function_names=[func_obj.full_function_name], client=dbx_client
        )
        toolkit2 = UCFunctionToolkit(
            function_names=[f.full_name for f in dbx_client.list_functions(CATALOG, SCHEMA)],
            client=dbx_client,
        )
        tool1 = toolkit1.tools[0]
        tool2 = [t for t in toolkit2.tools if t.name == func_obj.tool_name][0]

        input_args = {"number": 1}
        result1_str = await tool1.run_json(input_args, CancellationToken())
        result2_str = await tool2.run_json(input_args, CancellationToken())
        result1 = json.loads(result1_str)["value"]
        result2 = json.loads(result2_str)["value"]
        assert result1 == result2


def test_toolkit_creation_errors_bad_client():
    """
    If you pass `client="client"` instead of a real BaseFunctionClient, you get a ValidationError.
    """
    with pytest.raises(ValidationError, match=r"Input should be an instance of BaseFunctionClient"):
        UCFunctionToolkit(function_names=[], client="client")


def test_toolkit_creation_errors_missing_function_names(dbx_client):
    """
    If function_names is empty, raises a ValueError from validation logic.
    """
    with pytest.raises(
        ValueError, match=r"Cannot create tool instances without function_names being provided."
    ):
        UCFunctionToolkit(function_names=[], client=dbx_client)


def test_toolkit_function_argument_errors(dbx_client):
    """
    Test that we raise Pydantic validation if function_names is missing entirely.
    """
    with pytest.raises(
        ValidationError,
        match=r"1 validation error for UCFunctionToolkit\nfunction_names\n  Field required",
    ):
        UCFunctionToolkit(client=dbx_client)


@pytest.mark.asyncio
async def test_uc_function_to_autogen_tool(dbx_client):
    """
    Testing direct usage of `uc_function_to_autogen_tool` with mocking.
    """
    mock_function_info = generate_function_info()
    with (
        mock.patch(
            "unitycatalog.ai.core.databricks.DatabricksFunctionClient.get_function",
            return_value=mock_function_info,
        ),
        mock.patch(
            "unitycatalog.ai.core.databricks.DatabricksFunctionClient.execute_function",
            return_value=FunctionExecutionResult(format="SCALAR", value="some_string"),
        ),
    ):
        tool = UCFunctionToolkit.uc_function_to_autogen_tool(
            function_name=f"{CATALOG}.{SCHEMA}.test", client=dbx_client
        )
        result_str = await tool.run_json({"x": "some_string"}, CancellationToken())
        result = json.loads(result_str)["value"]
        assert result == "some_string"


@pytest.mark.parametrize(
    "filter_accessible_functions",
    [True, False],
)
def test_uc_function_to_autogen_tool_permission_denied(filter_accessible_functions):
    client = get_client()
    # Permission Error should be caught
    with mock.patch(
        "unitycatalog.ai.core.databricks.DatabricksFunctionClient.get_function",
        side_effect=PermissionError("Permission Denied to Underlying Assets"),
    ):
        if filter_accessible_functions:
            tool = UCFunctionToolkit.uc_function_to_autogen_tool(
                client=client,
                function_name=f"{CATALOG}.{SCHEMA}.test",
                filter_accessible_functions=filter_accessible_functions,
            )
            assert tool == None
        else:
            with pytest.raises(PermissionError):
                tool = UCFunctionToolkit.uc_function_to_autogen_tool(
                    client=client,
                    function_name=f"{CATALOG}.{SCHEMA}.test",
                    filter_accessible_functions=filter_accessible_functions,
                )
    # Other errors should not be Caught
    with mock.patch(
        "unitycatalog.ai.core.databricks.DatabricksFunctionClient.get_function",
        side_effect=ValueError("Wrong Get Function Call"),
    ):
        with pytest.raises(ValueError):
            tool = UCFunctionToolkit.uc_function_to_autogen_tool(
                client=client,
                function_name=f"{CATALOG}.{SCHEMA}.test",
                filter_accessible_functions=filter_accessible_functions,
            )


@pytest.mark.skipif(
    skip_mlflow_test, reason="MLflow autologging is not supported for autogen_core 0.4.0 and above."
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
def test_autogen_tool_with_tracing_as_retriever(
    format, function_output, data_type, full_data_type, return_params
):
    client = get_client()
    mock_function_info = generate_function_info(
        name=f"test_{format}",
        data_type=data_type,
        full_data_type=full_data_type,
        return_params=return_params,
    )

    with (
        mock.patch(
            "unitycatalog.ai.core.databricks.DatabricksFunctionClient.get_function",
            return_value=mock_function_info,
        ),
        mock.patch(
            "unitycatalog.ai.core.databricks.DatabricksFunctionClient._execute_uc_function",
            return_value=FunctionExecutionResult(format=format, value=function_output),
        ),
        mock.patch(
            "unitycatalog.ai.core.databricks.DatabricksFunctionClient.validate_input_params"
        ),
    ):
        import mlflow

        if TEST_IN_DATABRICKS:
            import mlflow.tracking._model_registry.utils

            mlflow.tracking._model_registry.utils._get_registry_uri_from_spark_session = (
                lambda: "databricks-uc"
            )

        mlflow.autogen.autolog()

        tool = UCFunctionToolkit.uc_function_to_autogen_tool(
            function_name=mock_function_info.full_name, client=client
        )
        result = tool.fn(x="some input")
        assert json.loads(result)["value"] == function_output

        trace = mlflow.get_last_active_trace()
        assert trace is not None
        assert trace.data.spans[0].name == mock_function_info.full_name
        assert trace.info.execution_time_ms is not None
        assert trace.data.request == '{"x": "some input"}'
        assert trace.data.response == RETRIEVER_OUTPUT_SCALAR

        mlflow.autogen.autolog(disable=True)


@pytest.mark.asyncio
async def test_toolkit_with_invalid_function_input(dbx_client):
    """
    Test toolkit with invalid input parameters for function conversion
    (e.g. 'unexpected_key'). Expects a validation error if `extra='forbid'`.
    """

    mock_function_info = generate_function_info()
    with (
        mock.patch(
            "unitycatalog.ai.core.utils.client_utils.validate_or_set_default_client",
            return_value=dbx_client,
        ),
        mock.patch.object(dbx_client, "get_function", return_value=mock_function_info),
    ):
        tool = UCFunctionToolkit.uc_function_to_autogen_tool(
            function_name="catalog.schema.test", client=dbx_client
        )
        invalid_inputs = {"unexpected_key": "value"}

        with pytest.raises(ValueError, match="Extra inputs are not permitted"):
            await tool.run_json(invalid_inputs, CancellationToken())
