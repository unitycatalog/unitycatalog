import base64
import json
import os
from unittest import mock

import pytest
from databricks.sdk.service.catalog import (
    ColumnTypeName,
    FunctionInfo,
    FunctionParameterInfo,
    FunctionParameterInfos,
)
from pydantic import ValidationError

from src.unitycatalog.ai.crewai.toolkit import UCFunctionToolkit
from unitycatalog.ai.core.base import (
    BaseFunctionClient,
    FunctionExecutionResult,
)
from unitycatalog.ai.core.utils.execution_utils import ExecutionModeDatabricks
from unitycatalog.ai.test_utils.client_utils import (
    TEST_IN_DATABRICKS,
    client,  # noqa: F401
    get_client,
    requires_databricks,
    set_default_client,
)
from unitycatalog.ai.test_utils.function_utils import (
    BINARY_ARGUMENTS,
    CATALOG,
    JSON_ARGUMENT_PARAMS,
    JSON_ARGUMENTS,
    NATIVE_ARGUMENTS,
    RETRIEVER_OUTPUT_CSV,
    RETRIEVER_OUTPUT_SCALAR,
    RETRIEVER_TABLE_FULL_DATA_TYPE,
    RETRIEVER_TABLE_RETURN_PARAMS,
    create_function_and_cleanup,
    generate_function_info_with_params,
    mock_schema_with_inaccessible_function,
)
from unitycatalog.ai.test_utils.function_utils_oss import generate_function_info_with_params_oss

SCHEMA = os.environ.get("SCHEMA", "ucai_crewai_test")


@pytest.mark.parametrize("execution_mode", ["serverless", "local"])
@requires_databricks
def test_toolkit_e2e(execution_mode):
    client = get_client()
    client.execution_mode = ExecutionModeDatabricks(execution_mode)
    with set_default_client(client), create_function_and_cleanup(client, schema=SCHEMA) as func_obj:
        toolkit = UCFunctionToolkit(function_names=[func_obj.full_function_name])
        tools = toolkit.tools
        assert len(tools) == 1
        tool = tools[0]
        assert func_obj.full_function_name.replace(".", "__") in tool.description
        assert func_obj.comment in tool.description
        assert tool.client_config == client.to_dict()

        input_args = {"number": 1}
        raw_result = tool.fn(**input_args)
        result = json.loads(raw_result)["value"]
        assert result == "11"

        toolkit = UCFunctionToolkit(function_names=[f"{CATALOG}.{SCHEMA}.*"])
        assert len(toolkit.tools) >= 1
        assert func_obj.tool_name in [t.name for t in toolkit.tools]


@pytest.mark.parametrize("execution_mode", ["serverless", "local"])
@requires_databricks
def test_toolkit_e2e_manually_passing_client(execution_mode):
    client = get_client()
    client.execution_mode = ExecutionModeDatabricks(execution_mode)
    with set_default_client(client), create_function_and_cleanup(client, schema=SCHEMA) as func_obj:
        toolkit = UCFunctionToolkit(function_names=[func_obj.full_function_name], client=client)
        tools = toolkit.tools
        assert len(tools) == 1
        tool = tools[0]
        assert tool.name == func_obj.tool_name
        assert func_obj.full_function_name.replace(".", "__") in tool.description
        assert func_obj.comment in tool.description
        assert "{'number': {'description': 'Add a given number to 10.'" in tool.description
        assert tool.client_config == client.to_dict()
        input_args = {"number": 2}
        raw_result = tool.fn(**input_args)
        result = json.loads(raw_result)["value"]
        assert result == "12"

        toolkit = UCFunctionToolkit(function_names=[f"{CATALOG}.{SCHEMA}.*"], client=client)
        assert len(toolkit.tools) >= 1
        assert func_obj.tool_name in [t.name for t in toolkit.tools]


@pytest.mark.parametrize("execution_mode", ["serverless", "local"])
@requires_databricks
def test_multiple_toolkits(execution_mode):
    client = get_client()
    client.execution_mode = ExecutionModeDatabricks(execution_mode)
    with set_default_client(client), create_function_and_cleanup(client, schema=SCHEMA) as func_obj:
        toolkit1 = UCFunctionToolkit(function_names=[func_obj.full_function_name])
        toolkit2 = UCFunctionToolkit(function_names=[f"{CATALOG}.{SCHEMA}.*"])
        tool1 = toolkit1.tools[0]
        tool2 = [t for t in toolkit2.tools if t.name == func_obj.tool_name][0]
        input_args = {"number": 3}
        raw_result1 = tool1.fn(**input_args)
        raw_result2 = tool2.fn(**input_args)
        result1 = json.loads(raw_result1)["value"]
        result2 = json.loads(raw_result2)["value"]
        assert result1 == result2


def test_toolkit_creation_errors():
    with pytest.raises(ValidationError, match=r"Input should be an instance of BaseFunctionClient"):
        UCFunctionToolkit(function_names=[], client="client")


def test_toolkit_function_argument_errors(client):
    with pytest.raises(
        ValidationError,
        match=r".*Cannot create tool instances without function_names being provided.*",
    ):
        UCFunctionToolkit(client=client)


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
        data_type=data_type,
        full_data_type=full_data_type,
        return_params=return_params,
    )


def test_uc_function_to_crewai_tool(client):
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
        tool = UCFunctionToolkit.uc_function_to_crewai_tool(
            function_name=f"{CATALOG}.{SCHEMA}.test", client=client
        )
        result = json.loads(tool.fn(x="some_string"))["value"]
        assert result == "some_string"

        # Check defaults for parameters not defined by UC
        assert tool.cache_function(1, {1: 1})
        assert not tool.result_as_answer
        assert not tool.description_updated


@pytest.mark.parametrize(
    "filter_accessible_functions",
    [True, False],
)
def test_uc_function_to_crewai_tool_permission_denied(filter_accessible_functions):
    client = get_client()
    # Permission Error should be caught
    with mock.patch(
        "unitycatalog.ai.core.databricks.DatabricksFunctionClient.get_function",
        side_effect=PermissionError("Permission Denied to Underlying Assets"),
    ):
        if filter_accessible_functions:
            tool = UCFunctionToolkit.uc_function_to_crewai_tool(
                client=client,
                function_name=f"{CATALOG}.{SCHEMA}.test",
                filter_accessible_functions=filter_accessible_functions,
            )
            assert tool == None
        else:
            with pytest.raises(PermissionError):
                tool = UCFunctionToolkit.uc_function_to_crewai_tool(
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
            tool = UCFunctionToolkit.uc_function_to_crewai_tool(
                client=client,
                function_name=f"{CATALOG}.{SCHEMA}.test",
                filter_accessible_functions=filter_accessible_functions,
            )


def test_toolkit_with_wildcard_skips_inaccessible_functions():
    client = get_client()
    with mock_schema_with_inaccessible_function(client):
        toolkit = UCFunctionToolkit(
            function_names=["catalog.schema.*"], client=client, filter_accessible_functions=True
        )

    assert [tool.name for tool in toolkit.tools] == ["catalog__schema__accessible"]


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
def test_crewai_tool_with_tracing_as_retriever(
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

        mlflow.crewai.autolog()

        tool = UCFunctionToolkit.uc_function_to_crewai_tool(
            function_name=mock_function_info.full_name, client=client
        )
        result = tool.fn(x="some input")
        assert json.loads(result)["value"] == function_output

        trace = mlflow.get_trace(mlflow.get_last_active_trace_id())
        assert trace is not None
        assert trace.data.spans[0].name == mock_function_info.full_name
        assert trace.info.execution_time_ms is not None
        assert trace.data.request == '{"x": "some input"}'
        assert trace.data.response == RETRIEVER_OUTPUT_SCALAR

        mlflow.crewai.autolog(disable=True)


def test_toolkit_with_invalid_function_input(client):
    """Test toolkit with invalid input parameters for function conversion."""
    mock_function_info = generate_function_info()

    with (
        mock.patch(
            "unitycatalog.ai.core.utils.client_utils.validate_or_set_default_client",
            return_value=client,
        ),
        mock.patch.object(client, "get_function", return_value=mock_function_info),
    ):
        # Test with invalid input params that are not matching expected schema
        invalid_inputs = {"unexpected_key": "value"}
        tool = UCFunctionToolkit.uc_function_to_crewai_tool(
            function_name="catalog.schema.test", client=client
        )

        with pytest.raises(ValueError, match="Extra parameters provided that are not defined"):
            tool.fn(**invalid_inputs)


def test_toolkit_crewai_kwarg_passthrough(client):
    """Test toolkit with invalid input parameters for function conversion."""
    mock_function_info = generate_function_info()

    with (
        mock.patch(
            "unitycatalog.ai.core.utils.client_utils.validate_or_set_default_client",
            return_value=client,
        ),
        mock.patch.object(client, "get_function", return_value=mock_function_info),
    ):
        tool = UCFunctionToolkit.uc_function_to_crewai_tool(
            function_name="catalog.schema.test",
            client=client,
            cache_function=lambda: True,
            result_as_answer=True,
            description_updated=True,
        )

        assert tool.cache_function()
        assert tool.result_as_answer
        assert tool.description_updated


@pytest.mark.parametrize("native", [False, True], ids=["json", "native"])
@pytest.mark.parametrize("use_async", [False, True])
@pytest.mark.parametrize("encoded", BINARY_ARGUMENTS)
@pytest.mark.parametrize(
    "function_info",
    [
        generate_function_info_with_params(JSON_ARGUMENT_PARAMS),
        generate_function_info_with_params_oss(JSON_ARGUMENT_PARAMS),
    ],
    ids=["databricks", "oss"],
)
@pytest.mark.asyncio
async def test_tool_sends_arguments_as_json_values(function_info, encoded, use_async, native):
    client = mock.create_autospec(BaseFunctionClient, instance=True)
    client.get_function.return_value = function_info
    client.to_dict.return_value = {}
    client.execute_function.return_value = FunctionExecutionResult(format="SCALAR", value="ok")
    toolkit = UCFunctionToolkit(function_names=[function_info.full_name], client=client)
    tool = toolkit.tools[0].to_structured_tool()
    arguments = {**JSON_ARGUMENTS, "data": encoded}
    tool_call_arguments = {
        **json.loads(json.dumps(arguments)),
        **({**NATIVE_ARGUMENTS, "data": base64.b64decode(encoded)} if native else {}),
    }

    if use_async:
        await tool.ainvoke(tool_call_arguments)
    else:
        tool.invoke(tool_call_arguments)

    # A native Decimal is sent as its text, as before this change.
    expected = {**arguments, "amount": "12.5"} if native else arguments
    assert client.execute_function.call_args.kwargs["parameters"] == expected


@pytest.mark.parametrize(
    "function_info",
    [
        generate_function_info_with_params({"a": ("int", "integer"), "b": ("int", "integer")}),
        generate_function_info_with_params_oss({"a": ("int", "integer"), "b": ("int", "integer")}),
    ],
    ids=["databricks", "oss"],
)
def test_tool_rejects_missing_argument_without_default(function_info):
    client = mock.create_autospec(BaseFunctionClient, instance=True)
    client.get_function.return_value = function_info
    client.to_dict.return_value = {}
    toolkit = UCFunctionToolkit(function_names=[function_info.full_name], client=client)
    tool = toolkit.tools[0].to_structured_tool()

    with pytest.raises(ValueError) as exc_info:
        tool.invoke({"b": 1})

    cause = exc_info.value.__cause__
    assert isinstance(cause, ValidationError)
    assert [(e["type"], e["loc"]) for e in cause.errors()] == [("missing", ("a",))]
    client.execute_function.assert_not_called()
