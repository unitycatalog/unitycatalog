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
from langchain_core.messages import BaseMessage
from langchain_core.outputs import ChatGeneration, ChatResult
from langchain_core.runnables import RunnableGenerator
from langchain_databricks.chat_models import ChatDatabricks, ChatGeneration
from langgraph.prebuilt import create_react_agent

from unitycatalog.ai.core.base import (
    FunctionExecutionResult,
)
from unitycatalog.ai.core.databricks import ExecutionMode
from unitycatalog.ai.core.utils.function_processing_utils import get_tool_name
from unitycatalog.ai.test_utils.client_utils import (
    TEST_IN_DATABRICKS,
    client,  # noqa: F401
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
    create_python_function_and_cleanup,
)

try:
    # v2
    from pydantic.v1.error_wrappers import ValidationError
except ImportError:
    # v1
    from pydantic.error_wrappers import ValidationError

from tests.helper_functions import wrap_output
from unitycatalog.ai.langchain.toolkit import UCFunctionToolkit

SCHEMA = os.environ.get("SCHEMA", "ucai_langchain_test")


@pytest.mark.parametrize("execution_mode", ["serverless", "local"])
@requires_databricks
def test_toolkit_e2e(execution_mode):
    client = get_client()
    client.execution_mode = ExecutionMode(execution_mode)
    with set_default_client(client), create_function_and_cleanup(client, schema=SCHEMA) as func_obj:
        toolkit = UCFunctionToolkit(function_names=[func_obj.full_function_name])
        tools = toolkit.tools
        assert len(tools) == 1
        tool = tools[0]
        assert tool.name == func_obj.tool_name
        assert tool.description == func_obj.comment
        assert tool.client_config == client.to_dict()
        tool.args_schema(**{"number": 1})
        raw_result = tool.func(number=1)
        result = json.loads(raw_result)["value"]
        assert result == "11"

        toolkit = UCFunctionToolkit(function_names=[f"{CATALOG}.{SCHEMA}.*"])
        assert len(toolkit.tools) >= 1
        assert func_obj.tool_name in [t.name for t in toolkit.tools]


@pytest.mark.parametrize("execution_mode", ["serverless", "local"])
@requires_databricks
def test_toolkit_e2e_manually_passing_client(execution_mode):
    client = get_client()
    client.execution_mode = ExecutionMode(execution_mode)
    with create_function_and_cleanup(client, schema=SCHEMA) as func_obj:
        toolkit = UCFunctionToolkit(function_names=[func_obj.full_function_name], client=client)
        tools = toolkit.tools
        assert len(tools) == 1
        tool = tools[0]
        assert tool.name == func_obj.tool_name
        assert tool.description == func_obj.comment
        assert tool.uc_function_name == func_obj.full_function_name
        assert tool.client_config == client.to_dict()
        tool.args_schema(**{"number": 2})
        raw_result = tool.func(number=2)
        result = json.loads(raw_result)["value"]
        assert result == "12"

        toolkit = UCFunctionToolkit(function_names=[f"{CATALOG}.{SCHEMA}.*"], client=client)
        assert len(toolkit.tools) >= 1
        assert func_obj.tool_name in [t.name for t in toolkit.tools]


@pytest.mark.parametrize("execution_mode", ["serverless", "local"])
@requires_databricks
def test_toolkit_e2e_tools_with_no_params(execution_mode):
    client = get_client()
    client.execution_mode = ExecutionMode(execution_mode)

    def get_weather() -> str:
        """
        Get the weather.
        """
        return "sunny"

    with create_python_function_and_cleanup(client, schema=SCHEMA, func=get_weather) as func_obj:
        toolkit = UCFunctionToolkit(function_names=[func_obj.full_function_name], client=client)
        tools = toolkit.tools
        assert len(tools) == 1
        tool = tools[0]
        assert tool.name == func_obj.tool_name
        assert tool.description == func_obj.comment
        assert tool.uc_function_name == func_obj.full_function_name
        assert tool.client_config == client.to_dict()
        tool.args_schema()
        raw_result = tool.func()
        result = json.loads(raw_result)["value"]
        assert result == "sunny"

        toolkit = UCFunctionToolkit(function_names=[f"{CATALOG}.{SCHEMA}.*"], client=client)
        assert len(toolkit.tools) >= 1
        assert func_obj.tool_name in [t.name for t in toolkit.tools]


@pytest.mark.parametrize("execution_mode", ["serverless", "local"])
@requires_databricks
def test_multiple_toolkits(execution_mode):
    client = get_client()
    client.execution_mode = ExecutionMode(execution_mode)
    with set_default_client(client), create_function_and_cleanup(client, schema=SCHEMA) as func_obj:
        toolkit1 = UCFunctionToolkit(function_names=[func_obj.full_function_name])
        toolkit2 = UCFunctionToolkit(function_names=[f"{CATALOG}.{SCHEMA}.*"])
        tool1 = toolkit1.tools[0]
        tool2 = [t for t in toolkit2.tools if t.name == func_obj.tool_name][0]
        input_args = {"number": 8}
        assert tool1.func(**input_args) == tool2.func(**input_args)


def test_toolkit_creation_errors():
    with pytest.raises(ValueError, match=r"instance of BaseFunctionClient expected"):
        UCFunctionToolkit(function_names=[], client="client")


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


def test_uc_function_to_langchain_tool():
    client = get_client()
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
        tool = UCFunctionToolkit.uc_function_to_langchain_tool(
            client=client, function_name=f"{CATALOG}.{SCHEMA}.test"
        )
        assert tool.name == get_tool_name(f"{CATALOG}.{SCHEMA}.test")
        assert json.loads(tool.func(x="some_string"))["value"] == "some_string"


@pytest.mark.parametrize(
    "filter_accessible_functions",
    [True, False],
)
def test_uc_function_to_langchain_tool_permission_denied(filter_accessible_functions):
    client = get_client()
    # Permission Error should be caught
    with mock.patch(
        "unitycatalog.ai.core.databricks.DatabricksFunctionClient.get_function",
        side_effect=PermissionError("Permission Denied to Underlying Assets"),
    ):
        if filter_accessible_functions:
            tool = UCFunctionToolkit.uc_function_to_langchain_tool(
                client=client,
                function_name=f"{CATALOG}.{SCHEMA}.test",
                filter_accessible_functions=filter_accessible_functions,
            )
            assert tool == None
        else:
            with pytest.raises(PermissionError):
                tool = UCFunctionToolkit.uc_function_to_langchain_tool(
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
            tool = UCFunctionToolkit.uc_function_to_langchain_tool(
                client=client,
                function_name=f"{CATALOG}.{SCHEMA}.test",
                filter_accessible_functions=filter_accessible_functions,
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
def test_langchain_tool_trace_as_retriever(
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

        mlflow.langchain.autolog()

        tool = UCFunctionToolkit.uc_function_to_langchain_tool(
            client=client, function_name=mock_function_info.full_name
        )

        result = tool.func(x="some_string")
        assert json.loads(result)["value"] == function_output

        trace = mlflow.get_last_active_trace()
        assert trace is not None
        assert trace.data.spans[0].name == mock_function_info.full_name
        assert trace.info.execution_time_ms is not None
        assert trace.data.request == '{"x": "some_string"}'
        assert trace.data.response == RETRIEVER_OUTPUT_SCALAR

        mlflow.langchain.autolog(disable=True)


@requires_databricks
@pytest.mark.parametrize("schema", [SCHEMA, "ucai_langchain_test_star"])
def test_langgraph_agents(schema):
    client = get_client()
    with create_function_and_cleanup(client, schema=schema) as func_obj:
        if schema == SCHEMA:
            toolkit = UCFunctionToolkit(function_names=[func_obj.full_function_name], client=client)
        else:
            toolkit = UCFunctionToolkit(function_names=[f"{CATALOG}.{schema}.*"], client=client)
        system_message = "You are a helpful assistant. Make sure to use tool for information."
        llm = ChatDatabricks(endpoint="databricks-meta-llama-3-1-70b-instruct")
        agent = create_react_agent(llm, toolkit.tools, state_modifier=system_message)
        chain = agent | RunnableGenerator(wrap_output)
        # we need this mock as the serving endpoint is not stable
        with mock.patch.object(
            llm,
            "_generate",
            return_value=ChatResult(
                generations=[
                    ChatGeneration(
                        text="1024",
                        type="ChatGeneration",
                        message=BaseMessage(content="1024", type="tool"),
                    )
                ]
            ),
        ):
            result = chain.invoke(
                {"messages": [{"role": "user", "content": "What is the result of 2**10?"}]}
            )
        assert "1024" in result


def test_toolkit_fields_validation(client):
    def test_tool():
        return "abc"

    with pytest.raises(ValidationError, match=r"str type expected"):
        UCFunctionToolkit(client=client, function_names=[test_tool])

    with pytest.raises(ValidationError, match=r"Invalid function name"):
        UCFunctionToolkit(client=client, function_names=["test_tool"])

    with pytest.raises(ValidationError, match=r"instance of BaseFunctionClient expected"):
        UCFunctionToolkit(client=test_tool, function_names=[])
