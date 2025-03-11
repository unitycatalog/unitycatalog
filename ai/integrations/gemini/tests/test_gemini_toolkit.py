import json
import os
from unittest import mock

import pytest
from databricks.sdk.service.catalog import (
    FunctionInfo,
    FunctionParameterInfo,
    FunctionParameterInfos,
)
from google.generativeai import GenerativeModel
from google.generativeai.types import CallableFunctionDeclaration
from pydantic import ValidationError

from unitycatalog.ai.core.client import FunctionExecutionResult
from unitycatalog.ai.core.databricks import ExecutionMode
from unitycatalog.ai.core.utils.validation_utils import has_retriever_signature
from unitycatalog.ai.gemini.toolkit import GeminiTool, UCFunctionToolkit
from unitycatalog.ai.test_utils.client_utils import (
    TEST_IN_DATABRICKS,
    client,  # noqa: F401
    get_client,
    requires_databricks,
    set_default_client,
)
from unitycatalog.ai.test_utils.function_utils import (
    CATALOG,
    create_function_and_cleanup,
    create_table_function_and_cleanup,
)
from unitycatalog.client.models.function_parameter_type import FunctionParameterType

SCHEMA = os.environ.get("SCHEMA", "ucai_gemini_test")


@pytest.fixture
def sample_gemini_tool():
    def dummy_function(**kwargs):
        return "dummy_result"

    name = "sample_function"
    description = "A sample function for testing."
    schema = {
        "name": name,
        "description": description,
        "parameters": {
            "properties": {
                "location": {
                    "type": "number",
                    "description": "Retrieves the current weather from a provided location.",
                    "nullable": True,
                }
            }
        },
        "type": "object",
        "required": ["location"],
    }

    return GeminiTool(fn=dummy_function, name=name, description=description, schema=schema)


def test_gemini_tool_to_dict(sample_gemini_tool):
    """Test the `to_dict` method of GeminiTool."""
    expected_output = {
        "name": sample_gemini_tool.name,
        "description": sample_gemini_tool.description,
        "schema": sample_gemini_tool.schema,
    }
    assert sample_gemini_tool.to_dict() == expected_output


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
        assert func_obj.comment in tool.description

        input_args = {"number": 4}
        raw_result = tool.fn(**input_args)
        result = json.loads(raw_result)["value"]
        assert result == "14"

        toolkit = UCFunctionToolkit(
            function_names=[f.full_name for f in client.list_functions(CATALOG, SCHEMA)]
        )
        assert len(toolkit.tools) >= 1
        assert func_obj.tool_name in [t.name for t in toolkit.tools]


@pytest.mark.parametrize("execution_mode", ["serverless", "local"])
@requires_databricks
def test_toolkit_e2e_manually_passing_client(execution_mode):
    client = get_client()
    client.execution_mode = ExecutionMode(execution_mode)
    with set_default_client(client), create_function_and_cleanup(client, schema=SCHEMA) as func_obj:
        toolkit = UCFunctionToolkit(function_names=[func_obj.full_function_name], client=client)
        tools = toolkit.tools
        assert len(tools) == 1
        tool = tools[0]
        assert tool.name == func_obj.tool_name
        assert func_obj.comment in tool.description
        input_args = {"number": 5}
        raw_result = tool.fn(**input_args)
        result = json.loads(raw_result)["value"]
        assert result == "15"

        toolkit = UCFunctionToolkit(
            function_names=[f.full_name for f in client.list_functions(CATALOG, SCHEMA)],
            client=client,
        )
        assert len(toolkit.tools) >= 1
        assert func_obj.tool_name in [t.name for t in toolkit.tools]


@pytest.mark.parametrize("execution_mode", ["serverless", "local"])
@requires_databricks
def test_multiple_toolkits(execution_mode):
    client = get_client()
    client.execution_mode = ExecutionMode(execution_mode)
    with set_default_client(client), create_function_and_cleanup(client, schema=SCHEMA) as func_obj:
        toolkit1 = UCFunctionToolkit(function_names=[func_obj.full_function_name])
        toolkit2 = UCFunctionToolkit(
            function_names=[f.full_name for f in client.list_functions(CATALOG, SCHEMA)]
        )
        tool1 = toolkit1.tools[0]
        tool2 = [t for t in toolkit2.tools if t.name == func_obj.tool_name][0]
        input_args = {"number": 6}
        raw_result1 = tool1.fn(**input_args)
        raw_result2 = tool2.fn(**input_args)
        result1 = json.loads(raw_result1)["value"]
        result2 = json.loads(raw_result2)["value"]
        assert result1 == result2


def test_toolkit_creation_errors():
    with pytest.raises(ValidationError, match=r"Input should be an instance of BaseFunctionClient"):
        UCFunctionToolkit(function_names=[], client="client")


def test_toolkit_creation_errors(client):
    with pytest.raises(
        ValueError, match=r"Cannot create tool instances without function_names being provided."
    ):
        UCFunctionToolkit(function_names=[], client=client)


def test_toolkit_function_argument_errors(client):
    with pytest.raises(
        ValidationError,
        match=r"1 validation error for UCFunctionToolkit\nfunction_names\n  Field required",
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
            "comment": "test comment",
            "name": "x",
            "type_text": "string",
            "type_json": '{"name":"x","type":"string","nullable":true,"metadata":{"EXISTS_DEFAULT":"\\"123\\"","default":"\\"123\\"","CURRENT_DEFAULT":"\\"123\\""}}',
            "type_name": "STRING",
            "type_precision": 0,
            "type_scale": 0,
            "position": 17,
            "parameter_type": FunctionParameterType.PARAM,
        }
    ]
    return FunctionInfo(
        catalog_name=catalog,
        schema_name=schema,
        name=name,
        full_name=f"{catalog}.{schema}.{name}",
        input_params=FunctionParameterInfos(
            parameters=[FunctionParameterInfo(**param) for param in parameters]
        ),
        comment="Executes Python code and returns its stdout.",
        data_type=data_type,
        full_data_type=full_data_type,
        return_params=return_params,
    )


def test_convert_to_gemini_schema_with_valid_function_info():
    """
    Test convert_to_gemini_schema with valid FunctionInfo input.
    """
    # Generate mock FunctionInfo using the provided generate_function_info
    function_info = generate_function_info()

    # Convert the FunctionInfo into Gemini-compatible schema
    result_schema = UCFunctionToolkit.convert_to_gemini_schema(function_info)

    # Expected output
    expected_schema = {
        "name": "test",
        "description": "Executes Python code and returns its stdout.",
        "parameters": {
            "properties": {
                "x": {"type": "string", "description": "test comment", "nullable": True},
            },
            "type": "object",
            "required": ["x"],
        },
    }

    assert result_schema == expected_schema, (
        "The generated schema does not match the expected output."
    )


def test_uc_function_to_gemini_tool(client):
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
        tool = UCFunctionToolkit.uc_function_to_gemini_tool(
            function_name=f"{CATALOG}.{SCHEMA}.test", client=client
        )
        result = json.loads(tool.fn(x="some_string"))["value"]
        assert result == "some_string"


@pytest.mark.parametrize(
    "filter_accessible_functions",
    [True, False],
)
def test_uc_function_to_gemini_tool_permission_denied(filter_accessible_functions):
    client = get_client()
    # Permission Error should be caught
    with mock.patch(
        "unitycatalog.ai.core.databricks.DatabricksFunctionClient.get_function",
        side_effect=PermissionError("Permission Denied to Underlying Assets"),
    ):
        if filter_accessible_functions:
            tool = UCFunctionToolkit.uc_function_to_gemini_tool(
                client=client,
                function_name=f"{CATALOG}.{SCHEMA}.test",
                filter_accessible_functions=filter_accessible_functions,
            )
            assert tool == None
        else:
            with pytest.raises(PermissionError):
                tool = UCFunctionToolkit.uc_function_to_gemini_tool(
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
            tool = UCFunctionToolkit.uc_function_to_gemini_tool(
                client=client,
                function_name=f"{CATALOG}.{SCHEMA}.test",
                filter_accessible_functions=filter_accessible_functions,
            )


@requires_databricks
def test_gemini_tool_calling_with_trace_as_retriever():
    client = get_client()
    import mlflow

    if TEST_IN_DATABRICKS:
        import mlflow.tracking._model_registry.utils

        mlflow.tracking._model_registry.utils._get_registry_uri_from_spark_session = (
            lambda: "databricks-uc"
        )

    with (
        set_default_client(client),
        create_table_function_and_cleanup(client, schema=SCHEMA) as func_obj,
    ):
        func_name = func_obj.full_function_name
        func_info = client.get_function(func_name)
        assert has_retriever_signature(func_info)
        toolkit = UCFunctionToolkit(function_names=[func_name])
        tools = toolkit.generate_callable_tool_list()

        mock_response = mock.MagicMock()

        mock_function_call = mock.MagicMock()
        mock_function_call.name = func_obj.full_function_name
        mock_function_call.args = {
            "query": "What are the page contents?",
        }
        mock_part = mock.MagicMock()
        mock_part.functionCall = mock_function_call
        mock_content = mock.MagicMock()
        mock_content.parts = [mock_part]
        mock_content.role = "model"
        mock_candidate = mock.MagicMock()
        mock_candidate.content = mock_content
        mock_candidate.finishReason = 1  # STOP
        mock_candidate.index = 0
        mock_candidate.safetyRatings = [
            {"category": "HARM_CATEGORY_SEXUALLY_EXPLICIT", "probability": "NEGLIGIBLE"},
            {"category": "HARM_CATEGORY_HARASSMENT", "probability": "NEGLIGIBLE"},
            {"category": "HARM_CATEGORY_HATE_SPEECH", "probability": "NEGLIGIBLE"},
            {"category": "HARM_CATEGORY_DANGEROUS_CONTENT", "probability": "NEGLIGIBLE"},
        ]
        mock_response.candidates = [mock_candidate]
        mock_prompt_feedback = mock.MagicMock()
        mock_prompt_feedback.block_reason = None
        mock_response.prompt_feedback = mock_prompt_feedback

        with mock.patch("google.generativeai.ChatSession.send_message", return_value=mock_response):
            mlflow.gemini.autolog()

            model = GenerativeModel(model_name="gemini-2.0-flash-exp", tools=tools)
            chat = model.start_chat(enable_automatic_function_calling=True)
            response = chat.send_message("What does the table say?")

            tool_call = response.candidates[0].content.parts[0].functionCall
            tool_call_args = tool_call.args

            result = client.execute_function(
                func_name, tool_call_args, enable_retriever_tracing=True
            )

            expected_result = "page_content,metadata\ntesting,\"{'doc_uri': 'https://docs.databricks.com/', 'chunk_id': '1'}\"\n"
            assert result.value == expected_result

            trace = mlflow.get_last_active_trace()
            assert trace is not None
            assert trace.data.spans[0].name == func_name
            assert trace.info.execution_time_ms is not None
            assert trace.data.request == json.dumps(tool_call_args)
            assert (
                trace.data.response
                == '[{"page_content": "testing", "metadata": {"doc_uri": "https://docs.databricks.com/", "chunk_id": "1"}}]'
            )


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
        invalid_inputs = {"unexpected_key": "value"}
        tool = UCFunctionToolkit.uc_function_to_gemini_tool(
            function_name="catalog.schema.test", client=client
        )

        with pytest.raises(ValueError, match="Parameter x is required but not provided."):
            tool.fn(**invalid_inputs)


def test_generate_callable_tool_list(client):
    """
    Test the generate_callable_tool_list method of UCFunctionToolkit.
    """
    # Mock UCFunctionToolkit instance with a GeminiTool
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
        toolkit = UCFunctionToolkit(function_names=["catalog.schema.test_function"], client=client)

    # Generate callable tool list
    callable_tools = toolkit.generate_callable_tool_list()
    tools = toolkit.tools

    # Verify the output
    assert len(callable_tools) == 1, "The callable tool list should contain exactly one tool."

    gemini_tool = callable_tools[0]
    tool = tools[0]
    assert isinstance(gemini_tool, CallableFunctionDeclaration), (
        "The tool should be a CallableFunctionDeclaration."
    )
    assert tool.name == "catalog__schema__test_function", (
        "The tool's name does not match the expected name."
    )
    assert tool.description == "Executes Python code and returns its stdout.", (
        "The tool's description does not match the expected description."
    )
    assert "parameters" in tool.schema, "The tool's schema should include parameters."
    assert tool.schema["parameters"]["required"] == ["x"], "The required parameters do not match."
