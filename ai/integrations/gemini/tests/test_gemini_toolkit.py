import dataclasses
import json
import os
from unittest import mock

import pytest
from databricks.sdk.service.catalog import (
    FunctionInfo,
    FunctionParameterInfo,
    FunctionParameterInfos,
)
from google.generativeai import GenerativeModel, protos
from google.generativeai import client as generativeai_client
from google.generativeai.types import CallableFunctionDeclaration, GenerateContentResponse
from pydantic import ValidationError

from unitycatalog.ai.core.base import BaseFunctionClient
from unitycatalog.ai.core.client import FunctionExecutionResult
from unitycatalog.ai.core.paged_list import PagedList
from unitycatalog.ai.core.utils.execution_utils import ExecutionModeDatabricks
from unitycatalog.ai.core.utils.validation_utils import has_retriever_signature
from unitycatalog.ai.gemini.toolkit import GeminiTool, UCFunctionToolkit, _restore_integers
from unitycatalog.ai.gemini.utils import generate_tool_call_messages
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
    create_function_and_cleanup,
    create_table_function_and_cleanup,
    generate_function_info_with_params,
    mock_schema_with_inaccessible_function,
)
from unitycatalog.ai.test_utils.function_utils_oss import generate_function_info_with_params_oss
from unitycatalog.client import FunctionInfo as OSSFunctionInfo
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
    client.execution_mode = ExecutionModeDatabricks(execution_mode)
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
    client.execution_mode = ExecutionModeDatabricks(execution_mode)
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
    client.execution_mode = ExecutionModeDatabricks(execution_mode)
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


def test_toolkit_creation_errors_with_client(client):
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
        "name": "catalog__schema__test",
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


def test_toolkit_with_wildcard_skips_inaccessible_functions():
    client = get_client()
    with mock_schema_with_inaccessible_function(client):
        toolkit = UCFunctionToolkit(
            function_names=["catalog.schema.*"], client=client, filter_accessible_functions=True
        )

    assert [tool.name for tool in toolkit.tools] == ["catalog__schema__accessible"]


@requires_databricks
def test_gemini_tool_calling_with_trace_as_retriever():
    client = get_client()
    import mlflow

    if TEST_IN_DATABRICKS:
        import mlflow.tracking._model_registry.utils

        mlflow.tracking._model_registry.utils._get_registry_uri_from_spark_session = lambda: (
            "databricks-uc"
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

            trace = mlflow.get_trace(mlflow.get_last_active_trace_id())
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


@pytest.mark.parametrize(
    "function_names",
    [["catalog.schema.get_weather", "catalog.schema.test"], ["catalog.schema.*"]],
    ids=["by_name", "wildcard"],
)
@pytest.mark.parametrize(
    "no_params_function_info",
    [
        FunctionInfo(
            catalog_name="catalog",
            schema_name="schema",
            name="get_weather",
            full_name="catalog.schema.get_weather",
            comment="Get the weather.",
            input_params=None,
        ),
        OSSFunctionInfo(
            catalog_name="catalog",
            schema_name="schema",
            name="get_weather",
            full_name="catalog.schema.get_weather",
            comment="Get the weather.",
            data_type="STRING",
            full_data_type="STRING",
            input_params=None,
        ),
    ],
    ids=["databricks", "oss"],
)
def test_toolkit_with_function_without_params(no_params_function_info, function_names):
    function_infos = {
        info.full_name: info for info in [no_params_function_info, generate_function_info()]
    }
    client = mock.create_autospec(BaseFunctionClient, instance=True)
    client.get_function.side_effect = function_infos.get
    client.list_functions.return_value = PagedList(list(function_infos.values()), None)
    client.to_dict.return_value = {}
    client.execute_function.return_value = FunctionExecutionResult(format="SCALAR", value="sunny")

    toolkit = UCFunctionToolkit(function_names=function_names, client=client)
    declarations = {
        declaration.name: declaration for declaration in toolkit.generate_callable_tool_list()
    }

    assert {tool.name: tool.schema for tool in toolkit.tools}["catalog__schema__get_weather"] == {
        "name": "catalog__schema__get_weather",
        "description": "Get the weather.",
    }
    assert declarations["catalog__schema__test"].to_proto().parameters.required == ["x"]
    response = declarations["catalog__schema__get_weather"](
        protos.FunctionCall(name="catalog__schema__get_weather", args={})
    )
    client.execute_function.assert_called_once_with(
        function_name="catalog.schema.get_weather", parameters={}, enable_retriever_tracing=False
    )
    assert json.loads(type(response).to_dict(response)["response"]["result"]) == {
        "format": "SCALAR",
        "value": "sunny",
    }


def function_infos_named(generate, full_names):
    function_info = generate({"x": ("STRING", "string")})
    infos = {}
    for full_name in full_names:
        catalog, schema, name = full_name.split(".")
        fields = {
            "catalog_name": catalog,
            "schema_name": schema,
            "name": name,
            "full_name": full_name,
        }
        if isinstance(function_info, OSSFunctionInfo):
            infos[full_name.lower()] = function_info.model_copy(update=fields)
        else:
            infos[full_name.lower()] = dataclasses.replace(function_info, **fields)
    return infos


def function_infos_in_schemas(generate, schemas):
    return function_infos_named(generate, [f"catalog.{schema}.test" for schema in schemas])


def mock_client_for(function_infos):
    client = mock.create_autospec(BaseFunctionClient, instance=True)
    client.get_function.side_effect = lambda name, **kwargs: function_infos[name.lower()]
    client.to_dict.return_value = {}
    client.execute_function.return_value = FunctionExecutionResult(format="SCALAR", value="ok")
    return client


def model_response(part):
    return protos.GenerateContentResponse(
        candidates=[
            protos.Candidate(
                content=protos.Content(role="model", parts=[part]),
                finish_reason=protos.Candidate.FinishReason.STOP,
            )
        ]
    )


def function_call_response(name, args):
    return model_response(protos.Part(function_call=protos.FunctionCall(name=name, args=args)))


SAME_NAMED_FUNCTIONS = ["catalog.sales.test", "catalog.hr.test"]

FUNCTION_INFO_GENERATORS = pytest.mark.parametrize(
    "generate",
    [generate_function_info_with_params, generate_function_info_with_params_oss],
    ids=["databricks", "oss"],
)


@FUNCTION_INFO_GENERATORS
def test_toolkit_declares_same_named_functions_by_tool_name(generate):
    client = mock_client_for(function_infos_in_schemas(generate, ["sales", "hr"]))
    toolkit = UCFunctionToolkit(function_names=SAME_NAMED_FUNCTIONS, client=client)
    declarations = toolkit.generate_callable_tool_list()

    GenerativeModel(model_name="gemini-2.0-flash", tools=declarations)

    assert [declaration.name for declaration in declarations] == [
        "catalog__sales__test",
        "catalog__hr__test",
    ]
    assert [tool.name for tool in toolkit.tools] == ["catalog__sales__test", "catalog__hr__test"]


@pytest.mark.parametrize(
    ("called", "tool_name"),
    [("catalog.sales.test", "catalog__sales__test"), ("catalog.hr.test", "catalog__hr__test")],
)
@FUNCTION_INFO_GENERATORS
def test_generate_tool_call_messages_runs_the_called_same_named_function(
    generate, called, tool_name
):
    client = mock_client_for(function_infos_in_schemas(generate, ["sales", "hr"]))
    toolkit = UCFunctionToolkit(function_names=SAME_NAMED_FUNCTIONS, client=client)
    model = GenerativeModel(
        model_name="gemini-2.0-flash", tools=toolkit.generate_callable_tool_list()
    )
    response = GenerateContentResponse.from_response(function_call_response(tool_name, {"x": "hi"}))

    _, parts = generate_tool_call_messages(model=model, response=response, conversation_history=[])

    client.execute_function.assert_called_once_with(
        function_name=called, parameters={"x": "hi"}, enable_retriever_tracing=False
    )
    assert [part.function_response.name for part in parts] == [tool_name]


@pytest.mark.parametrize(
    ("called", "tool_name"),
    [("catalog.sales.test", "catalog__sales__test"), ("catalog.hr.test", "catalog__hr__test")],
)
@FUNCTION_INFO_GENERATORS
def test_automatic_function_calling_runs_the_called_same_named_function(
    generate, called, tool_name
):
    client = mock_client_for(function_infos_in_schemas(generate, ["sales", "hr"]))
    toolkit = UCFunctionToolkit(function_names=SAME_NAMED_FUNCTIONS, client=client)
    model = GenerativeModel(
        model_name="gemini-2.0-flash", tools=toolkit.generate_callable_tool_list()
    )
    gemini_client = mock.MagicMock()
    gemini_client.generate_content.side_effect = [
        function_call_response(tool_name, {"x": "hi"}),
        model_response(protos.Part(text="done")),
    ]

    with mock.patch.object(
        generativeai_client, "get_default_generative_client", return_value=gemini_client
    ):
        response = model.start_chat(enable_automatic_function_calling=True).send_message("hi")

    assert response.text == "done"
    client.execute_function.assert_called_once_with(
        function_name=called, parameters={"x": "hi"}, enable_retriever_tracing=False
    )


@FUNCTION_INFO_GENERATORS
def test_mixed_case_function_name_is_declared_as_the_tool_name(generate):
    client = mock_client_for(function_infos_in_schemas(generate, ["hr"]))
    toolkit = UCFunctionToolkit(function_names=["Catalog.HR.Test"], client=client)
    model = GenerativeModel(
        model_name="gemini-2.0-flash", tools=toolkit.generate_callable_tool_list()
    )
    response = GenerateContentResponse.from_response(
        function_call_response("Catalog__HR__Test", {"x": "hi"})
    )

    generate_tool_call_messages(model=model, response=response, conversation_history=[])

    assert [tool.name for tool in toolkit.tools] == ["Catalog__HR__Test"]
    client.execute_function.assert_called_once_with(
        function_name="Catalog.HR.Test", parameters={"x": "hi"}, enable_retriever_tracing=False
    )


# Unlike JSON_ARGUMENT_PARAMS, these have no DATE, TIMESTAMP or DECIMAL parameter and no nullable
# struct field, since a Gemini function schema accepts no union type and no default.
PLACE_TYPE_JSON = {
    "type": "struct",
    "fields": [
        {"name": "city", "type": "string", "nullable": False, "metadata": {}},
        {"name": "country", "type": "string", "nullable": False, "metadata": {}},
    ],
}

GEMINI_JSON_ARGUMENT_PARAMS = {
    "address": ("struct<city:string,country:string>", PLACE_TYPE_JSON),
    "stops": (
        "array<struct<city:string,country:string>>",
        {"type": "array", "elementType": PLACE_TYPE_JSON, "containsNull": False},
    ),
    "by_name": (
        "map<string,struct<city:string,country:string>>",
        {
            "type": "map",
            "keyType": "string",
            "valueType": PLACE_TYPE_JSON,
            "valueContainsNull": False,
        },
    ),
    "data": ("binary", "binary"),
}


@pytest.mark.parametrize("encoded", BINARY_ARGUMENTS)
@pytest.mark.parametrize(
    "function_info",
    [
        generate_function_info_with_params(GEMINI_JSON_ARGUMENT_PARAMS),
        generate_function_info_with_params_oss(GEMINI_JSON_ARGUMENT_PARAMS),
    ],
    ids=["databricks", "oss"],
)
def test_function_call_sends_arguments_as_json_values(function_info, encoded):
    client = mock.create_autospec(BaseFunctionClient, instance=True)
    client.get_function.return_value = function_info
    client.to_dict.return_value = {}
    client.execute_function.return_value = FunctionExecutionResult(format="SCALAR", value="ok")
    toolkit = UCFunctionToolkit(function_names=[function_info.full_name], client=client)
    declaration = toolkit.generate_callable_tool_list()[0]
    arguments = {
        "address": {"city": "Oslo", "country": "NO"},
        "stops": [{"city": "Bergen", "country": "NO"}, {"city": "Lund", "country": "SE"}],
        "by_name": {"home": {"city": "Oslo", "country": "NO"}},
        "data": encoded,
    }

    declaration(protos.FunctionCall(name=declaration.name, args=arguments))

    assert client.execute_function.call_args.kwargs["parameters"] == arguments


POINT_TYPE_JSON = {
    "type": "struct",
    "fields": [
        {"name": "x", "type": "integer", "nullable": False, "metadata": {}},
        {"name": "weight", "type": "double", "nullable": False, "metadata": {}},
    ],
}

GEMINI_NUMBER_ARGUMENT_PARAMS = {
    "tiny": ("byte", "byte"),
    "small": ("short", "short"),
    "medium": ("int", "integer"),
    "big": ("long", "long"),
    "ratio": ("double", "double"),
    "label": ("string", "string"),
    "point": ("struct<x:int,weight:double>", POINT_TYPE_JSON),
    "ids": (
        "array<long>",
        {"type": "array", "elementType": "long", "containsNull": True},
    ),
    "counts": (
        "map<string,short>",
        {"type": "map", "keyType": "string", "valueType": "short", "valueContainsNull": False},
    ),
}


@pytest.mark.parametrize(
    ("arguments", "expected"),
    [
        (
            {
                "tiny": 1,
                "small": 2,
                "medium": 3,
                "big": 4,
                "ratio": 2,
                "label": "5",
                "point": {"x": 6, "weight": 7},
                "ids": [8, None],
                "counts": {"a": 9},
            },
            {
                "tiny": 1,
                "small": 2,
                "medium": 3,
                "big": 4,
                "ratio": 2.0,
                "label": "5",
                "point": {"x": 6, "weight": 7.0},
                "ids": [8, None],
                "counts": {"a": 9},
            },
        ),
        (
            {"big": 4.5, "point": {"x": 6.5, "weight": 7.5}, "ids": [8.5], "counts": {"a": 9.5}},
            {"big": 4.5, "point": {"x": 6.5, "weight": 7.5}, "ids": [8.5], "counts": {"a": 9.5}},
        ),
        (
            {"big": 2**53 - 1, "ids": [-(2**53 - 1)]},
            {"big": 2**53 - 1, "ids": [-(2**53 - 1)]},
        ),
        # The protobuf Struct rounds 2**53 + 1 to the double 2**53, so it is sent as a float, which
        # the function client rejects for a LONG parameter, instead of as a different integer.
        (
            {"big": 2**53 + 1, "ids": [-(2**53 + 1)]},
            {"big": float(2**53), "ids": [-float(2**53)]},
        ),
    ],
    ids=["integral", "fractional", "largest-exact", "beyond-float-precision"],
)
@pytest.mark.parametrize(
    "function_info",
    [
        generate_function_info_with_params(GEMINI_NUMBER_ARGUMENT_PARAMS),
        generate_function_info_with_params_oss(GEMINI_NUMBER_ARGUMENT_PARAMS),
    ],
    ids=["databricks", "oss"],
)
def test_function_call_sends_integer_arguments_as_int(function_info, arguments, expected):
    client = mock.create_autospec(BaseFunctionClient, instance=True)
    client.get_function.return_value = function_info
    client.to_dict.return_value = {}
    client.execute_function.return_value = FunctionExecutionResult(format="SCALAR", value="ok")
    toolkit = UCFunctionToolkit(function_names=[function_info.full_name], client=client)
    declaration = toolkit.generate_callable_tool_list()[0]

    declaration(protos.FunctionCall(name=declaration.name, args=arguments))

    parameters = client.execute_function.call_args.kwargs["parameters"]
    assert json.dumps(parameters, sort_keys=True) == json.dumps(expected, sort_keys=True)


@pytest.mark.parametrize(
    "type_json",
    [
        '{"type": "long"}',
        '{"name": "other", "type": "long", "nullable": false, "metadata": {}}',
    ],
    ids=["no-embedded-name", "different-embedded-name"],
)
def test_function_call_sends_integer_argument_by_parameter_name(type_json):
    function_info = generate_function_info_with_params_oss({"n": ("long", "long")})
    function_info.input_params.parameters[0].type_json = type_json
    client = mock.create_autospec(BaseFunctionClient, instance=True)
    client.get_function.return_value = function_info
    client.to_dict.return_value = {}
    client.execute_function.return_value = FunctionExecutionResult(format="SCALAR", value="ok")
    toolkit = UCFunctionToolkit(function_names=[function_info.full_name], client=client)
    declaration = toolkit.generate_callable_tool_list()[0]

    declaration(protos.FunctionCall(name=declaration.name, args={"n": 2}))

    assert repr(client.execute_function.call_args.kwargs["parameters"]) == repr({"n": 2})


INT_STRUCT_TYPE_JSON = {
    "type": "struct",
    "fields": [{"name": "x", "type": "integer", "nullable": False, "metadata": {}}],
}


@pytest.mark.parametrize(
    ("value", "uc_type_json", "expected"),
    [
        (
            [{"x": 1.0}, {"x": 2.0}],
            {"type": "array", "elementType": INT_STRUCT_TYPE_JSON, "containsNull": False},
            [{"x": 1}, {"x": 2}],
        ),
        (
            {"a": [1.0, 2.0]},
            {
                "type": "map",
                "keyType": "string",
                "valueType": {"type": "array", "elementType": "long", "containsNull": True},
                "valueContainsNull": False,
            },
            {"a": [1, 2]},
        ),
        ({"x": 1.0, "extra": 3.0}, INT_STRUCT_TYPE_JSON, {"x": 1, "extra": 3.0}),
        ("1", "int", "1"),
        ([1.0], INT_STRUCT_TYPE_JSON, [1.0]),
        ({"a": 1.0}, {"type": "array", "elementType": "int", "containsNull": False}, {"a": 1.0}),
        (float(2**53 - 1), "long", 2**53 - 1),
        (float(2**53), "long", float(2**53)),
        (-float(2**53), "long", -float(2**53)),
    ],
    ids=[
        "array-of-structs",
        "map-of-arrays",
        "unknown-struct-key",
        "string-for-int",
        "list-for-struct",
        "dict-for-array",
        "largest-exact",
        "first-inexact",
        "first-inexact-negative",
    ],
)
def test_restore_integers(value, uc_type_json, expected):
    assert repr(_restore_integers(value, uc_type_json)) == repr(expected)


LONG_FUNCTION_NAME = "main.customer_analytics_prod.compute_lifetime_value_by_segment"


@pytest.mark.parametrize(
    ("function_name", "declared_name"),
    [
        (
            LONG_FUNCTION_NAME[:-1],
            "main__customer_analytics_prod__compute_lifetime_value_by_segmen",
        ),
        (LONG_FUNCTION_NAME, "ain__customer_analytics_prod__compute_lifetime_value_by_segment"),
        ("team@prod.sales.test", "team_prod__sales__test"),
    ],
    ids=["63-characters", "64-characters", "at-sign"],
)
@FUNCTION_INFO_GENERATORS
def test_declared_name_follows_the_gemini_name_rules(generate, function_name, declared_name):
    client = mock_client_for(function_infos_named(generate, [function_name]))
    toolkit = UCFunctionToolkit(function_names=[function_name], client=client)
    model = GenerativeModel(
        model_name="gemini-2.0-flash", tools=toolkit.generate_callable_tool_list()
    )
    response = GenerateContentResponse.from_response(
        function_call_response(declared_name, {"x": "hi"})
    )

    generate_tool_call_messages(model=model, response=response, conversation_history=[])

    assert len(declared_name) <= 63
    assert [(tool.name, tool.schema["name"]) for tool in toolkit.tools] == [
        (declared_name, declared_name)
    ]
    client.execute_function.assert_called_once_with(
        function_name=function_name, parameters={"x": "hi"}, enable_retriever_tracing=False
    )


@pytest.mark.parametrize(
    "function_names",
    [
        ["catalog.s.a__b", "catalog.s__a.b"],
        ["x" + LONG_FUNCTION_NAME, "y" + LONG_FUNCTION_NAME],
        ["catalog.sales.a@b", "catalog.sales.a_b"],
    ],
    ids=["underscore-join", "cut-to-63", "replaced-character"],
)
@FUNCTION_INFO_GENERATORS
def test_functions_whose_declared_names_collide_each_run_their_own_function(
    generate, function_names
):
    client = mock_client_for(function_infos_named(generate, function_names))
    toolkit = UCFunctionToolkit(function_names=function_names, client=client)
    declarations = toolkit.generate_callable_tool_list()
    model = GenerativeModel(model_name="gemini-2.0-flash", tools=declarations)
    names = [tool.name for tool in toolkit.tools]

    assert len(set(names)) == 2
    assert all(len(name) <= 63 for name in names)
    assert [declaration.name for declaration in declarations] == names
    for function_name, name in zip(function_names, names):
        client.execute_function.reset_mock()
        response = GenerateContentResponse.from_response(function_call_response(name, {"x": "hi"}))
        generate_tool_call_messages(model=model, response=response, conversation_history=[])
        client.execute_function.assert_called_once_with(
            function_name=function_name, parameters={"x": "hi"}, enable_retriever_tracing=False
        )


@FUNCTION_INFO_GENERATORS
def test_same_named_function_infos_are_declared_by_distinct_names(generate):
    client = mock_client_for({})
    tools = [
        UCFunctionToolkit.uc_function_to_gemini_tool(function_info=function_info, client=client)
        for function_info in function_infos_in_schemas(generate, ["sales", "hr"]).values()
    ]

    assert [(tool.name, tool.schema["name"]) for tool in tools] == [
        ("catalog__sales__test", "catalog__sales__test"),
        ("catalog__hr__test", "catalog__hr__test"),
    ]
