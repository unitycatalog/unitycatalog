import json
import os
from typing import Dict, List
from unittest import mock

import openai
import pytest
from databricks.sdk.service.catalog import (
    FunctionInfo,
    FunctionParameterInfo,
    FunctionParameterInfos,
)
from openai.types.chat.chat_completion_message_tool_call import Function

from tests.helper_functions import mock_chat_completion_response, mock_choice
from unitycatalog.ai.core.base import set_uc_function_client
from unitycatalog.ai.core.utils.function_processing_utils import get_tool_name
from unitycatalog.ai.openai.toolkit import UCFunctionToolkit
from unitycatalog.ai.test_utils.client_utils import (
    TEST_IN_DATABRICKS,
    get_client,
    requires_databricks,
    set_default_client,
)
from unitycatalog.ai.test_utils.function_utils import (
    create_function_and_cleanup,
    random_func_name,
)

SCHEMA = os.environ.get("SCHEMA", "ucai_openai_test")


@pytest.fixture(autouse=True)
def env_setup(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "fake-key")


@requires_databricks
def test_tool_calling():
    client = get_client()
    with (
        set_default_client(client),
        create_function_and_cleanup(client, schema=SCHEMA) as func_obj,
    ):
        func_name = func_obj.full_function_name
        toolkit = UCFunctionToolkit(function_names=[func_name])
        tools = toolkit.tools
        assert len(tools) == 1

        messages = [
            {
                "role": "system",
                "content": "You are a helpful customer support assistant. Use the supplied tools to assist the user.",
            },
            {"role": "user", "content": "What is the result of 2**10?"},
        ]

        with mock.patch(
            "openai.chat.completions.create",
            return_value=mock_chat_completion_response(
                function=Function(
                    arguments='{"code":"result = 2**10\\nprint(result)"}',
                    name=func_obj.tool_name,
                ),
            ),
        ):
            response = openai.chat.completions.create(
                model="gpt-4o-mini",
                messages=messages,
                tools=tools,
            )
            tool_calls = response.choices[0].message.tool_calls
            assert len(tool_calls) == 1
            tool_call = tool_calls[0]
            assert tool_call.function.name == func_obj.tool_name
            arguments = json.loads(tool_call.function.arguments)
            assert isinstance(arguments.get("code"), str)

            # execute the function based on the arguments
            result = client.execute_function(func_name, arguments)
            assert result.value == "1024\n"

            # Create a message containing the result of the function call
            function_call_result_message = {
                "role": "tool",
                "content": json.dumps({"content": result.value}),
                "tool_call_id": tool_call.id,
            }
            assistant_message = response.choices[0].message.to_dict()
            completion_payload = {
                "model": "gpt-4o-mini",
                "messages": [*messages, assistant_message, function_call_result_message],
            }
            # Generate final response
            openai.chat.completions.create(
                model=completion_payload["model"], messages=completion_payload["messages"]
            )


@requires_databricks
def test_tool_calling_with_trace_as_retriever():
    client = get_client()
    import mlflow

    if TEST_IN_DATABRICKS:
        import mlflow.tracking._model_registry.utils

        mlflow.tracking._model_registry.utils._get_registry_uri_from_spark_session = (
            lambda: "databricks-uc"
        )
    with (
        set_default_client(client),
        create_function_and_cleanup(client, schema=SCHEMA) as func_obj,
    ):
        func_name = func_obj.full_function_name
        toolkit = UCFunctionToolkit(function_names=[func_name])
        tools = toolkit.tools

        messages = [
            {
                "role": "system",
                "content": "You are a helpful customer support assistant. Use the supplied tools to assist the user.",
            },
            {"role": "user", "content": "What is Databricks?"},
        ]

        with mock.patch(
            "openai.chat.completions.create",
            return_value=mock_chat_completion_response(
                function=Function(
                    arguments=json.dumps(
                        {"code": "print([{'page_content': 'This is the page content.'}], end='')"}
                    ),
                    name=func_obj.tool_name,
                ),
            ),
        ):
            response = openai.chat.completions.create(
                model="gpt-4o-mini",
                messages=messages,
                tools=tools,
            )
            tool_calls = response.choices[0].message.tool_calls
            tool_call = tool_calls[0]
            arguments = json.loads(tool_call.function.arguments)

            # execute the function based on the arguments
            result = client.execute_function(func_name, arguments, enable_retriever_tracing=True)
            assert result.value == "[{'page_content': 'This is the page content.'}]"

            trace = mlflow.get_last_active_trace()
            assert trace is not None
            assert trace.data.spans[0].name == func_name
            assert trace.info.execution_time_ms is not None
            assert trace.data.request == tool_call.function.arguments
            assert trace.data.response == result.value.replace("'", '"')


@requires_databricks
def test_tool_calling_with_multiple_choices():
    client = get_client()
    with (
        set_default_client(client),
        create_function_and_cleanup(client, schema=SCHEMA) as func_obj,
    ):
        func_name = func_obj.full_function_name
        toolkit = UCFunctionToolkit(function_names=[func_name])
        tools = toolkit.tools
        assert len(tools) == 1

        messages = [
            {
                "role": "system",
                "content": "You are a helpful customer support assistant. Use the supplied tools to assist the user.",
            },
            {"role": "user", "content": "What is the result of 2**10?"},
        ]

        function = Function(
            arguments='{"code":"result = 2**10\\nprint(result)"}',
            name=func_obj.tool_name,
        )
        with mock.patch(
            "openai.chat.completions.create",
            return_value=mock_chat_completion_response(
                choices=[mock_choice(function), mock_choice(function), mock_choice(function)],
            ),
        ):
            response = openai.chat.completions.create(
                model="gpt-4o-mini",
                messages=messages,
                tools=tools,
                n=3,
            )
            choices = response.choices
            assert len(choices) == 3
            # only choose one of the choices
            tool_calls = choices[0].message.tool_calls
            assert len(tool_calls) == 1

            tool_call = tool_calls[0]
            assert tool_call.function.name == func_obj.tool_name
            arguments = json.loads(tool_call.function.arguments)
            assert isinstance(arguments.get("code"), str)

            # execute the function based on the arguments
            result = client.execute_function(func_name, arguments)
            assert result.value == "1024\n"

            # Create a message containing the result of the function call
            function_call_result_message = {
                "role": "tool",
                "content": json.dumps({"content": result.value}),
                "tool_call_id": tool_call.id,
            }
            assistant_message = response.choices[0].message.to_dict()
            completion_payload = {
                "model": "gpt-4o-mini",
                "messages": [*messages, assistant_message, function_call_result_message],
            }
            # Generate final response
            openai.chat.completions.create(
                model=completion_payload["model"], messages=completion_payload["messages"]
            )


@requires_databricks
def test_tool_calling_work_with_non_json_schema():
    func_name = random_func_name(schema=SCHEMA)
    function_name = func_name.split(".")[-1]
    sql_body = f"""CREATE FUNCTION {func_name}(start DATE, end DATE)
RETURNS TABLE(day_of_week STRING, day DATE)
COMMENT 'Calculate the weekdays between start and end, return as a table'
RETURN SELECT extract(DAYOFWEEK_ISO FROM day), day
            FROM (SELECT sequence({function_name}.start, {function_name}.end)) AS T(days)
                LATERAL VIEW explode(days) AS day
            WHERE extract(DAYOFWEEK_ISO FROM day) BETWEEN 1 AND 5;
"""

    client = get_client()
    with (
        set_default_client(client),
        create_function_and_cleanup(
            client, func_name=func_name, sql_body=sql_body, schema=SCHEMA
        ) as func_obj,
    ):
        toolkit = UCFunctionToolkit(function_names=[func_name], client=client)
        tools = toolkit.tools
        assert len(tools) == 1
        assert tools[0]["function"]["strict"] is False

        messages = [
            {
                "role": "system",
                "content": "You are a helpful customer support assistant. Use the supplied tools to assist the user.",
            },
            {"role": "user", "content": "What are the weekdays between 2024-01-01 and 2024-01-07?"},
        ]

        with mock.patch(
            "openai.chat.completions.create",
            return_value=mock_chat_completion_response(
                function=Function(
                    arguments='{"start":"2024-01-01","end":"2024-01-07"}',
                    name=func_obj.tool_name,
                ),
            ),
        ):
            response = openai.chat.completions.create(
                model="gpt-4o-mini",
                messages=messages,
                tools=tools,
            )
            tool_calls = response.choices[0].message.tool_calls
            assert len(tool_calls) == 1
            tool_call = tool_calls[0]
            assert tool_call.function.name == func_obj.tool_name
            arguments = json.loads(tool_call.function.arguments)
            assert isinstance(arguments.get("start"), str)
            assert isinstance(arguments.get("end"), str)

            # execute the function based on the arguments
            result = client.execute_function(func_name, arguments)
            assert result.value is not None

            # Create a message containing the result of the function call
            function_call_result_message = {
                "role": "tool",
                "content": json.dumps({"content": result.value}),
                "tool_call_id": tool_call.id,
            }
            assistant_message = response.choices[0].message.to_dict()
            completion_payload = {
                "model": "gpt-4o-mini",
                "messages": [*messages, assistant_message, function_call_result_message],
            }
            # Generate final response
            openai.chat.completions.create(
                model=completion_payload["model"], messages=completion_payload["messages"]
            )


@requires_databricks
def test_tool_choice_param():
    cap_func = random_func_name(schema=SCHEMA)
    sql_body1 = f"""CREATE FUNCTION {cap_func}(s STRING)
RETURNS STRING
COMMENT 'Capitalizes the input string'
LANGUAGE PYTHON
AS $$
  return s.capitalize()
$$
"""
    upper_func = random_func_name(schema=SCHEMA)
    sql_body2 = f"""CREATE FUNCTION {upper_func}(s STRING)
RETURNS STRING
COMMENT 'Uppercases the input string'
LANGUAGE PYTHON
AS $$
  return s.upper()
$$
"""
    client = get_client()
    with (
        create_function_and_cleanup(
            client, func_name=cap_func, sql_body=sql_body1, schema=SCHEMA
        ) as cap_func_obj,
        create_function_and_cleanup(
            client, func_name=upper_func, sql_body=sql_body2, schema=SCHEMA
        ) as upper_func_obj,
    ):
        toolkit = UCFunctionToolkit(function_names=[cap_func, upper_func], client=client)
        tools = toolkit.tools
        assert len(tools) == 2

        messages = [
            {
                "role": "system",
                "content": "You are a helpful customer support assistant. Use the supplied tools to assist the user.",
            },
            {"role": "user", "content": "What's the result after capitalize 'abc'?"},
        ]

        with mock.patch(
            "openai.chat.completions.create",
            return_value=mock_chat_completion_response(
                function=Function(
                    arguments='{"s":"abc"}',
                    name=cap_func_obj.tool_name,
                ),
            ),
        ):
            response = openai.chat.completions.create(
                model="gpt-4o-mini",
                messages=messages,
                tools=tools,
                tool_choice="required",
            )
        tool_calls = response.choices[0].message.tool_calls
        assert len(tool_calls) == 1
        tool_call = tool_calls[0]
        assert tool_call.function.name == cap_func_obj.tool_name
        arguments = json.loads(tool_call.function.arguments)
        result = client.execute_function(cap_func, arguments)
        assert result.value == "Abc"

        messages = [
            {
                "role": "system",
                "content": "You are a helpful customer support assistant. Use the supplied tools to assist the user.",
            },
            {"role": "user", "content": "What's the result after uppercase 'abc'?"},
        ]
        with mock.patch(
            "openai.chat.completions.create",
            return_value=mock_chat_completion_response(
                function=Function(
                    arguments='{"s":"abc"}',
                    name=upper_func_obj.tool_name,
                ),
            ),
        ):
            response = openai.chat.completions.create(
                model="gpt-4o-mini",
                messages=messages,
                tools=tools,
                tool_choice={"type": "function", "function": {"name": upper_func_obj.tool_name}},
            )
        tool_calls = response.choices[0].message.tool_calls
        assert len(tool_calls) == 1
        tool_call = tool_calls[0]
        assert tool_call.function.name == upper_func_obj.tool_name
        arguments = json.loads(tool_call.function.arguments)
        result = client.execute_function(upper_func, arguments)
        assert result.value == "ABC"


def test_openai_toolkit_initialization():
    client = get_client()
    with pytest.raises(
        ValueError,
        match=r"No client provided, either set the client when creating a toolkit or set the default client",
    ):
        toolkit = UCFunctionToolkit(function_names=[])

    set_uc_function_client(client)
    toolkit = UCFunctionToolkit(function_names=[])
    assert len(toolkit.tools) == 0
    set_uc_function_client(None)

    toolkit = UCFunctionToolkit(function_names=[], client=client)
    assert len(toolkit.tools) == 0


def generate_function_info(parameters: List[Dict], catalog="catalog", schema="schema"):
    return FunctionInfo(
        catalog_name=catalog,
        schema_name=schema,
        name="test",
        input_params=FunctionParameterInfos(
            parameters=[FunctionParameterInfo(**param) for param in parameters]
        ),
        full_name=f"{catalog}.{schema}.test",
        comment="Executes Python code and returns its stdout.",
    )


def test_function_definition_generation():
    client = get_client()
    with set_default_client(client):
        function_info = generate_function_info(
            [
                {
                    "name": "code",
                    "type_text": "string",
                    "type_json": '{"name":"code","type":"string","nullable":true,"metadata":{"comment":"Python code to execute. Remember to print the final result to stdout."}}',
                    "type_name": "STRING",
                    "type_precision": 0,
                    "type_scale": 0,
                    "position": 0,
                    "parameter_type": "PARAM",
                    "comment": "Python code to execute. Remember to print the final result to stdout.",
                }
            ]
        )

        with mock.patch(
            "unitycatalog.ai.core.databricks.DatabricksFunctionClient.get_function",
            return_value=function_info,
        ):
            function_definition = UCFunctionToolkit.uc_function_to_openai_function_definition(
                function_name=function_info.full_name
            )
        assert function_definition == {
            "type": "function",
            "function": {
                "name": get_tool_name(function_info.full_name),
                "description": function_info.comment,
                "strict": True,
                "parameters": {
                    "properties": {
                        "code": {
                            "anyOf": [{"type": "string"}, {"type": "null"}],
                            "description": "Python code to execute. Remember to print the final result to stdout.",
                            "title": "Code",
                        }
                    },
                    "title": get_tool_name(function_info.full_name) + "__params",
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["code"],
                },
            },
        }
