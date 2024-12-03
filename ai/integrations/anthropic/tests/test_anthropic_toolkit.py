import os
from unittest import mock

import pytest
from anthropic import Anthropic
from anthropic.types import Message, TextBlock, ToolUseBlock
from databricks.sdk.service.catalog import (
    FunctionInfo,
    FunctionParameterInfo,
    FunctionParameterInfos,
)

from unitycatalog.ai.anthropic.toolkit import UCFunctionToolkit
from unitycatalog.ai.core.client import set_uc_function_client
from unitycatalog.ai.core.utils.function_processing_utils import get_tool_name
from unitycatalog.ai.test_utils.client_utils import (
    get_client,
    requires_databricks,
    set_default_client,
)
from unitycatalog.ai.test_utils.function_utils import create_function_and_cleanup

try:
    # v2
    from pydantic_core._pydantic_core import ValidationError
except ImportError:
    # v1
    from pydantic.error_wrappers import ValidationError


SCHEMA = os.environ.get("SCHEMA", "ucai_core_test")


def mock_anthropic_tool_response(function_name, input_data, message_id):
    input_data["code"] = 'print("Hello, World!")'

    return Message(
        id=message_id,
        type="message",
        content=[
            TextBlock(text="Sure, I can execute that code for you.", type="text"),
            ToolUseBlock(
                id="toolu_01A09q90qw90lq917835lq9",
                name=function_name,
                input=input_data,  # Now contains escaped code
                type="tool_use",
            ),
        ],
        role="assistant",
        model="claude-3-5-sonnet-20240620",
        usage={
            "input_tokens": 10,
            "output_tokens": 20,
            "tokens": 30,
            "duration_ms": 100,
        },
    )


@requires_databricks
@pytest.mark.parametrize("use_serverless", [True, False])
def test_tool_calling_with_anthropic(use_serverless, monkeypatch):
    monkeypatch.setenv("USE_SERVERLESS", str(use_serverless))
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
                "role": "user",
                "content": "Please execute the following code: print('Hello, World!')",
            },
        ]

        converted_func_name = get_tool_name(func_name)

        with mock.patch("anthropic.resources.messages.Messages.create") as mock_create:
            mock_create.return_value = mock_anthropic_tool_response(
                function_name=converted_func_name,
                input_data={"code": "print('Hello, World!')"},
                message_id="msg_01H6Y3Z0XYZ123456789",
            )

            response = Anthropic().messages.create(
                model="claude-3-5-sonnet-20240620", messages=messages, tools=tools, max_tokens=512
            )

            tool_calls = response.content
            assert len(tool_calls) == 2
            assert tool_calls[1].name == converted_func_name
            arguments = tool_calls[1].input
            assert isinstance(arguments.get("code"), str)

            result = client.execute_function(func_name, arguments)
            assert result.value.strip() == "Hello, World!"

            function_call_result_message = {
                "role": "user",
                "content": [
                    {
                        "type": "tool_result",
                        "content": result.value,
                        "tool_use_id": tool_calls[1].id,
                    }
                ],
            }

            with mock.patch("anthropic.resources.messages.Messages.create") as mock_create_final:
                mock_create_final.return_value = Message(
                    id="msg_01H6Y3Z0XYZ123456780",
                    type="message",
                    content=[
                        TextBlock(
                            text="The code has been executed. Output:\n\nHello, World!", type="text"
                        ),
                    ],
                    role="assistant",
                    model="claude-3-5-sonnet-20240620",
                    usage={
                        "input_tokens": 15,
                        "output_tokens": 25,
                        "tokens": 40,
                        "duration_ms": 150,
                    },
                )

                final_response = Anthropic().messages.create(
                    model="claude-3-5-sonnet-20240620",
                    messages=[
                        *messages,
                        {"role": "assistant", "content": tool_calls},
                        function_call_result_message,
                    ],
                    tools=tools,
                    max_tokens=200,
                )

                assert (
                    final_response.content[0].text
                    == "The code has been executed. Output:\n\nHello, World!"
                )


@requires_databricks
@pytest.mark.parametrize("use_serverless", [True, False])
def test_tool_calling_with_multiple_tools_anthropic(use_serverless, monkeypatch):
    monkeypatch.setenv("USE_SERVERLESS", str(use_serverless))
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
                "role": "user",
                "content": "Please execute the following code: print('Hello from Paris!') and then print('Hello from New York!')",
            },
        ]

        converted_func_name = get_tool_name(func_name)

        code_paris = "print('Hello from Paris!')"
        with mock.patch("anthropic.resources.messages.Messages.create") as mock_create_first:
            mock_create_first.return_value = mock_anthropic_tool_response(
                function_name=converted_func_name,
                input_data={"code": code_paris},
                message_id="msg_01H6Y3Z0XYZ123456789",
            )

            response = Anthropic().messages.create(
                model="claude-3-5-sonnet-20240620", messages=messages, tools=tools, max_tokens=512
            )

            tool_calls = response.content
            assert len(tool_calls) == 2
            assert tool_calls[1].name == converted_func_name
            arguments = tool_calls[1].input
            assert isinstance(arguments.get("code"), str)

            result = client.execute_function(func_name, arguments)
            assert result.value.strip() == "Hello, World!"

            function_call_result_message = {
                "role": "user",
                "content": [
                    {
                        "type": "tool_result",
                        "content": result.value,
                        "tool_use_id": tool_calls[1].id,
                    }
                ],
            }

            code_new_york = "print('Hello from New York!')"
            with mock.patch("anthropic.resources.messages.Messages.create") as mock_create_second:
                mock_create_second.return_value = mock_anthropic_tool_response(
                    function_name=converted_func_name,
                    input_data={"code": code_new_york},
                    message_id="msg_01H6Y3Z0XYZ123456780",
                )

                messages_second = [
                    *messages,
                    {"role": "assistant", "content": tool_calls},
                    function_call_result_message,
                ]

                response_second = Anthropic().messages.create(
                    model="claude-3-5-sonnet-20240620",
                    messages=messages_second,
                    tools=tools,
                    max_tokens=200,
                )

                final_tool_calls = response_second.content
                assert len(final_tool_calls) == 2
                assert final_tool_calls[1].name == converted_func_name
                arguments_second = final_tool_calls[1].input
                assert isinstance(arguments_second.get("code"), str)

                result_second = client.execute_function(func_name, arguments_second)

                assert result_second.value.strip() == "Hello, World!"

                function_call_result_message_second = {
                    "role": "user",
                    "content": [
                        {
                            "type": "tool_result",
                            "content": result_second.value,
                            "tool_use_id": final_tool_calls[1].id,
                        }
                    ],
                }

                with mock.patch(
                    "anthropic.resources.messages.Messages.create"
                ) as mock_create_final:
                    mock_create_final.return_value = Message(
                        id="msg_01H6Y3Z0XYZ123456781",
                        type="message",
                        content=[
                            TextBlock(
                                text="I've executed both code snippets. Output:\n\nHello, World!\nHello, World!",
                                type="text",
                            ),
                        ],
                        role="assistant",
                        model="claude-3-5-sonnet-20240620",
                        usage={
                            "input_tokens": 15,
                            "output_tokens": 25,
                            "tokens": 40,
                            "duration_ms": 150,
                        },
                    )

                    messages_final = [
                        *messages_second,
                        function_call_result_message_second,
                    ]

                    final_response = Anthropic().messages.create(
                        model="claude-3-5-sonnet-20240620",
                        messages=messages_final,
                        tools=tools,
                        max_tokens=200,
                    )

                    assert (
                        final_response.content[0].text
                        == "I've executed both code snippets. Output:\n\nHello, World!\nHello, World!"
                    )


@pytest.mark.parametrize("use_serverless", [True, False])
def test_anthropic_toolkit_initialization(use_serverless, monkeypatch):
    monkeypatch.setenv("USE_SERVERLESS", str(use_serverless))
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


def generate_function_info(parameters, catalog="catalog", schema="schema"):
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


@pytest.mark.parametrize("use_serverless", [True, False])
def test_anthropic_tool_definition_generation(use_serverless, monkeypatch):
    monkeypatch.setenv("USE_SERVERLESS", str(use_serverless))
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

        function_definition = UCFunctionToolkit.uc_function_to_anthropic_tool(
            function_info=function_info, client=client
        )

        assert function_definition.to_dict() == {
            "name": get_tool_name(function_info.full_name),
            "description": function_info.comment,
            "input_schema": {
                "type": "object",
                "properties": {
                    "code": {
                        "anyOf": [{"type": "string"}, {"type": "null"}],
                        "default": None,
                        "description": "Python code to execute. Remember to print the final result to stdout.",
                        "title": "Code",
                    }
                },
                "required": [],
            },
        }


def test_toolkit_prohibits_wildcard_functions():
    client = get_client()
    with pytest.raises(
        ValidationError, match=r"Function names with wildcard characters '\*' are not supported"
    ):
        UCFunctionToolkit(client=client, function_names=["catalog.schema.*"])
