import os
from unittest import mock

import pytest
import pytest_asyncio
from anthropic import Anthropic
from anthropic.types import Message, TextBlock, ToolUseBlock

from unitycatalog.ai.anthropic.toolkit import UCFunctionToolkit
from unitycatalog.ai.core.base import set_uc_function_client
from unitycatalog.ai.core.client import (
    ExecutionMode,
    UnitycatalogFunctionClient,
)
from unitycatalog.ai.core.utils.function_processing_utils import get_tool_name
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

try:
    # v2
    from pydantic_core._pydantic_core import ValidationError
except ImportError:
    # v1
    from pydantic.error_wrappers import ValidationError


SCHEMA = os.environ.get("SCHEMA", "ucai_core_test")


def mock_anthropic_tool_response(function_name, input_data, message_id):
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


def simple_math(a: int, b: int) -> int:
    """
    Adds two numbers.

    Args:
        a: First number.
        b: Second number.

    Returns:
        The sum of a and b.
    """
    return a + b


def additional_math(a: int, b: int) -> int:
    """
    Multiplies two numbers

    Args:
        a: First number.
        b: Second number.

    Returns:
        The product of a and b.
    """
    return a * b


@pytest_asyncio.fixture
async def uc_client():
    config = Configuration()
    config.host = "http://localhost:8080/api/2.1/unity-catalog"
    uc_api_client = ApiClient(configuration=config)

    uc_client = UnitycatalogFunctionClient(api_client=uc_api_client)
    uc_client.uc.create_catalog(name=CATALOG)
    uc_client.uc.create_schema(name=SCHEMA, catalog_name=CATALOG)

    yield uc_client

    uc_client.close()
    await uc_api_client.close()


def test_toolkit_creation_errors_no_client(monkeypatch):
    monkeypatch.setattr("unitycatalog.ai.core.base._is_databricks_client_available", lambda: False)

    with pytest.raises(
        ValidationError,
        match=r"No client provided, either set the client when creating a toolkit or set the default client",
    ):
        UCFunctionToolkit(function_names=["test.test.test"])


@pytest.mark.parametrize("execution_mode", ["local", "sandbox"])
@pytest.mark.asyncio
async def test_tool_calling_with_anthropic(uc_client, execution_mode):
    uc_client.execution_mode = ExecutionMode(execution_mode)
    with create_function_and_cleanup_oss(
        uc_client, schema=SCHEMA, callable=simple_math
    ) as func_obj:
        func_name = func_obj.full_function_name
        toolkit = UCFunctionToolkit(function_names=[func_name], client=uc_client)
        tools = toolkit.tools
        assert len(tools) == 1

        messages = [
            {
                "role": "user",
                "content": "What is the sum of 2 and 3?",
            },
        ]

        converted_func_name = get_tool_name(func_name)

        with mock.patch("anthropic.resources.messages.Messages.create") as mock_create:
            mock_create.return_value = mock_anthropic_tool_response(
                function_name=converted_func_name,
                input_data={"a": 2, "b": 3},
                message_id="msg_01H6Y3Z0XYZ123456789",
            )

            response = Anthropic().messages.create(
                model="claude-3-5-sonnet-20240620", messages=messages, tools=tools, max_tokens=512
            )

            tool_calls = response.content
            assert len(tool_calls) == 2
            assert tool_calls[1].name == converted_func_name
            arguments = tool_calls[1].input
            assert isinstance(arguments.get("a"), int)
            assert isinstance(arguments.get("b"), int)

            result = uc_client.execute_function(func_name, arguments)
            assert result.value.strip() == "5"

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
                        TextBlock(text="The sum of 2 and 3 is 5", type="text"),
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

                assert final_response.content[0].text == "The sum of 2 and 3 is 5"


@pytest.mark.parametrize("execution_mode", ["local", "sandbox"])
@pytest.mark.asyncio
async def test_tool_calling_with_multiple_tools_anthropic(uc_client, execution_mode):
    uc_client.execution_mode = ExecutionMode(execution_mode)
    with (
        create_function_and_cleanup_oss(
            uc_client, schema=SCHEMA, callable=simple_math
        ) as func_obj_1,
        create_function_and_cleanup_oss(
            uc_client, schema=SCHEMA, callable=additional_math
        ) as func_obj_2,
    ):
        func_name_1 = func_obj_1.full_function_name
        func_name_2 = func_obj_2.full_function_name
        toolkit = UCFunctionToolkit(function_names=[func_name_1, func_name_2], client=uc_client)
        tools = toolkit.tools
        assert len(tools) == 2

        messages = [
            {
                "role": "user",
                "content": "What is the sum of 2 and 3 and the product of 4 and 5?",
            },
        ]

        converted_func_name_1 = get_tool_name(func_name_1)
        converted_func_name_2 = get_tool_name(func_name_2)

        with mock.patch("anthropic.resources.messages.Messages.create") as mock_create_first:
            mock_create_first.return_value = mock_anthropic_tool_response(
                function_name=converted_func_name_1,
                input_data={"a": 2, "b": 3},
                message_id="msg_01H6Y3Z0XYZ123456789",
            )

            response = Anthropic().messages.create(
                model="claude-3-5-sonnet-20240620", messages=messages, tools=tools, max_tokens=512
            )

            tool_calls = response.content
            assert len(tool_calls) == 2
            assert tool_calls[1].name == converted_func_name_1
            arguments = tool_calls[1].input
            assert isinstance(arguments.get("a"), int)
            assert isinstance(arguments.get("b"), int)

            result = uc_client.execute_function(func_name_1, arguments)
            assert result.value.strip() == "5"

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

            with mock.patch("anthropic.resources.messages.Messages.create") as mock_create_second:
                mock_create_second.return_value = mock_anthropic_tool_response(
                    function_name=converted_func_name_2,
                    input_data={"a": 4, "b": 5},
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
                assert final_tool_calls[1].name == converted_func_name_2
                arguments_second = final_tool_calls[1].input
                assert isinstance(arguments_second.get("a"), int)
                assert isinstance(arguments_second.get("b"), int)

                result_second = uc_client.execute_function(func_name_2, arguments_second)

                assert result_second.value.strip() == "20"

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
                                text="The sum of 2 and 3 is 5 and the product of 4 and 5 is 20",
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
                        == "The sum of 2 and 3 is 5 and the product of 4 and 5 is 20"
                    )


@pytest.mark.asyncio
async def test_anthropic_toolkit_initialization(uc_client):
    with (
        mock.patch(
            "unitycatalog.ai.core.base._is_databricks_client_available",
            return_value=False,
        ),
        pytest.raises(
            ValidationError,
            match=r"No client provided, either set the client when creating a toolkit or set the default client",
        ),
    ):
        toolkit = UCFunctionToolkit(function_names=[])

    set_uc_function_client(uc_client)
    toolkit = UCFunctionToolkit(function_names=[])
    assert len(toolkit.tools) == 0
    set_uc_function_client(None)

    toolkit = UCFunctionToolkit(function_names=[], client=uc_client)
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


@pytest.mark.asyncio
async def test_anthropic_tool_definition_generation(uc_client):
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
        "unitycatalog.ai.core.client.UnitycatalogFunctionClient.get_function",
        return_value=function_info,
    ):
        function_definition = UCFunctionToolkit(client=uc_client).uc_function_to_anthropic_tool(
            function_name=function_info.full_name, client=uc_client
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
