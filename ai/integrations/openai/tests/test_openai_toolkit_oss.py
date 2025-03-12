import json
import os
from typing import Dict, List
from unittest import mock

import openai
import pytest
import pytest_asyncio
from openai.types.chat.chat_completion_message_tool_call import Function
from pydantic import ValidationError

from tests.helper_functions import mock_chat_completion_response, mock_choice
from unitycatalog.ai.core.client import (
    ExecutionMode,
    UnitycatalogFunctionClient,
)
from unitycatalog.ai.core.utils.function_processing_utils import get_tool_name
from unitycatalog.ai.openai.toolkit import UCFunctionToolkit
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

SCHEMA = os.environ.get("SCHEMA", "ucai_openai_test")


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


@pytest.mark.parametrize("execution_mode", ["local", "sandbox"])
@pytest.mark.asyncio
async def test_tool_calling(uc_client, execution_mode):
    uc_client.execution_mode = ExecutionMode(execution_mode)
    with (
        create_function_and_cleanup_oss(uc_client, schema=SCHEMA) as func_obj,
    ):
        func_name = func_obj.full_function_name
        toolkit = UCFunctionToolkit(function_names=[func_name], client=uc_client)
        tools = toolkit.tools
        assert len(tools) == 1

        messages = [
            {
                "role": "system",
                "content": "You are a helpful customer support assistant. Use the supplied tools to assist the user.",
            },
            {"role": "user", "content": "What is the sum of 2 and 3?"},
        ]

        with mock.patch(
            "openai.chat.completions.create",
            return_value=mock_chat_completion_response(
                function=Function(
                    arguments='{"a": 2, "b": 3}',
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
            assert isinstance(arguments.get("a"), int)
            assert isinstance(arguments.get("b"), int)

            # execute the function based on the arguments
            result = uc_client.execute_function(func_name, arguments)
            assert result.value == "5"

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


@pytest.mark.parametrize("execution_mode", ["local", "sandbox"])
@pytest.mark.asyncio
async def test_tool_calling_with_multiple_choices(uc_client, execution_mode):
    uc_client.execution_mode = ExecutionMode(execution_mode)
    with (
        create_function_and_cleanup_oss(uc_client, schema=SCHEMA) as func_obj,
    ):
        func_name = func_obj.full_function_name
        toolkit = UCFunctionToolkit(function_names=[func_name], client=uc_client)
        tools = toolkit.tools
        assert len(tools) == 1

        messages = [
            {
                "role": "system",
                "content": "You are a helpful customer support assistant. Use the supplied tools to assist the user.",
            },
            {"role": "user", "content": "What is the sum of 2 and 4?"},
        ]

        function = Function(
            arguments='{"a": 2, "b": 4}',
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
            assert isinstance(arguments.get("a"), int)
            assert isinstance(arguments.get("b"), int)

            # execute the function based on the arguments
            result = uc_client.execute_function(func_name, arguments)
            assert result.value == "6"

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


@pytest.mark.asyncio
async def test_tool_choice_param(uc_client):
    def cap_str(s: str) -> str:
        """
        Capitalizes the input string.

        Args:
            s: The input string to capitalize.
        """
        return s.capitalize()

    def upper_str(s: str) -> str:
        """
        Uppercases the input string.

        Args:
            s: The input string to uppercase.
        """
        return s.upper()

    with (
        create_function_and_cleanup_oss(uc_client, schema=SCHEMA, callable=cap_str) as cap_func_obj,
        create_function_and_cleanup_oss(
            uc_client, schema=SCHEMA, callable=upper_str
        ) as upper_func_obj,
    ):
        toolkit = UCFunctionToolkit(
            function_names=[cap_func_obj.full_function_name, upper_func_obj.full_function_name],
            client=uc_client,
        )
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
        result = uc_client.execute_function(cap_func_obj.full_function_name, arguments)
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
        result = uc_client.execute_function(upper_func_obj.full_function_name, arguments)
        assert result.value == "ABC"


def test_toolkit_creation_errors_no_client(monkeypatch):
    monkeypatch.setattr("unitycatalog.ai.core.base._is_databricks_client_available", lambda: False)

    with pytest.raises(
        ValidationError,
        match=r"No client provided, either set the client when creating a toolkit or set the default client",
    ):
        UCFunctionToolkit(function_names=["test.test.test"])


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


# NB: This test is replicated in ai/integrations/openai/tests/test_openai_toolkit.py
# with the sole exception being the class statements for assembling the FunctionInfo payload
# to ensure that the OSS implementation and the Databricks implementations are compatible with
# the Tookit interface integrations.
@pytest.mark.asyncio
async def test_function_definition_generation(uc_client):
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
        function_definition = UCFunctionToolkit(
            client=uc_client
        ).uc_function_to_openai_function_definition(
            function_name=function_info.full_name, client=uc_client
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
