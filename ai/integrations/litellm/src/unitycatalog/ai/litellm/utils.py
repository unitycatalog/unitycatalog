import json
from typing import Any, Optional, Union

from pydantic import BaseModel, Field, ValidationError

from litellm.types.utils import ChatCompletionMessageToolCall, Choices, Message
from unitycatalog.ai.core.client import BaseFunctionClient
from unitycatalog.ai.core.utils.client_utils import validate_or_set_default_client
from unitycatalog.ai.core.utils.function_processing_utils import (
    construct_original_function_name,
)


class ConversationMessage(BaseModel):
    role: str = Field(
        ..., description="The role of the message sender, e.g., 'user' or 'assistant'."
    )
    content: str = Field(
        ...,
        description="The content of the message, e.g. original user question or an assistant response.",
    )

    def to_dict(self) -> dict[str, Any]:
        return {
            "role": self.role,
            "content": self.content,
        }


class ToolCallData:
    """
    Class representing a tool call, encapsulating the function name, arguments, and tool use ID.
    """

    def __init__(self, function_name: str, arguments: dict[str, Any], tool_call_id: str):
        self.function_name = function_name
        self.arguments = arguments
        self.tool_call_id = tool_call_id

    def to_dict(self) -> dict[str, Any]:
        """
        Converts the ToolCallData instance into a dictionary.

        Returns:
            dict[str, Any]: A dictionary representing the ToolCallData instance.
        """
        return {
            "function_name": self.function_name,
            "arguments": self.arguments,
            "tool_call_id": self.tool_call_id,
        }

    def execute(self, client: BaseFunctionClient) -> str:
        """
        Executes the Unity Catalog function using the provided client.

        Args:
            client (BaseFunctionClient): The client to use for executing the function.

        Returns:
            str: The result of the function execution.
        """
        result = client.execute_function(self.function_name, self.arguments)
        return str(result.value)

    def to_tool_result_message(self, result: str) -> dict[str, Any]:
        """
        Creates a tool result message based on the result of the function execution.

        Args:
            result (str): The result of the tool execution.

        Returns:
            dict[str, Any]: A dictionary representing the tool result message.
        """
        return {
            "role": "tool",
            "tool_call_id": self.tool_call_id,
            "name": self.function_name,
            "content": result,
        }


def _extract_tool_call_data_from_choice(choice: Choices) -> list[ToolCallData]:
    tool_calls_data = []

    if choice.finish_reason == "tool_calls":
        if choice.message and (tool_calls := choice.message.tool_calls):
            for tool_call in tool_calls:
                if isinstance(tool_call, ChatCompletionMessageToolCall):
                    tool_calls_data.append(
                        ToolCallData(
                            function_name=construct_original_function_name(tool_call.function.name),
                            arguments=json.loads(tool_call.function.arguments),
                            tool_call_id=tool_call.id,
                        )
                    )

    return tool_calls_data


def extract_tool_call_data(response: Message) -> list[list[ToolCallData]]:
    """
    Extracts the tool call information from the LiteLLM response. Note there can be multiple tool
    choices for a single LiteLLM Message, and for each choice there can be multiple tool calls.

    Args:
        response (Message): The response from LiteLLM optionally containing tool usage blocks.

    Returns:
        list[list[ToolCallData]]: A list of ToolCallData objects containing function names,
        arguments, and tool use IDs.
    """
    return [_extract_tool_call_data_from_choice(choice) for choice in response.choices or []]


def generate_tool_call_messages(
    *,
    response: Message,
    conversation_history: Union[dict[str, Any], list[dict[str, Any]]],
    client: Optional[BaseFunctionClient] = None,
    choice_index: int = 0,
) -> list[dict[str, Any]]:
    """
    Generate tool call messages from the response.

    If there are multiple tool calls in the selected Choice, each tool's function call response will
    be appended to the messages as a unique message. For instance, if there are 2 tool calls
    requested by the LiteLLM response, we will append the following payload to the
    `conversation_history` and `response` list.

    [
        {"role", "user": "content": "function_1_response_as_a_string"},
        {"role", "user": "content": "function_2_response_as_a_string"}
    ]

    Args:
        response: The chat completion response object returned by the LiteLLM.completion API.
        client: The client for managing functions, must be an instance of BaseFunctionClient.
            Defaults to None.
        conversation_history (Union[dict[str, Any], list[dict[str, Any]]]): The history of the
            conversation to prepend.
        choice_index: The index of the choice to process. Defaults to 0. Note that multiple
            choices are not supported yet.

    Returns:
        A list of messages containing the assistant message and the function call results.
    """
    client = validate_or_set_default_client(client)
    if isinstance(conversation_history, dict):
        conversation_history = [conversation_history]

    validated_history = []
    try:
        validated_history = [
            ConversationMessage(**message).model_dump() for message in conversation_history
        ]
    except ValidationError as e:
        raise ValueError("Invalid conversation history format") from e

    choice = response.choices[choice_index]

    function_calls = []
    for tool_call_data in _extract_tool_call_data_from_choice(choice):
        result = tool_call_data.execute(client)
        function_calls.append(tool_call_data.to_tool_result_message(result))

    assistant_message = choice.message.to_dict()
    return [*validated_history, assistant_message, *function_calls]
