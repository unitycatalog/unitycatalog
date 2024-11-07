import json
from typing import Any, Optional, Union

from litellm.types.utils import ChatCompletionMessageToolCall, Choices, Message
from pydantic import BaseModel, Field, ValidationError

from unitycatalog.ai.core.client import BaseFunctionClient
from unitycatalog.ai.core.utils.client_utils import validate_or_set_default_client
from unitycatalog.ai.core.utils.function_processing_utils import construct_original_function_name


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

    def __init__(self, function_name: str, arguments: dict[str, Any], tool_use_id: str):
        self.function_name = function_name
        self.arguments = arguments
        self.tool_use_id = tool_use_id

    def to_dict(self) -> dict[str, Any]:
        """
        Converts the ToolCallData instance into a dictionary.

        Returns:
            dict[str, Any]: A dictionary representing the ToolCallData instance.
        """
        return {
            "function_name": self.function_name,
            "arguments": self.arguments,
            "tool_use_id": self.tool_use_id,
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
            "tool_call_id": self.tool_use_id,
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
                            tool_use_id=tool_call.id,
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
        List[List[ToolCallData]]: A list of ToolCallData objects containing function names,
        arguments, and tool use IDs.
    """
    if choices := response.choices:
        return [_extract_tool_call_data_from_choice(choice) for choice in choices]

    return []


def generate_tool_call_messages(
    *,
    response: Message,
    conversation_history: Union[dict[str, Any], list[dict[str, Any]]],
    client: Optional[BaseFunctionClient] = None,
    choice_index: int = 0,
) -> list[dict[str, Any]]:
    """
    Generate tool call messages from the response.

    Note:
        This function relies on that the UC function names don't contain '__' in the catalog, schema
        or function names, and the total length of the function name is less than 64 characters.
        Otherwise, the original function name is not guaranteed to be correctly reconstructed.

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
    # TODO: validate that this is how function calls should be passed for mulitple tools
    return [*validated_history, assistant_message, *function_calls]


# def generate_tool_call_messages(
#     *,
#     response: Message,
#     conversation_history: Union[dict[str, Any], list[dict[str, Any]]],
#     client: Optional[BaseFunctionClient] = None,
# ) -> list[dict[str, Any]]:
#     """
#     Generates tool call messages based on the response from LiteLLM and conversation history.

#     Args:
#         response (Message): The response from LiteLLM.
#         conversation_history (Union[dict[str, Any], list[dict[str, Any]]]): The history of the conversation to prepend.
#         client (Optional[BaseFunctionClient]): The client for executing Unity Catalog functions.

#     Returns:
#         List[Dict[str, Any]]: A list of messages including tool execution results and conversation history.
#     """
#     client = validate_or_set_default_client(client)

#     if isinstance(conversation_history, dict):
#         conversation_history = [conversation_history]

#     validated_history = []
#     try:
#         validated_history = [
#             ConversationMessage(**message).model_dump() for message in conversation_history
#         ]
#     except ValidationError as e:
#         raise ValueError("Invalid conversation history format") from e

#     tool_calls_data = extract_tool_call_data(response)

#     # NB: if multiple choices are returned, we pick the first list of tool calls to execute
#     # and append to message history.
#     # TODO: validate this is the logic we want
#     chosen_tools = tool_calls_data[0]
#     response_message = response.choices[0].message

#     # TODO: currently there isn't any logic to add tool info to the message history. See if we want
#     # to add this in.
#     assistant_message = {"role": response_message.role, "content": response_message.content}

#     function_call_messages = []
#     if tool_calls_data:
#         for tool_call_data in chosen_tools:
#             result = tool_call_data.execute(client)
#             tool_result = tool_call_data.to_tool_result_message(result)
#             message = {"role": "user", "content": tool_result}
#             function_call_messages.append(message)

#     return [*validated_history, assistant_message, *function_call_messages]
