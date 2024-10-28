from pydantic import BaseModel, Field, ValidationError

from litellm.utils.types import ChatCompletionMessageToolCall, Function, Message
from litellm.types.utils import Choices

from unitycatalog.ai.core.utils.client_utils import validate_or_set_default_client
from unitycatalog.ai.core.client import BaseFunctionClient
from unitycatalog.ai.core.utils.function_processing_utils import construct_original_function_name
from typing import Any, Optional, Union


class ConversationMessage(BaseModel):
    role: str = Field(
        ..., description="The role of the message sender, e.g., 'user' or 'assistant'."
    )
    # TODO: map this to validate content is correct format
    # content: Union[str, list[MessageParam], list[ContentBlock]] = Field(
    #     ...,
    #     description="The content of the message, whether from an original user question or an assistant response.",
    # )


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
        return {"role": "tool", "tool_call_id": self.tool_use_id, "name": self.function_name, "content": result}


def extract_tool_call_data(response: Choices) -> list[ToolCallData]:
    """
    Extracts the tool call information from the LiteLLM response.

    Args:
        response (Message): The response from Anthropic containing tool usage blocks.

    Returns:
        List[ToolCallData]: A list of ToolCallData objects containing function names, arguments, and tool use IDs.
    """
    tool_calls_data = []

    if response.finish_reason == "tool_calls":
        if response.message and response.message.tool_calls: # NOTE this cna probably be removed with validation
            tool_calls = [
                tool for tool in response.message.tool_calls if isinstance(tool, ChatCompletionMessageToolCall)
            ]

        if tool_calls:
            for tool_call in tool_calls:
                tool_calls_data.append(
                    ToolCallData(
                        function_name=construct_original_function_name(tool_call.name),
                        arguments=tool_call.function.arguments,
                        tool_use_id=tool_call.id
                    )
                )

    return tool_calls_data
def generate_tool_call_messages(
    *,
    response: Message,
    conversation_history: Union[dict[str, Any], list[dict[str, Any]]],
    client: Optional[BaseFunctionClient] = None,
) -> list[dict[str, Any]]:
    """
    Generates tool call messages based on the response from LiteLLM and conversation history.

    Args:
        response (Message): The response from LiteLLM.
        conversation_history (Union[dict[str, Any], list[dict[str, Any]]]): The history of the conversation to prepend.
        client (Optional[BaseFunctionClient]): The client for executing Unity Catalog functions.

    Returns:
        List[Dict[str, Any]]: A list of messages including tool execution results and conversation history.
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

    tool_calls_data = extract_tool_call_data(response)

    tool_results = []
    if tool_calls_data:
        for tool_call_data in tool_calls_data:
            result = tool_call_data.execute(client)
            tool_results.append(tool_call_data.to_tool_result_message(result))

    assistant_message_content = [block.to_dict() for block in response.content]
    assistant_message = {"role": response.role, "content": assistant_message_content}

    function_calls = []
    if tool_results:
        function_calls.append({"role": "user", "content": tool_results})

    return [*validated_history, assistant_message, *function_calls]
