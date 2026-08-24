import logging
from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, Field, ValidationError

from anthropic.types import ContentBlock, Message, MessageParam, ToolUseBlock
from unitycatalog.ai.core.base import BaseFunctionClient
from unitycatalog.ai.core.utils.client_utils import validate_or_set_default_client
from unitycatalog.ai.core.utils.function_processing_utils import construct_original_function_name
from unitycatalog.ai.core.utils.validation_utils import mlflow_tracing_enabled

_logger = logging.getLogger(__name__)


class ConversationMessage(BaseModel):
    role: str = Field(
        ..., description="The role of the message sender, e.g., 'user' or 'assistant'."
    )
    content: Union[str, List[MessageParam], List[ContentBlock]] = Field(
        ...,
        description="The content of the message, whether from an original user question or an assistant response.",
    )


class ToolCallData:
    """
    Class representing a tool call, encapsulating the function name, arguments, and tool use ID.
    """

    def __init__(self, function_name: str, arguments: Dict[str, Any], tool_use_id: str):
        self.function_name = function_name
        self.arguments = arguments
        self.tool_use_id = tool_use_id

    def to_dict(self) -> Dict[str, Any]:
        """
        Converts the ToolCallData instance into a dictionary.

        Returns:
            Dict[str, Any]: A dictionary representing the ToolCallData instance.
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
        result = client.execute_function(
            self.function_name,
            self.arguments,
            enable_retriever_tracing=mlflow_tracing_enabled("anthropic"),
        )
        return str(result.value)

    def to_tool_result_message(self, result: str) -> Dict[str, Any]:
        """
        Creates a tool result message based on the result of the function execution.

        Args:
            result (str): The result of the tool execution.

        Returns:
            Dict[str, Any]: A dictionary representing the tool result message.
        """
        return {"type": "tool_result", "tool_use_id": self.tool_use_id, "content": result}


def extract_tool_call_data(response: Message) -> List[ToolCallData]:
    """
    Extracts the tool call information from the Anthropic response.

    Args:
        response (Message): The response from Anthropic containing tool usage blocks.

    Returns:
        List[ToolCallData]: A list of ToolCallData objects containing function names, arguments, and tool use IDs.
    """
    tool_calls_data = []

    if response.stop_reason == "tool_use":
        tool_calls = [
            tool_use for tool_use in response.content if isinstance(tool_use, ToolUseBlock)
        ]
        if tool_calls:
            for tool_call in tool_calls:
                if not tool_call.id:
                    raise ValueError(
                        "Tool use block is missing an ID and is unable to resolve the tool call."
                    )

                arguments = tool_call.input
                func_name = construct_original_function_name(tool_call.name)

                tool_calls_data.append(
                    ToolCallData(
                        function_name=func_name, arguments=arguments, tool_use_id=tool_call.id
                    )
                )

    return tool_calls_data


def generate_tool_call_messages(
    *,
    response: Message,
    conversation_history: Union[Dict[str, Any], List[Dict[str, Any]]],
    client: Optional[BaseFunctionClient] = None,
) -> List[Dict[str, Any]]:
    """
    Generates tool call messages based on the response from Anthropic and conversation history.

    Args:
        response (Message): The response from Anthropic.
        conversation_history (Union[Dict[str, Any], List[Dict[str, Any]]]): The history of the conversation to prepend.
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
