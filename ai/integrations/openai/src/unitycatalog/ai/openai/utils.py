import json
from typing import Any, Dict, List, Optional

from openai.types.chat.chat_completion import ChatCompletion
from unitycatalog.ai.core.base import BaseFunctionClient
from unitycatalog.ai.core.utils.client_utils import validate_or_set_default_client
from unitycatalog.ai.core.utils.function_processing_utils import construct_original_function_name
from unitycatalog.ai.core.utils.validation_utils import mlflow_tracing_enabled


# TODO: support async
def generate_tool_call_messages(
    *, response: ChatCompletion, client: Optional[BaseFunctionClient] = None, choice_index: int = 0
) -> List[Dict[str, Any]]:
    """
    Generate tool call messages from the response.

    Note:
        This function relies on that the UC function names don't contain '__' in the catalog, schema
        or function names, and the total length of the function name is less than 64 characters.
        Otherwise, the original function name is not guaranteed to be correctly reconstructed.

    Args:
        response: The chat completion response object returned by the OpenAI API.
        client: The client for managing functions, must be an instance of BaseFunctionClient.
            Defaults to None.
        choice_index: The index of the choice to process. Defaults to 0. Note that multiple
            choices are not supported yet.

    Returns:
        A list of messages containing the assistant message and the function call results.
    """
    client = validate_or_set_default_client(client)
    message = response.choices[choice_index].message
    tool_calls = message.tool_calls
    function_calls = []
    if tool_calls:
        for tool_call in tool_calls:
            arguments = json.loads(tool_call.function.arguments)
            func_name = construct_original_function_name(tool_call.function.name)
            result = client.execute_function(
                func_name, arguments, enable_retriever_tracing=mlflow_tracing_enabled("openai")
            )
            function_call_result_message = {
                "role": "tool",
                "content": json.dumps({"content": result.value}),
                "tool_call_id": tool_call.id,
            }
            function_calls.append(function_call_result_message)
    assistant_message = message.to_dict()
    return [assistant_message, *function_calls]
