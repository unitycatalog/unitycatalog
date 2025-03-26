import logging
from typing import Any, Dict, List
import time

logger = logging.getLogger(__name__)


def extract_response_details(response: Dict[str, Any]) -> Dict[str, Any]:
    """Extracts returnControl or chunks from Bedrock response."""
    chunks = []
    tool_calls = []

    for event in response.get("completion", []):
        try:
            chunk = event.get("chunk", {}).get("bytes", b"").decode("utf-8")
            if chunk:
                chunks.append(chunk)

            if event.get("returnControl", {}):
                tool_calls.extend(extract_tool_calls_from_event(event))
        except Exception as e:
            logger.error(f"Error processing event: {e}")

    return {"chunks": "".join(chunks), "tool_calls": tool_calls}


def extract_tool_calls_from_event(event: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Extracts tool calls from a single event."""
    control_data = event.get("returnControl")
    if not control_data:
        return []

    return [
        {
            "action_group": func_input["actionGroup"],
            "function": func_input["function"],
            "function_name": f"{func_input['actionGroup']}__{func_input['function']}",
            "parameters": {p["name"]: p["value"] for p in func_input.get("parameters", [])},
            "invocation_id": control_data["invocationId"],
        }
        for invocation in control_data.get("invocationInputs", [])
        if (func_input := invocation.get("functionInvocationInput"))
    ]


def extract_tool_calls(response: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Extracts tool calls from Bedrock response with support for multiple functions."""
    tool_calls = []
    for event in response.get("completion", []):
        tool_calls.extend(extract_tool_calls_from_event(event))
    return tool_calls


def execute_tool_calls(
    tool_calls: List[Dict[str, Any]],
    client: Any,
    catalog_name: str,
    schema_name: str,
    function_name: str,
) -> List[Dict[str, str]]:
    """Execute tool calls and return results."""
    results = []
    for tool_call in tool_calls:
        try:
            full_function_name = f"{catalog_name}.{schema_name}.{function_name}"
            result = client.execute_function(full_function_name, tool_call["parameters"])
            results.append(
                {"invocation_id": tool_call["invocation_id"], "result": str(result.value)}
            )
        except Exception as e:
            logger.error(f"Error executing tool call: {e}")
            results.append({"invocation_id": tool_call["invocation_id"], "error": str(e)})
    return results


def generate_tool_call_session_state(
    tool_result: Dict[str, Any], tool_call: Dict[str, Any]
) -> Dict[str, Any]:
    """Generate session state for tool call results."""
    return {
        "invocationId": tool_result["invocation_id"],
        "returnControlInvocationResults": [
            {
                "functionResult": {
                    "actionGroup": tool_call["action_group"],
                    "function": tool_call["function"],
                    "confirmationState": "CONFIRM",
                    "responseBody": {"TEXT": {"body": tool_result["result"]}},
                }
            }
        ],
    }


def retry_with_exponential_backoff(
    func, max_retries=10, base_delay=1, backoff_factor=2, retryable_error="Rate limit exceeded"
):
    """
    Retries a function with exponential backoff if a retryable error occurs.

    Args:
        func (callable): The function to retry.
        max_retries (int): Maximum number of retries.
        base_delay (int): Initial delay in seconds.
        backoff_factor (int): Factor by which the delay increases after each retry.
        retryable_error (str): The error message indicating a retryable error.

    Returns:
        Any: The result of the function if successful.

    Raises:
        Exception: If the maximum retries are exceeded or a non-retryable error occurs.
    """
    delay = base_delay
    for attempt in range(1, max_retries + 1):
        try:
            return func()
        except Exception as e:
            error_message = str(e)
            if retryable_error in error_message:
                logger.warning(
                    f"Retryable error encountered. Retrying in {delay} seconds (Attempt {attempt}/{max_retries})..."
                )
                time.sleep(delay)
                delay *= backoff_factor
            else:
                logger.error(f"Non-retryable error encountered: {error_message}")
                raise
    raise Exception(f"Maximum retries ({max_retries}) exceeded.")
