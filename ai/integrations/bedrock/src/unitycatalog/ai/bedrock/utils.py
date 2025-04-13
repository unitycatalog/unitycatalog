import logging
from typing import Any, Dict, List

logger = logging.getLogger(__name__)

# List of errors that should trigger a retry
RETRYABLE_ERRORS = [
    "Rate limit exceeded",
    "throttlingException",
    "Your request rate is too high",
    "TooManyRequestsException",
    "ServiceUnavailable",
    "Throttling",
    "ThrottlingException",
]


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
                {
                    "invocation_id": tool_call["invocation_id"],
                    "result": str(result.value),
                }
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


from pprint import pformat


def pretty_print(obj, indent=2, width=100):
    """
    Pretty-print the dictionary representation of an object.

    Args:
        obj: A dictionary-like object or an object with an `as_dict()` method.
        indent (int): Number of spaces for indentation.
        width (int): Maximum width of each line.

    Returns:
        str: A formatted string representation of the object.
    """
    if hasattr(obj, "as_dict") and callable(obj.as_dict):
        # If the object has an `as_dict()` method, use it
        obj = obj.as_dict()
    elif not isinstance(obj, dict):
        raise ValueError("The provided object must be a dictionary or have an 'as_dict()' method.")

    return pformat(obj, indent=indent, width=width)
