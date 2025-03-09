import logging
from typing import Any, Dict, List

logger = logging.getLogger(__name__)


def extract_response_details(response: Dict[str, Any]) -> Dict[str, Any]:
    """Extracts returnControl or chunks from Bedrock response"""
    response_details = {"chunks": "", "tool_calls": []}

    for event in response.get("completion", []):
        try:
            if "chunk" in event:
                chunk = event["chunk"].get("bytes", b"").decode("utf-8")
                if chunk:
                    response_details["chunks"] += chunk

            if "returnControl" in event:
                response_details["tool_calls"].extend(extract_tool_calls_from_event(event))
        except Exception as e:
            logger.error(f"Error processing event: {e}")
    return response_details


def extract_tool_calls_from_event(event: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Extracts tool calls from a single event."""
    tool_calls = []
    if "returnControl" in event:
        control_data = event["returnControl"]
        for invocation in control_data.get("invocationInputs", []):
            if "functionInvocationInput" in invocation:
                func_input = invocation["functionInvocationInput"]
                action_group = func_input["actionGroup"]
                function = func_input["function"]
                tool_calls.append(
                    {
                        "action_group": action_group,
                        "function": function,
                        "function_name": f"{action_group}__{function}",
                        "parameters": {p["name"]: p["value"] for p in func_input["parameters"]},
                        "invocation_id": control_data["invocationId"],
                    }
                )
    return tool_calls


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
) -> List[Dict[str, Any]]:
    """Execute tool calls and return results."""
    results = []
    for tool_call in tool_calls:
        try:
            full_function_name = f"{catalog_name}.{schema_name}.{function_name}"
            logger.info(f"Full Function Name: {full_function_name}")
            function_info = client.get_function(full_function_name)
            logger.info(f"Retrieved function info: {function_info}")

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
