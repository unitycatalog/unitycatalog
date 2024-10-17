import json
from typing import Any, Dict, Iterator

from langchain_core.messages import (
    AIMessage,
    HumanMessage,
    MessageLikeRepresentation,
    ToolMessage,
)


def stringify_tool_call(tool_call: Dict[str, Any]) -> str:
    return json.dumps(
        {
            "id": tool_call.get("id"),
            "name": tool_call.get("name"),
            "arguments": str(tool_call.get("args", {})),
        },
        indent=2,
    )


def stringify_tool_result(tool_msg: ToolMessage) -> str:
    return json.dumps({"id": tool_msg.tool_call_id, "content": tool_msg.content}, indent=2)


def parse_message(msg) -> str:
    if isinstance(msg, ToolMessage):
        return stringify_tool_result(msg)
    elif isinstance(msg, AIMessage) and msg.tool_calls:
        tool_call_results = [stringify_tool_call(call) for call in msg.tool_calls]
        return "".join(tool_call_results)
    elif isinstance(msg, (AIMessage, HumanMessage)):
        return msg.content
    else:
        return str(msg)


def wrap_output(stream: Iterator[MessageLikeRepresentation]) -> Iterator[str]:
    for event in stream:
        # the agent was called with invoke()
        if "messages" in event:
            for msg in event["messages"]:
                yield parse_message(msg) + "\n\n"
