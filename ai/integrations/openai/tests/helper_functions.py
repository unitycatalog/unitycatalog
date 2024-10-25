from typing import List, Optional

from openai.types.chat.chat_completion import (
    ChatCompletion,
    ChatCompletionMessage,
    Choice,
)
from openai.types.chat.chat_completion_message import ChatCompletionMessageToolCall
from openai.types.chat.chat_completion_message_tool_call import Function
from openai.types.completion_usage import CompletionTokensDetails, CompletionUsage


def mock_chat_completion_response(
    *, function: Optional[Function] = None, choices: Optional[List[Choice]] = None
):
    return ChatCompletion(
        id="chatcmpl-mock",
        choices=choices or [mock_choice(function)],
        created=1727076144,
        model="gpt-4o-mini-2024-07-18",
        object="chat.completion",
        service_tier=None,
        system_fingerprint="fp_mock",
        usage=CompletionUsage(
            completion_tokens=32,
            prompt_tokens=116,
            total_tokens=148,
            completion_tokens_details=CompletionTokensDetails(reasoning_tokens=0),
        ),
    )


def mock_choice(function: Function):
    return Choice(
        finish_reason="tool_calls",
        index=0,
        logprobs=None,
        message=ChatCompletionMessage(
            content=None,
            refusal=None,
            role="assistant",
            function_call=None,
            tool_calls=[
                ChatCompletionMessageToolCall(
                    id="call_mock",
                    function=function,
                    type="function",
                )
            ],
        ),
    )
