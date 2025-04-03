import logging
import random
import threading
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List

from unitycatalog.ai.bedrock.envs.bedrock_env_vars import BedrockEnvVars

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
            "parameters": {
                p["name"]: p["value"] for p in func_input.get("parameters", [])
            },
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
            result = client.execute_function(
                full_function_name, tool_call["parameters"]
            )
            results.append(
                {
                    "invocation_id": tool_call["invocation_id"],
                    "result": str(result.value),
                }
            )
        except Exception as e:
            logger.error(f"Error executing tool call: {e}")
            results.append(
                {"invocation_id": tool_call["invocation_id"], "error": str(e)}
            )
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


# Track last call times for rate limiting
_last_calls = {}
_lock = threading.RLock()

# Model rate limits (calls per minute)
MODEL_LIMITS = {
    "anthropic.claude-3-5-sonnet-20240620-v1:0": 1  # Account limit: 1 call per minute
}

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


def retry_with_exponential_backoff(
    func,
    max_retries=10,
    base_delay=2,  # Increased base delay
    backoff_factor=2,
    retryable_errors=RETRYABLE_ERRORS,
    model_id=None,
):
    """
    Retries a function with exponential backoff and respects model rate limits.
    Adapts to any rate limit value (calls per minute).

    Args:
        func: Function to execute
        max_retries: Maximum retry attempts
        base_delay: Initial retry delay in seconds
        backoff_factor: Multiplier for delay between retries
        retryable_errors: List of error strings indicating retryable errors
        model_id: Optional model ID for rate limiting

    Returns:
        Result of the function call

    Raises:
        Exception: If max retries exceeded or non-retryable error occurs
    """

    bedrock_env = BedrockEnvVars.get_instance(load_from_file=True)
    if model_id is None:
        model_id = bedrock_env.bedrock_model_id
        if model_id:
            logger.info(f"Using model ID from environment: {model_id}")
        else:
            logger.warning("No model ID found in environment variables")

    # Apply rate limiting if model_id is provided
    if model_id:
        # Get rate limit from environment, with fallback to model-specific limits
        rate_limit = bedrock_env.bedrock_rpm_limit
        if rate_limit is None or rate_limit <= 0:
            # Try to get from model-specific limits
            rate_limit = MODEL_LIMITS.get(model_id, 1)  # Default to 1 if not found
            logger.info(
                f"Using model-specific rate limit: {rate_limit}/minute for {model_id}"
            )
        else:
            logger.info(f"Using environment rate limit: {rate_limit}/minute")

        window_seconds = 120  # Use a 2-minute window for better smoothing

        delay = base_delay

        with _lock:
            now = datetime.now()

            # Get array of recent calls for this model (or create empty one)
            recent_calls = _last_calls.get(model_id, [])

            # Remove calls older than our extended window
            window_ago = now - timedelta(seconds=window_seconds)
            recent_calls = [t for t in recent_calls if t > window_ago]

            # Calculate the effective limit for our window
            effective_limit = int(rate_limit * (window_seconds / 60.0))

            # If we've reached the rate limit in the window, wait
            if len(recent_calls) >= effective_limit:
                # Calculate when the oldest call will expire from our window
                next_available_slot = recent_calls[0] + timedelta(
                    seconds=window_seconds
                )
                wait_seconds = (next_available_slot - now).total_seconds()

                if wait_seconds > 0:
                    # Add extra buffer to be safer (10% extra wait time)
                    buffer_wait = wait_seconds * 1.1
                    logger.info(
                        f"Rate limiting for {model_id}: reached limit of {rate_limit}/minute. "
                        f"Waiting {buffer_wait:.1f}s until next available slot."
                    )
                    time.sleep(buffer_wait)

                    # Update now time after waiting
                    now = datetime.now()

                    # Clean the list again after waiting
                    window_ago = now - timedelta(seconds=window_seconds)
                    recent_calls = [t for t in recent_calls if t > window_ago]

            # Add this call to the history and update the model's entry
            recent_calls.append(now)
            _last_calls[model_id] = recent_calls

            logger.info(
                f"Rate limit status for {model_id}: {len(recent_calls)}/{effective_limit} calls in the {window_seconds}s window"
            )

    # Execute with retry logic
    for attempt in range(1, max_retries + 1):
        try:
            return func()

        except Exception as e:
            error_message = str(
                e
            ).lower()  # Convert to lowercase for case-insensitive matching

            # Check if this is a retryable error by looking for any matching patterns
            is_retryable = any(
                err_pattern.lower() in error_message for err_pattern in retryable_errors
            )

            if not is_retryable:
                logger.error(f"Non-retryable error: {e}")
                raise

            # If we got a throttling error, update our rate limit to be more conservative
            if "throttling" in error_message or "rate" in error_message:
                if model_id and model_id in MODEL_LIMITS:
                    old_limit = MODEL_LIMITS[model_id]
                    # Make the limit more conservative (reduce by 50%)
                    new_limit = max(1, int(old_limit * 0.5))  # Ensure minimum of 1
                    MODEL_LIMITS[model_id] = new_limit
                    logger.warning(
                        f"Throttling detected for {model_id}. "
                        f"Reducing rate limit from {old_limit} to {new_limit} calls/minute."
                    )

            # Check if we've reached max retries
            if attempt >= max_retries:
                logger.error(f"Max retries ({max_retries}) reached. Last error: {e}")
                raise

            # Calculate backoff time with increasing delay
            current_delay = base_delay * (backoff_factor ** (attempt - 1))

            # Add jitter to avoid thundering herd (5-15% randomness)
            jitter = random.uniform(0.05, 0.15) * current_delay
            sleep_time = current_delay + jitter

            logger.warning(
                f"Retryable error: '{error_message}'. "
                f"Retry {attempt}/{max_retries} in {sleep_time:.1f}s"
            )

            time.sleep(sleep_time)
