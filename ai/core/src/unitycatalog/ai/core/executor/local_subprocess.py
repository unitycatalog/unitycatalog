import base64
import json
import logging
import os
import subprocess
import sys
import tempfile
import textwrap
from typing import Any, Callable

import cloudpickle

from unitycatalog.ai.core.envs.executor_env_vars import (
    EXECUTOR_MAX_CPU_TIME_LIMIT,
    EXECUTOR_MAX_MEMORY_LIMIT,
    EXECUTOR_TIMEOUT,
)
from unitycatalog.ai.core.executor.common import (
    MB_CONVERSION,
    NO_OUTPUT_MESSAGE,
    TIMEOUT_ERROR_MESSAGE,
)

_logger = logging.getLogger(__name__)


def _generate_runner_script(cpu_time_limit: int, memory_limit: int) -> str:
    """
    Generates a Python runner script that sets resource limits.
    """
    script = f"""
import sys, base64, json, traceback, asyncio, resource, os, cloudpickle

def _limit_resources():
    try:
        resource.setrlimit(resource.RLIMIT_CPU, ({cpu_time_limit}, {cpu_time_limit}))
    except Exception as e:
        sys.stderr.write("Warning: unable to set RLIMIT_CPU: " + str(e) + "\\n")
    try:
        mem_bytes = {memory_limit} * {MB_CONVERSION}
        resource.setrlimit(resource.RLIMIT_AS, (mem_bytes, mem_bytes))
    except Exception as e:
        sys.stderr.write("Warning: unable to set RLIMIT_AS: " + str(e) + "\\n")

def main():
    _limit_resources()
    try:
        b64_func = sys.argv[1]
        b64_params = sys.argv[2]
        func = cloudpickle.loads(base64.b64decode(b64_func))
        params = cloudpickle.loads(base64.b64decode(b64_params))
        result = func(**params)
        if asyncio.iscoroutine(result):
            result = asyncio.run(result)
        if result is None:
            result = "{NO_OUTPUT_MESSAGE}"
        print(json.dumps({{"success": True, "result": result}}))
        sys.stdout.flush()
    except Exception:
        tb = traceback.format_exc()
        print(json.dumps({{"success": False, "error": tb}}))
        sys.stdout.flush()

if __name__ == "__main__":
    main()
"""
    return textwrap.dedent(script)


_logger = logging.getLogger(__name__)


def run_in_sandbox_subprocess(func: Callable[..., Any], params: dict[str, Any]) -> tuple[bool, Any]:
    """
    Executes a Python callable in a sandboxed subprocess.

    The function and parameters are serialized via cloudpickle and passed to a temporary runner
    script that applies resource limits, disables dangerous module imports and built-ins, and executes the function.
    The runner returns a JSON message indicating success or error.

    Returns:
        (success, result): If success is True, result is the function’s return value (or a default message if None).
        Otherwise, result contains the error message.
    """
    timeout = float(EXECUTOR_TIMEOUT.get())

    if not callable(func):
        raise TypeError("The provided function is not callable.")

    pickled_func = cloudpickle.dumps(func)
    pickled_params = cloudpickle.dumps(params)
    b64_func = base64.b64encode(pickled_func).decode("utf-8")
    b64_params = base64.b64encode(pickled_params).decode("utf-8")

    cpu_time_limit = EXECUTOR_MAX_CPU_TIME_LIMIT.get()
    memory_limit = EXECUTOR_MAX_MEMORY_LIMIT.get()

    script = _generate_runner_script(cpu_time_limit=cpu_time_limit, memory_limit=memory_limit)

    with tempfile.NamedTemporaryFile("w", delete=False, suffix=".py") as f:
        f.write(script)
        script_path = f.name

    env = os.environ.copy()
    env["PYTHONPATH"] = os.pathsep.join(sys.path)

    try:
        proc = subprocess.run(
            [sys.executable, script_path, b64_func, b64_params],
            capture_output=True,
            text=True,
            timeout=timeout,
            env=env,
        )
    except subprocess.TimeoutExpired:
        return False, TIMEOUT_ERROR_MESSAGE
    finally:
        try:
            os.remove(script_path)
        except Exception as e:
            _logger.warning("Failed to remove temporary file %s: %s", script_path, e)

    if proc.returncode < 0 or not proc.stdout.strip():
        return False, "The function execution has been terminated with a signal"

    try:
        output = proc.stdout.strip()
        result_data = json.loads(output)
        if result_data.get("success"):
            return True, result_data.get("result")
        else:
            return False, result_data.get("error")
    except Exception as e:
        return False, f"Failed to parse subprocess output: {e}"
