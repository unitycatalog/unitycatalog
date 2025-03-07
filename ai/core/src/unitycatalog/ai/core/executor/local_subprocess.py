import ast
import base64
import json
import logging
import os
import subprocess
import sys
import tempfile
import textwrap
from typing import Any

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


def _extract_function_name(function_source: str) -> str:
    """
    Extracts the name of the first function defined in the provided source code
    using the ast module.
    """
    try:
        module_ast = ast.parse(textwrap.dedent(function_source))
    except SyntaxError as e:
        raise ValueError("Invalid Python source code") from e

    for node in module_ast.body:
        if isinstance(node, ast.FunctionDef):
            return node.name
    raise ValueError("No function definition found in the provided source code.")


def _generate_runner_script(function_source: str, cpu_time_limit: int, memory_limit: int) -> str:
    """
    Generates a Python runner script that sets resource limits and embeds the function's source code
    directly into the script that will be executed in a subprocess.
    """
    function_name = _extract_function_name(function_source)
    function_source = textwrap.dedent(function_source)
    script = f"""
import sys, base64, cloudpickle, json, traceback, asyncio, resource, os

{function_source}

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
        b64_params = sys.argv[1]
        params = cloudpickle.loads(base64.b64decode(b64_params))
        result = {function_name}(**params)
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


def run_in_sandbox_subprocess(function_source: str, params: dict[str, Any]) -> tuple[bool, Any]:
    """
    Executes a Python source in a sandboxed subprocess.

    The parameters are serialized via cloudpickle and passed to a temporary runner
    script that applies resource limits and executes the function.
    The runner returns a JSON message indicating success or error.

    Args:
        function_source: The source code of the function to execute.
        params: The parameters to pass to the function.

    Returns:
        (success, result): If success is True, result is the function's return value (or a default message if None).
        Otherwise, result contains the error message.
    """
    timeout = EXECUTOR_TIMEOUT.get()

    pickled_params = cloudpickle.dumps(params)
    b64_params = base64.b64encode(pickled_params).decode("utf-8")

    cpu_time_limit = EXECUTOR_MAX_CPU_TIME_LIMIT.get()
    memory_limit = EXECUTOR_MAX_MEMORY_LIMIT.get()

    script = _generate_runner_script(
        function_source=function_source, cpu_time_limit=cpu_time_limit, memory_limit=memory_limit
    )

    # NB: The tempfile is left on disk and is cleaned up after the subprocess completes within the
    # `finally`` block later. This is due to the context manager for tempfile introducing a file lock on
    # on the file which prevents the subprocess from reading the file.
    with tempfile.NamedTemporaryFile("w", delete=False, suffix=".py") as f:
        f.write(script)
        script_path = f.name

    env = os.environ.copy()
    env["PYTHONPATH"] = os.pathsep.join(sys.path)

    try:
        proc = subprocess.run(
            [sys.executable, script_path, b64_params],
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

    # NB: This is helpful for debugging
    if proc.stderr:
        _logger.error("Subprocess stderr: %s", proc.stderr)

    try:
        output = proc.stdout.strip()
        result_data = json.loads(output)
        if result_data.get("success"):
            return True, result_data.get("result")
        else:
            return False, result_data.get("error")
    except Exception as e:
        return False, f"Failed to parse subprocess output: {e}"
