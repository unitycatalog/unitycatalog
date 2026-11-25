import ast
import base64
import json
import logging
import os
import subprocess
import sys
import tempfile
from typing import Any

import cloudpickle

from unitycatalog.ai.core.envs.executor_env_vars import (
    EXECUTOR_MAX_CPU_TIME_LIMIT,
    EXECUTOR_MAX_MEMORY_LIMIT,
    EXECUTOR_TIMEOUT,
)
from unitycatalog.ai.core.executor.common import (
    EXITED_WITHOUT_RESULT_MESSAGE_TEMPLATE,
    MB_CONVERSION,
    NO_OUTPUT_MESSAGE,
    TERMINATED_MESSAGE_TEMPLATE,
    TIMEOUT_ERROR_MESSAGE,
    UNREADABLE_RESULT_MESSAGE_TEMPLATE,
)
from unitycatalog.ai.core.utils.code_layout import dedent_code

_logger = logging.getLogger(__name__)

_MAX_STDERR_CHARS_IN_MESSAGE = 4096


def _extract_function_name(function_source: str) -> str:
    """
    Extracts the name of the first function defined in the provided source code
    using the ast module.
    """
    try:
        module_ast = ast.parse(function_source)
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
    function_source = dedent_code(function_source)
    function_name = _extract_function_name(function_source)
    script = f"""
import sys, base64, cloudpickle, json, traceback, asyncio, os
import sys as _uc_sys, base64 as _uc_base64, builtins as _uc_builtins
import cloudpickle as _uc_cloudpickle, json as _uc_json, traceback as _uc_traceback
import asyncio as _uc_asyncio

try:
    import resource
except ImportError:
    resource = None
_uc_resource = resource

{function_source}

def _uc_limit_resources():
    if _uc_resource is None:
        return
    try:
        _uc_resource.setrlimit(_uc_resource.RLIMIT_CPU, ({cpu_time_limit}, {cpu_time_limit}))
    except Exception as e:
        _uc_sys.stderr.write("Warning: unable to set RLIMIT_CPU: " + str(e) + "\\n")
    try:
        mem_bytes = {memory_limit} * {MB_CONVERSION}
        _uc_resource.setrlimit(_uc_resource.RLIMIT_AS, (mem_bytes, mem_bytes))
    except Exception as e:
        _uc_sys.stderr.write("Warning: unable to set RLIMIT_AS: " + str(e) + "\\n")

def _uc_main():
    _uc_limit_resources()
    _uc_result_path = _uc_sys.argv[2]
    try:
        _uc_params = _uc_cloudpickle.loads(_uc_base64.b64decode(_uc_sys.argv[1]))
        _uc_result = {function_name}(**_uc_params)
        if _uc_asyncio.iscoroutine(_uc_result):
            _uc_result = _uc_asyncio.run(_uc_result)
        if _uc_result is None:
            _uc_result = "{NO_OUTPUT_MESSAGE}"
        _uc_output = _uc_json.dumps({{"success": True, "result": _uc_result}})
    except Exception:
        _uc_output = _uc_json.dumps({{"success": False, "error": _uc_traceback.format_exc()}})
    with _uc_builtins.open(_uc_result_path, "w", encoding="utf-8") as _uc_file:
        _uc_file.write(_uc_output)

if __name__ == "__main__":
    _uc_main()
"""
    return script


def _read_result(result_path: str) -> tuple[dict[str, Any] | None, str | None]:
    """
    Reads the record that the runner script wrote to the result file.

    Returns the record, or None with the reason it could not be read. Both are None
    when the runner wrote nothing.
    """
    try:
        with open(result_path, encoding="utf-8") as f:
            output = f.read()
        if not output:
            return None, None
        result_data = json.loads(output)
    except (OSError, ValueError, RecursionError) as e:
        return None, f"{type(e).__name__}: {e}"
    if not isinstance(result_data, dict):
        return None, f"expected a JSON object, got {type(result_data).__name__}"
    return result_data, None


# TODO(#2359): share the no-result error handling with the sandbox executor.
def _stderr_tail(stderr: str) -> str:
    if not stderr:
        return ""
    return (
        "Standard error output of the process "
        f"(at most the last {_MAX_STDERR_CHARS_IN_MESSAGE} characters):\n"
        f"{stderr[-_MAX_STDERR_CHARS_IN_MESSAGE:]}"
    )


def run_in_sandbox_subprocess(function_source: str, params: dict[str, Any]) -> tuple[bool, Any]:
    """
    Executes a Python source in a sandboxed subprocess.

    The parameters are serialized via cloudpickle and passed to a temporary runner
    script that applies resource limits and executes the function.
    The runner writes a JSON message indicating success or error to a temporary result file,
    so output that the function writes to stdout does not change the result.

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

    env = os.environ.copy()
    env["PYTHONPATH"] = os.pathsep.join(sys.path)

    result_fd, result_path = tempfile.mkstemp(suffix=".json")
    os.close(result_fd)
    temp_paths = [result_path]
    try:
        # NB: The script file is left on disk and is removed in the `finally` block after the
        # subprocess completes. The tempfile context manager holds a lock on the file, which
        # prevents the subprocess from reading it.
        with tempfile.NamedTemporaryFile("w", delete=False, suffix=".py") as f:
            temp_paths.append(f.name)
            f.write(script)
            script_path = f.name
        proc = subprocess.run(
            [sys.executable, script_path, b64_params, result_path],
            capture_output=True,
            text=True,
            errors="replace",
            timeout=timeout,
            env=env,
        )
        result_data, read_error = _read_result(result_path)
    except subprocess.TimeoutExpired:
        return False, TIMEOUT_ERROR_MESSAGE
    finally:
        for path in temp_paths:
            try:
                os.remove(path)
            except Exception as e:
                _logger.warning("Failed to remove temporary file %s: %s", path, e)

    # NB: This is helpful for debugging
    if proc.stdout:
        _logger.debug("Subprocess stdout: %s", proc.stdout)
    if proc.stderr:
        _logger.error("Subprocess stderr: %s", proc.stderr)

    if result_data is not None:
        if result_data.get("success"):
            return True, result_data.get("result")
        return False, result_data.get("error")
    if proc.returncode < 0:
        return False, TERMINATED_MESSAGE_TEMPLATE.format(signal_num=-proc.returncode)
    if read_error is not None:
        message = UNREADABLE_RESULT_MESSAGE_TEMPLATE.format(
            exit_code=proc.returncode, error=read_error
        )
    else:
        message = EXITED_WITHOUT_RESULT_MESSAGE_TEMPLATE.format(exit_code=proc.returncode)
    return False, message + _stderr_tail(proc.stderr)
