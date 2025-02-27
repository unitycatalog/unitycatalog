import asyncio
import builtins
import logging
import resource
import traceback
from multiprocessing import get_context
from typing import Any, Callable

from unitycatalog.ai.core.envs.executor_env_vars import (
    EXECUTOR_MAX_CPU_TIME_LIMIT,
    EXECUTOR_MAX_MEMORY_LIMIT,
    EXECUTOR_TIMEOUT,
)

DISALLOWED_MODULES = {
    "sys",
    "subprocess",
    "ctypes",
    "socket",
    "importlib",
    "pickle",
    "marshal",
    "shutil",
    "pathlib",
}
MB_CONVERSION = 1024 * 1024

TIMEOUT_ERROR_MESSAGE = (
    "The function execution has timed out and has been canceled due to excessive resource consumption.\n"
    "There are two timeout conditions to consider:\n"
    "\t1. You can increase the CPU execution timeout by setting "
    "the environment variable EXECUTOR_MAX_CPU_TIME_LIMIT. The default value is 10 seconds."
    "\t2. You can increase the wall-clock time limit by setting the environment variable "
    "EXECUTOR_TIMEOUT. The default value is 20 seconds.\n"
)

NO_OUTPUT_MESSAGE = (
    "The function execution has completed, but no output was produced. "
    "This may indicate that the function did not return a value or that an error occurred "
    "without being captured.\nCheck the function implementation and ensure it returns a value "
    "by locally executing the function returned from calling the `get_function_source` method on the function "
    "client object and converting your function to a callable using `unitycatalog.ai.core.utils.execution_utils.load_function_from_string`."
)

_logger = logging.getLogger(__name__)


def generate_terminated_message(signal_num: int) -> str:
    """
    Generate a message indicating that the process was terminated by a signal.

    Parameters:
        signal_num: The signal number that caused the termination.

    Returns:
        A string message indicating the termination reason.
    """
    return (
        f"The function execution has been terminated with a signal {signal_num}. "
        "This likely indicates that the function exceeded its resource limits "
        "(CPU execution time or exceeded its virtual memory cgroup allocation). "
        "You can adjust the following environment variables to increase the limits if "
        "you are certain that your function is behaving as expected:\n"
        "\t1. EXECUTOR_MAX_CPU_TIME_LIMIT: Maximum CPU execution time limit for the executor "
        "measured in CPU seconds (total CPU execution time, not wall-clock time).\n"
        "\t2. EXECUTOR_MAX_MEMORY_LIMIT: Maximum memory limit for the executor measured in MB. "
        "The default value is 100 MB. Note that this restriction is only applicable to Linux environments "
        "and has no functional use in Mac OSX or Windows.\n"
    )


def generate_import_disallowed_message(module_name: str) -> str:
    """
    Generate a message indicating that the import of a module is disallowed.

    Parameters:
        module_name: The name of the disallowed module.

    Returns:
        A string message indicating the disallowed import.
    """
    return (
        f"The import of module '{module_name}' is restricted within the local sandbox executor for safety reasons. "
        "This is to prevent the execution of potentially dangerous code or access to sensitive system resources.\n"
        "The list of modules that are disallowed includes: {', '.join(DISALLOWED_MODULES)}.\n"
        "Set a different execution mode in your function client in order to execute this function."
    )


def _limit_resources(cpu_time_limit: int, memory_limit: int):
    """
    Limit CPU and memory usage.

    Notes:
        This CPU restriction will only work on Unix-based systems. When
        operating in an OS that does not support these limits, the function
        execution process will have full access to system resources.
        The memory limit is set for the virtual memory size (address space).
        The memory limit configuration will only work on Linux machines.
        The CPU time limit is for total CPU execution time and is not related
        to the wall-clock time. The process may still be terminated if it exceeds
        the wall-clock time limit set by the multiprocessing library, depending on
        the system's scheduling and load.

    Parameters:
        cpu_time_limit: Maximum CPU time in seconds.
        memory_limit: Maximum memory in MB.
    """
    try:
        resource.setrlimit(resource.RLIMIT_CPU, (cpu_time_limit, cpu_time_limit))
    except Exception as e:
        _logger.info("Warning: unable to set RLIMIT_CPU: %s", e)

    try:
        memory_bytes = memory_limit * MB_CONVERSION
        resource.setrlimit(resource.RLIMIT_AS, (memory_bytes, memory_bytes))
    except Exception as e:
        _logger.info("Warning: unable to set RLIMIT_AS: %s", e)


def _disable_unwanted_imports():
    """
    Override the built-in __import__ and open function to block
    potentially dangerous modules and file access.
    """
    original_import = builtins.__import__

    def restricted_import(name, globals=None, locals=None, fromlist=(), level=0):
        if name in DISALLOWED_MODULES:
            raise ImportError(
                f"Import of module '{name}' is restricted within the local sandbox "
                "executor. Set a different execution environment to execute this function."
            )
        return original_import(name, globals, locals, fromlist, level)

    builtins.__import__ = restricted_import

    def disabled_open(*args, **kwargs):
        raise ImportError("The open function is disabled in this sandbox")

    builtins.open = disabled_open


def _sandboxed_wrapper(
    q, func: Callable[..., Any], params: dict[str, Any], cpu_time_limit: int, memory_limit: int
):
    """
    Execute the provided callable in a sandboxed environment.

    Applies resource limits and restricts dangerous module imports.
    The function `func` is called with keyword arguments from `params`.
    The result or any exception's full stack trace is placed on a queue.
    """
    try:
        _limit_resources(cpu_time_limit, memory_limit)
        _disable_unwanted_imports()
        result = func(**params)
        q.put((True, result))
    except Exception:
        tb = traceback.format_exc()
        q.put((False, tb))


async def async_run_in_sandbox(
    func: Callable[..., Any], params: dict[str, Any]
) -> tuple[bool, Any]:
    """
    Executes a Python callable in a sandboxed subprocess using multiprocessing.
    Specific core Python modules are restricted to prevent unwanted behavior.
    The function is executed with a timeout and resource limits for CPU and memory.
    The function's result or any exception's full stack trace is returned.

    Parameters:
        func: The callable to execute.
        params: A dictionary of keyword arguments to pass to the function.

    Returns:
        A tuple (success, result). If success is False, result contains a descriptive
        error message or the error stack trace.
    """
    cpu_time_limit = EXECUTOR_MAX_CPU_TIME_LIMIT.get()
    memory_limit = EXECUTOR_MAX_MEMORY_LIMIT.get()
    timeout = EXECUTOR_TIMEOUT.get()

    ctx = get_context("fork")
    q = ctx.Queue()
    p = ctx.Process(target=_sandboxed_wrapper, args=(q, func, params, cpu_time_limit, memory_limit))
    p.start()

    loop = asyncio.get_event_loop()
    try:
        await asyncio.wait_for(loop.run_in_executor(None, p.join), timeout)
    except asyncio.TimeoutError:
        p.terminate()
        await loop.run_in_executor(None, p.join)
        return False, TIMEOUT_ERROR_MESSAGE

    if not q.empty():
        return q.get()
    else:
        exitcode = p.exitcode
        if exitcode is not None and exitcode < 0:
            signal_num = -exitcode
            return False, generate_terminated_message(signal_num)
        return False, NO_OUTPUT_MESSAGE
