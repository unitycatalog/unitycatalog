import asyncio
import builtins
import logging
import resource
import traceback
from multiprocessing import get_context
from typing import Any, Callable

from unitycatalog.ai.core.envs.executor_env_vars import (
    EXECUTOR_DISALLOWED_MODULES,
    EXECUTOR_MAX_CPU_TIME_LIMIT,
    EXECUTOR_MAX_MEMORY_LIMIT,
    EXECUTOR_TIMEOUT,
)
from unitycatalog.ai.core.executor.common import (
    IMPORT_DISALLOWED_MESSAGE,
    IMPORT_DISALLOWED_MESSAGE_TEMPLATE,
    MB_CONVERSION,
    NO_OUTPUT_MESSAGE,
    OPEN_DISALLOWED_MESSAGE,
    TERMINATED_MESSAGE_TEMPLATE,
    TIMEOUT_ERROR_MESSAGE,
)

_logger = logging.getLogger(__name__)


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
        if name in EXECUTOR_DISALLOWED_MODULES.get():
            error_msg = IMPORT_DISALLOWED_MESSAGE_TEMPLATE.format(
                module_name=name, import_disallowed_message=IMPORT_DISALLOWED_MESSAGE
            )
            raise ImportError(error_msg)
        return original_import(name, globals, locals, fromlist, level)

    builtins.__import__ = restricted_import

    def disabled_open(*args, **kwargs):
        raise ImportError(OPEN_DISALLOWED_MESSAGE)

    builtins.open = disabled_open


def _sandboxed_wrapper(
    q, func: Callable[..., Any], params: dict[str, Any], cpu_time_limit: int, memory_limit: int
):
    """
    Execute the provided callable in a sandboxed environment.

    Applies resource limits and restricts dangerous module imports.
    The function `func` is called with keyword arguments from `params`.
    If the function returns a coroutine, it is executed via asyncio.run().
    The result or any exception's full stack trace is placed on a queue.
    """
    try:
        _limit_resources(cpu_time_limit, memory_limit)
        _disable_unwanted_imports()
        result = func(**params)
        if asyncio.iscoroutine(result):
            result = asyncio.run(result)
        parsed_result = result if result is not None else NO_OUTPUT_MESSAGE
        q.put((True, parsed_result))
    except Exception:
        tb = traceback.format_exc()
        q.put((False, tb))


async def run_in_sandbox_async(
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
            return False, TERMINATED_MESSAGE_TEMPLATE.format(signal_num=signal_num)
        return False, NO_OUTPUT_MESSAGE


def run_in_sandbox(func: Callable[..., Any], params: dict[str, Any]) -> tuple[bool, Any]:
    """
    Synchronous version of run_in_sandbox_async to support non-async APIs.
    Executes the given function in a sandboxed subprocess using multiprocessing
    with resource limits and a restricted import mechanism.

    This function first checks if an event loop is already running. If one is found
    (e.g. in a Jupyter environment), it uses that loop's run_until_complete method;
    otherwise, it creates a new event loop with asyncio.run().

    Parameters:
        func: The callable to execute.
        params: A dictionary of keyword arguments to pass to the function.

    Returns:
        A tuple (success, result) as returned by run_in_sandbox_async.
    """
    if not callable(func):
        raise TypeError("The provided function is not callable.")
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        # No running loop: create one and schedule the coroutine as a task.
        loop = asyncio.new_event_loop()
        try:
            task = loop.create_task(run_in_sandbox_async(func, params))
            return loop.run_until_complete(task)
        finally:
            loop.close()
    else:
        return loop.run_until_complete(run_in_sandbox_async(func, params))
