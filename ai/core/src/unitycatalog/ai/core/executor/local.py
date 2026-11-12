import asyncio
import builtins
import logging
import multiprocessing
import multiprocessing.connection
import traceback
from typing import Any, Callable, Optional

try:
    import resource
except ImportError:  # Not available on Windows
    resource = None

from unitycatalog.ai.core.envs.executor_env_vars import (
    EXECUTOR_DISALLOWED_MODULES,
    EXECUTOR_MAX_CPU_TIME_LIMIT,
    EXECUTOR_MAX_MEMORY_LIMIT,
    EXECUTOR_TIMEOUT,
)
from unitycatalog.ai.core.executor.common import (
    EXITED_WITHOUT_RESULT_MESSAGE_TEMPLATE,
    IMPORT_DISALLOWED_MESSAGE,
    IMPORT_DISALLOWED_MESSAGE_TEMPLATE,
    MB_CONVERSION,
    NO_OUTPUT_MESSAGE,
    OPEN_DISALLOWED_MESSAGE,
    SANDBOX_UNSUPPORTED_MESSAGE,
    TERMINATED_MESSAGE_TEMPLATE,
    TIMEOUT_ERROR_MESSAGE,
    UNREADABLE_RESULT_MESSAGE_TEMPLATE,
)

_logger = logging.getLogger(__name__)

_STATM_PATH = "/proc/self/statm"


def _limit_resources(cpu_time_limit: int, memory_limit: int):
    """
    Limit CPU and memory usage.

    Notes:
        This CPU restriction will only work on Unix-based systems. When
        operating in an OS that does not support these limits, the function
        execution process will have full access to system resources.
        The memory limit caps the virtual memory (address space) that the process can
        allocate on top of what it already holds when the limit is applied. A forked
        process inherits the whole address space of its parent, which can already be
        far larger than the limit, so an absolute cap would leave it unable to allocate
        anything at all. An existing lower limit is never raised, and when the current
        address space size cannot be read, no memory limit is applied.
        The memory limit configuration will only work on Linux machines.
        The CPU time limit is for total CPU execution time and is not related
        to the wall-clock time. The process may still be terminated if it exceeds
        the wall-clock time limit set by the multiprocessing library, depending on
        the system's scheduling and load.

    Parameters:
        cpu_time_limit: Maximum CPU time in seconds.
        memory_limit: Maximum additional virtual memory in MB that the process can
            allocate on top of the address space it holds when the limit is applied.
    """
    if resource is None:
        return

    try:
        resource.setrlimit(resource.RLIMIT_CPU, (cpu_time_limit, cpu_time_limit))
    except Exception as e:
        _logger.info("Warning: unable to set RLIMIT_CPU: %s", e)

    address_space = _address_space_in_use()
    if address_space is None:
        _logger.info(
            "Warning: unable to set RLIMIT_AS: cannot read the current address space size from %s",
            _STATM_PATH,
        )
        return

    try:
        existing_limits = [
            limit
            for limit in resource.getrlimit(resource.RLIMIT_AS)
            if limit != resource.RLIM_INFINITY
        ]
        memory_bytes = min([address_space + memory_limit * MB_CONVERSION, *existing_limits])
        resource.setrlimit(resource.RLIMIT_AS, (memory_bytes, memory_bytes))
    except Exception as e:
        _logger.info("Warning: unable to set RLIMIT_AS: %s", e)


def _address_space_in_use() -> Optional[int]:
    """Returns the current process's virtual memory size in bytes, or None when it is unknown."""
    try:
        with open(_STATM_PATH) as statm:
            return int(statm.read().split()[0]) * resource.getpagesize()
    except (OSError, ValueError, IndexError):
        return None


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
    sender: multiprocessing.connection.Connection,
    func: Callable[..., Any],
    params: dict[str, Any],
    cpu_time_limit: int,
    memory_limit: int,
):
    """
    Execute the provided callable in a sandboxed environment.

    Applies resource limits and restricts dangerous module imports.
    The function `func` is called with keyword arguments from `params`.
    If the function returns a coroutine, it is executed via asyncio.run().
    The result or any exception's full stack trace is sent through `sender`.
    The send happens on the calling thread, because starting a new thread
    (as `multiprocessing.Queue.put` does) needs a fresh stack mapping that the
    memory limit may not leave room for.
    """
    try:
        _limit_resources(cpu_time_limit, memory_limit)
        _disable_unwanted_imports()
        result = func(**params)
        if asyncio.iscoroutine(result):
            result = asyncio.run(result)
        parsed_result = result if result is not None else NO_OUTPUT_MESSAGE
        sender.send((True, parsed_result))
    except Exception:
        tb = traceback.format_exc()
        sender.send((False, tb))


def _receive_result(
    receiver: multiprocessing.connection.Connection, process: multiprocessing.Process
) -> tuple[bool, Any]:
    """
    Waits for the sandboxed process to send its result or to exit, then reaps it.

    The result is read before joining the process, so a result larger than the pipe
    buffer cannot block the process from exiting. When the process exits without
    sending anything, or sends something that cannot be read, the returned error
    describes how it exited.
    """
    read_error = None
    try:
        with receiver:
            multiprocessing.connection.wait([receiver, process.sentinel])
            result = receiver.recv() if receiver.poll() else None
    except EOFError:
        result = None
    except Exception as e:
        result = None
        read_error = f"{type(e).__name__}: {e}"
    finally:
        process.join()
    exit_code = process.exitcode
    if read_error is not None:
        return False, UNREADABLE_RESULT_MESSAGE_TEMPLATE.format(
            exit_code=exit_code, error=read_error
        )
    if result is not None:
        return result
    if exit_code is not None and exit_code < 0:
        return False, TERMINATED_MESSAGE_TEMPLATE.format(signal_num=-exit_code)
    return False, EXITED_WITHOUT_RESULT_MESSAGE_TEMPLATE.format(exit_code=exit_code)


def is_sandbox_supported() -> bool:
    """Returns whether the 'fork' start method that the sandbox executor needs is available."""
    return "fork" in multiprocessing.get_all_start_methods()


async def run_in_sandbox_async(
    func: Callable[..., Any], params: dict[str, Any]
) -> tuple[bool, Any]:
    """
    Executes a Python callable in a sandboxed subprocess using multiprocessing.
    Specific core Python modules are restricted to prevent unwanted behavior.
    The function is executed with a timeout and resource limits for CPU and memory.
    The function's result or any exception's full stack trace is returned.

    On a timeout, the process is terminated and the thread reading its result
    finishes once the process is gone. Known limitation: if the process dies partway
    through sending its result while another process forked from this one still holds
    the sending end of the pipe, that thread stays blocked until the other process exits.

    Parameters:
        func: The callable to execute.
        params: A dictionary of keyword arguments to pass to the function.

    Returns:
        A tuple (success, result). If success is False, result contains a descriptive
        error message or the error stack trace.
    """
    if not is_sandbox_supported():
        return False, SANDBOX_UNSUPPORTED_MESSAGE

    cpu_time_limit = EXECUTOR_MAX_CPU_TIME_LIMIT.get()
    memory_limit = EXECUTOR_MAX_MEMORY_LIMIT.get()
    timeout = EXECUTOR_TIMEOUT.get()

    ctx = multiprocessing.get_context("fork")
    receiver, sender = ctx.Pipe(duplex=False)
    p = ctx.Process(
        target=_sandboxed_wrapper, args=(sender, func, params, cpu_time_limit, memory_limit)
    )
    p.start()
    sender.close()

    loop = asyncio.get_event_loop()
    try:
        return await asyncio.wait_for(
            loop.run_in_executor(None, _receive_result, receiver, p), timeout
        )
    except asyncio.TimeoutError:
        p.terminate()
        await loop.run_in_executor(None, p.join)
        return False, TIMEOUT_ERROR_MESSAGE


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
