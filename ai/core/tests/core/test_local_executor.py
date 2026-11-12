import asyncio
import concurrent.futures
import importlib
import logging
import mmap
import multiprocessing
import os
import struct
import sys
import time

import pytest

import unitycatalog.ai.core.executor.local as local_executor
from unitycatalog.ai.core.executor.common import SANDBOX_UNSUPPORTED_MESSAGE
from unitycatalog.ai.core.executor.local import run_in_sandbox, run_in_sandbox_async


def simple_function(x: int, y: int) -> int:
    """
    A simple function that adds two numbers.
    """
    return x + y


def computational_complex_function(x: int, y: int) -> int:
    """
    A function that performs a computationally complex operation.
    """
    return sum(range(x**y))


def invalid_import_function():
    """
    A function that uses a disallowed module.
    """
    import sys

    return sys.version


def invalid_file_access_function():
    """
    A function that tries to read a file.
    """
    with open("test.txt", "r") as f:
        return f.read()


def sleep_function(seconds: int):
    """
    A function that sleeps for a specified number of seconds.
    """
    time.sleep(seconds)
    return "Slept for {} seconds".format(seconds)


def allowed_import_function():
    """
    A function that uses an allowed module.
    """
    import math  # math is allowed

    return math.sqrt(16)


def none_returning_function():
    """
    A function that returns None.
    """
    return None


def test_invalid_import():
    succeeded, result = run_in_sandbox(invalid_import_function, {})
    assert not succeeded
    assert "ImportError: The import of module 'sys' is restricted" in result


@pytest.mark.asyncio
async def test_invalid_import_async():
    succeeded, result = await run_in_sandbox_async(invalid_import_function, {})
    assert not succeeded
    assert "ImportError: The import of module 'sys' is restricted" in result


def test_computational_complex_function():
    succeeded, result = run_in_sandbox(computational_complex_function, {"x": 2, "y": 6})
    assert succeeded
    assert result == 2016


@pytest.mark.asyncio
async def test_computational_complex_function_async():
    succeeded, result = await run_in_sandbox_async(computational_complex_function, {"x": 2, "y": 6})
    assert succeeded
    assert result == 2016


def test_guard_against_high_cpu_usage(monkeypatch):
    monkeypatch.setenv("EXECUTOR_MAX_CPU_TIME_LIMIT", "1")
    succeeded, result = run_in_sandbox(computational_complex_function, {"x": 2, "y": 100})
    assert not succeeded
    assert "The function execution has been terminated with a signal" in result


@pytest.mark.asyncio
async def test_guard_against_high_cpu_usage_async(monkeypatch):
    monkeypatch.setenv("EXECUTOR_MAX_CPU_TIME_LIMIT", "1")
    succeeded, result = await run_in_sandbox_async(
        computational_complex_function, {"x": 2, "y": 100}
    )
    assert not succeeded
    assert "The function execution has been terminated with a signal" in result


def test_function_execution_timeout(monkeypatch):
    monkeypatch.setenv("EXECUTOR_TIMEOUT", "1")
    succeeded, result = run_in_sandbox(sleep_function, {"seconds": 2})
    assert not succeeded
    assert "The function execution has timed out and has been canceled" in result


@pytest.mark.asyncio
async def test_function_execution_timeout_async(monkeypatch):
    monkeypatch.setenv("EXECUTOR_TIMEOUT", "1")
    succeeded, result = await run_in_sandbox_async(sleep_function, {"seconds": 2})
    assert not succeeded
    assert "The function execution has timed out and has been canceled" in result


@pytest.mark.asyncio
async def test_concurrent_calls():
    tasks = [run_in_sandbox_async(sleep_function, {"seconds": 1}) for _ in range(10)]

    start_time = time.perf_counter()
    results = await asyncio.gather(*tasks)
    elapsed = time.perf_counter() - start_time

    for success, result in results:
        assert success, f"Task failed with error: {result}"

    assert elapsed < 2, f"Expected concurrent execution, but tasks took {elapsed:.2f} seconds"


def test_no_op_function():
    def no_op():
        pass

    succeeded, result = run_in_sandbox(no_op, {})
    assert succeeded
    assert "The function execution has completed, but no output was produced" in result


@pytest.mark.asyncio
async def test_no_op_function_async():
    def no_op():
        pass

    succeeded, result = await run_in_sandbox_async(no_op, {})
    assert succeeded
    assert "The function execution has completed, but no output was produced" in result


def test_exception_in_function():
    def raise_exception():
        raise ValueError("This is a test exception")

    succeeded, result = run_in_sandbox(raise_exception, {})
    assert not succeeded
    assert "ValueError: This is a test exception" in result


@pytest.mark.asyncio
async def test_exception_in_function_async():
    def raise_exception():
        raise ValueError("This is a test exception")

    succeeded, result = await run_in_sandbox_async(raise_exception, {})
    assert not succeeded
    assert "ValueError: This is a test exception" in result


def test_invalid_file_access():
    succeeded, result = run_in_sandbox(invalid_file_access_function, {})
    assert not succeeded
    assert "ImportError: The use of the 'open' function is restricted" in result


@pytest.mark.asyncio
async def test_invalid_file_access_async():
    succeeded, result = await run_in_sandbox_async(invalid_file_access_function, {})
    assert not succeeded
    assert "ImportError: The use of the 'open' function is restricted" in result


def test_allowed_import_function():
    succeeded, result = run_in_sandbox(allowed_import_function, {})
    assert succeeded
    assert result == 4.0


@pytest.mark.asyncio
async def test_allowed_import_function_async():
    succeeded, result = await run_in_sandbox_async(allowed_import_function, {})
    assert succeeded
    assert result == 4.0


def test_none_returning_function():
    succeeded, result = run_in_sandbox(none_returning_function, {})
    assert succeeded
    assert "The function execution has completed, but no output was produced" in result


@pytest.mark.asyncio
async def test_none_returning_function_async():
    succeeded, result = await run_in_sandbox_async(none_returning_function, {})
    assert succeeded
    assert "The function execution has completed, but no output was produced" in result


def test_non_callable_function():
    with pytest.raises(TypeError):
        run_in_sandbox("not a function", {})


def test_concurrent_sync_calls():
    def sleep_and_return(seconds: int):
        time.sleep(seconds)
        return seconds

    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [
            executor.submit(run_in_sandbox, sleep_and_return, {"seconds": 1}) for _ in range(10)
        ]
        results = [f.result() for f in futures]

    for succeeded, result in results:
        assert succeeded
        assert result == 1


@pytest.fixture
def executor_without_resource_module(monkeypatch):
    monkeypatch.setitem(sys.modules, "resource", None)
    yield importlib.reload(local_executor)
    # Must run before the reload below so the real `resource` module is imported again.
    monkeypatch.undo()
    importlib.reload(local_executor)


def test_executor_imports_and_skips_limits_without_resource_module(
    executor_without_resource_module, caplog
):
    assert executor_without_resource_module.resource is None
    with caplog.at_level(logging.INFO, logger=local_executor.__name__):
        executor_without_resource_module._limit_resources(cpu_time_limit=1, memory_limit=1)

    assert "unable to set RLIMIT" not in caplog.text


def test_sandbox_reports_unsupported_platform_without_fork(monkeypatch):
    monkeypatch.setattr(multiprocessing, "get_all_start_methods", lambda: ["spawn"])

    assert run_in_sandbox(simple_function, {"x": 1, "y": 2}) == (
        False,
        SANDBOX_UNSUPPORTED_MESSAGE,
    )


MB = 1024 * 1024

linux_only = pytest.mark.skipif(
    sys.platform != "linux", reason="The memory limit is only enforced on Linux"
)


@pytest.fixture
def parent_address_space_above_memory_limit():
    try:
        reservation = mmap.mmap(-1, 1024 * MB, flags=mmap.MAP_PRIVATE, prot=mmap.PROT_READ)
    except OSError as e:
        pytest.skip(f"Unable to reserve 1 GB of address space: {e}")
    yield
    reservation.close()


def allocate_and_return_size(megabytes: int) -> int:
    return len(bytearray(megabytes * MB))


@linux_only
def test_function_can_allocate_memory_when_parent_address_space_exceeds_limit(
    parent_address_space_above_memory_limit, monkeypatch
):
    monkeypatch.setenv("EXECUTOR_MAX_MEMORY_LIMIT", "100")

    succeeded, result = run_in_sandbox(allocate_and_return_size, {"megabytes": 80})

    assert succeeded, result
    assert result == 80 * MB


@linux_only
@pytest.mark.asyncio
async def test_concurrent_calls_return_results_when_parent_address_space_exceeds_limit(
    parent_address_space_above_memory_limit,
):
    results = await asyncio.gather(
        *[run_in_sandbox_async(simple_function, {"x": i, "y": 1}) for i in range(5)]
    )

    assert results == [(True, i + 1) for i in range(5)]


@linux_only
def test_memory_limit_is_still_enforced_when_parent_address_space_exceeds_limit(
    parent_address_space_above_memory_limit, monkeypatch
):
    monkeypatch.setenv("EXECUTOR_MAX_MEMORY_LIMIT", "50")

    succeeded, result = run_in_sandbox(allocate_and_return_size, {"megabytes": 200})

    assert not succeeded
    assert "MemoryError" in result


def test_result_larger_than_pipe_buffer_is_returned(monkeypatch):
    monkeypatch.setenv("EXECUTOR_TIMEOUT", "5")

    def large_result():
        return "x" * MB

    succeeded, result = run_in_sandbox(large_result, {})

    assert succeeded, result
    assert result == "x" * MB


def test_unpicklable_result_reports_the_pickling_error():
    def unpicklable_result():
        return lambda: None

    succeeded, result = run_in_sandbox(unpicklable_result, {})

    assert not succeeded
    assert "pickle" in result.lower()


def write_partial_result_frame_and_exit(sender):
    os.write(sender.fileno(), struct.pack("!i", 100) + b"partial")
    os._exit(0)


def test_partial_result_is_reported_as_unreadable():
    ctx = multiprocessing.get_context("fork")
    receiver, sender = ctx.Pipe(duplex=False)
    process = ctx.Process(target=write_partial_result_frame_and_exit, args=(sender,))
    process.start()
    sender.close()

    succeeded, result = local_executor._receive_result(receiver, process)

    assert not succeeded
    assert "exited with code 0, but its result could not be read: OSError" in result


def fail_to_unpickle():
    raise ValueError("cannot rebuild this object")


class UnpicklableInParent:
    def __reduce__(self):
        return fail_to_unpickle, ()


def test_result_that_cannot_be_unpickled_is_reported_as_unreadable():
    def unreadable_result():
        return UnpicklableInParent()

    succeeded, result = run_in_sandbox(unreadable_result, {})

    assert not succeeded
    assert "its result could not be read: ValueError: cannot rebuild this object" in result


def test_process_exiting_without_result_reports_its_exit_code():
    def exit_process():
        import os

        os._exit(3)

    succeeded, result = run_in_sandbox(exit_process, {})

    assert not succeeded
    assert "exited with code 3" in result


def read_memory_limit():
    import resource

    return resource.getrlimit(resource.RLIMIT_AS)


def test_memory_limit_is_not_applied_when_address_space_size_is_unknown(
    monkeypatch, tmp_path, caplog
):
    resource = pytest.importorskip("resource")
    monkeypatch.setattr(local_executor, "_STATM_PATH", str(tmp_path / "missing"))
    original_limits = resource.getrlimit(resource.RLIMIT_AS)
    original_cpu_limits = resource.getrlimit(resource.RLIMIT_CPU)

    try:
        with caplog.at_level(logging.INFO, logger=local_executor.__name__):
            local_executor._limit_resources(cpu_time_limit=resource.RLIM_INFINITY, memory_limit=1)
        limits_after_call = resource.getrlimit(resource.RLIMIT_AS)
    finally:
        resource.setrlimit(resource.RLIMIT_CPU, original_cpu_limits)
        resource.setrlimit(resource.RLIMIT_AS, original_limits)
    succeeded, result = run_in_sandbox(read_memory_limit, {})

    assert "cannot read the current address space size" in caplog.text
    assert limits_after_call == original_limits
    assert succeeded, result
    assert result == original_limits


@linux_only
def test_lower_existing_memory_limit_is_not_raised(monkeypatch):
    import resource

    monkeypatch.setenv("EXECUTOR_MAX_MEMORY_LIMIT", "1000")
    original_limits = resource.getrlimit(resource.RLIMIT_AS)
    lower_soft_limit = local_executor._address_space_in_use() + 300 * MB
    resource.setrlimit(resource.RLIMIT_AS, (lower_soft_limit, original_limits[1]))
    try:
        succeeded, result = run_in_sandbox(read_memory_limit, {})
    finally:
        resource.setrlimit(resource.RLIMIT_AS, original_limits)

    assert succeeded, result
    assert result == (lower_soft_limit, lower_soft_limit)
