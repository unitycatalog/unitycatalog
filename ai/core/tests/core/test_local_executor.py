import asyncio
import concurrent.futures
import time

import pytest

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
