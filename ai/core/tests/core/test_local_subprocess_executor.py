import inspect

import pytest

from unitycatalog.ai.core.executor.local_subprocess import run_in_sandbox_subprocess


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


def sleep_function(seconds: int):
    """
    A function that sleeps for a specified number of seconds.
    """
    import time

    time.sleep(seconds)
    return "Slept for {} seconds".format(seconds)


def allowed_import_function():
    """
    A function that uses an allowed module.
    """
    import math

    return math.sqrt(16)


def none_returning_function():
    """
    A function that returns None.
    """
    return None


def no_op():
    pass


def test_computational_complex_function():
    src = inspect.getsource(computational_complex_function)
    succeeded, result = run_in_sandbox_subprocess(src, {"x": 2, "y": 6})
    assert succeeded
    assert result == 2016


def test_guard_against_high_cpu_usage(monkeypatch):
    monkeypatch.setenv("EXECUTOR_MAX_CPU_TIME_LIMIT", "1")
    src = inspect.getsource(computational_complex_function)
    succeeded, result = run_in_sandbox_subprocess(src, {"x": 2, "y": 100})
    assert not succeeded
    assert "The function execution has been terminated with a signal" in result


def test_function_execution_timeout(monkeypatch):
    monkeypatch.setenv("EXECUTOR_TIMEOUT", "1")
    src = inspect.getsource(sleep_function)
    succeeded, result = run_in_sandbox_subprocess(src, {"seconds": 2})
    assert not succeeded
    assert "The function execution has timed out and has been canceled" in result


def test_no_op_function():
    src = inspect.getsource(no_op)
    succeeded, result = run_in_sandbox_subprocess(src, {})
    assert succeeded
    assert "The function execution has completed, but no output was produced" in result


def test_exception_in_function():
    def raise_exception():
        raise ValueError("This is a test exception")

    src = inspect.getsource(raise_exception)
    succeeded, result = run_in_sandbox_subprocess(src, {})
    assert not succeeded
    assert "ValueError: This is a test exception" in result


def test_allowed_import_function():
    src = inspect.getsource(allowed_import_function)
    succeeded, result = run_in_sandbox_subprocess(src, {})
    assert succeeded
    assert result == 4.0


def test_none_returning_function():
    src = inspect.getsource(none_returning_function)
    succeeded, result = run_in_sandbox_subprocess(src, {})
    assert succeeded
    assert "The function execution has completed, but no output was produced" in result


def test_non_callable_function():
    # run_in_sandbox_subprocess now expects a string containing function source.
    with pytest.raises(TypeError):
        run_in_sandbox_subprocess(12345, {})
