import time

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


def test_computational_complex_function():
    succeeded, result = run_in_sandbox_subprocess(computational_complex_function, {"x": 2, "y": 6})
    assert succeeded
    assert result == 2016


def test_guard_against_high_cpu_usage(monkeypatch):
    monkeypatch.setenv("EXECUTOR_MAX_CPU_TIME_LIMIT", "1")
    succeeded, result = run_in_sandbox_subprocess(
        computational_complex_function, {"x": 2, "y": 100}
    )
    assert not succeeded
    assert "The function execution has been terminated with a signal" in result


def test_function_execution_timeout(monkeypatch):
    monkeypatch.setenv("EXECUTOR_TIMEOUT", "1")
    succeeded, result = run_in_sandbox_subprocess(sleep_function, {"seconds": 2})
    assert not succeeded
    assert "The function execution has timed out and has been canceled" in result


def test_no_op_function():
    def no_op():
        pass

    succeeded, result = run_in_sandbox_subprocess(no_op, {})
    assert succeeded
    assert "The function execution has completed, but no output was produced" in result


def test_exception_in_function():
    def raise_exception():
        raise ValueError("This is a test exception")

    succeeded, result = run_in_sandbox_subprocess(raise_exception, {})
    assert not succeeded
    assert "ValueError: This is a test exception" in result


def test_allowed_import_function():
    succeeded, result = run_in_sandbox_subprocess(allowed_import_function, {})
    assert succeeded
    assert result == 4.0


def test_none_returning_function():
    succeeded, result = run_in_sandbox_subprocess(none_returning_function, {})
    assert succeeded
    assert "The function execution has completed, but no output was produced" in result


def test_non_callable_function():
    with pytest.raises(TypeError):
        run_in_sandbox_subprocess("not a function", {})
