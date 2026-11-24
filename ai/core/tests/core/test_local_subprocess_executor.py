import inspect
import json
import logging
import sys
import tempfile
from pathlib import Path

import pytest

from unitycatalog.ai.core.executor.local_subprocess import (
    _generate_runner_script,
    run_in_sandbox_subprocess,
)


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


def printing_function(x: int, y: int) -> int:
    print("debug")  # noqa: T201
    return x + y


def coroutine_printing_function(x: int, y: int):
    async def add():
        print("debug")  # noqa: T201
        return x + y

    return add()


def shell_echo_function(x: int, y: int) -> int:
    import os

    os.system("echo hi")
    return x + y


def undecodable_stdout_function(x: int, y: int) -> int:
    import os

    os.write(1, b"\x81\xff\n")
    return x + y


def print_then_raise_function():
    print("debug")  # noqa: T201
    raise ValueError("bad input")


def exiting_function():
    import sys

    sys.exit(3)


def exiting_with_message_function():
    import sys

    sys.exit("bad config")


def exiting_with_long_stderr_function():
    import sys

    sys.stderr.write("x" * 5000 + "END")
    sys.exit(1)


def corrupt_result_function():
    import os
    import sys

    with open(sys.argv[2], "w") as f:
        f.write("{")
    sys.stderr.write("writer stopped")
    sys.stderr.flush()
    os._exit(0)


def deeply_nested_result_function():
    import os
    import sys

    with open(sys.argv[2], "w") as f:
        f.write('{"success": true, "result": ' + "[" * 100000 + "]" * 100000 + "}")
    os._exit(0)


def non_ascii_function(x: int, y: int) -> str:
    return "caf\u00e9 \u2713 \u65e5\u672c"


def non_object_result_function():
    import os
    import sys

    with open(sys.argv[2], "w") as f:
        f.write("3")
    os._exit(0)


def deleted_result_function():
    import os
    import sys

    os.remove(sys.argv[2])
    os._exit(0)


def temporary_files_location_function(report_path: str, seconds: int) -> int:
    import json
    import os
    import sys
    import time

    with open(report_path, "w") as f:
        json.dump([os.path.dirname(sys.argv[0]), os.path.dirname(sys.argv[2])], f)
    time.sleep(seconds)
    return 3


def self_killing_function():
    import os
    import signal

    os.kill(os.getpid(), signal.SIGKILL)


def set_returning_function():
    return {1}


@pytest.mark.parametrize(
    "function",
    [
        simple_function,
        printing_function,
        coroutine_printing_function,
        shell_echo_function,
        undecodable_stdout_function,
    ],
)
def test_function_writing_to_stdout_returns_its_result(function):
    src = inspect.getsource(function)
    assert run_in_sandbox_subprocess(src, {"x": 1, "y": 2}) == (True, 3)


@pytest.mark.parametrize(
    ("function", "expected_stdout"),
    [(printing_function, "debug"), (undecodable_stdout_function, "")],
)
def test_function_stdout_is_logged_at_debug_level(caplog, function, expected_stdout):
    src = inspect.getsource(function)
    with caplog.at_level(logging.DEBUG, logger="unitycatalog.ai.core.executor.local_subprocess"):
        run_in_sandbox_subprocess(src, {"x": 1, "y": 2})
    assert any(
        record.levelno == logging.DEBUG
        and record.getMessage().startswith("Subprocess stdout: ")
        and expected_stdout in record.getMessage()
        for record in caplog.records
    )


def test_non_ascii_result_round_trips():
    src = inspect.getsource(non_ascii_function)
    assert run_in_sandbox_subprocess(src, {"x": 1, "y": 2}) == (
        True,
        "caf\u00e9 \u2713 \u65e5\u672c",
    )


def test_function_named_like_a_builtin_used_by_the_runner_returns_its_result():
    src = "def open(x: int, y: int) -> int:\n    return x + y\n"
    assert run_in_sandbox_subprocess(src, {"x": 1, "y": 2}) == (True, 3)


def test_function_that_prints_then_raises_reports_the_traceback():
    src = inspect.getsource(print_then_raise_function)
    succeeded, result = run_in_sandbox_subprocess(src, {})
    assert not succeeded
    assert "ValueError: bad input" in result


@pytest.mark.parametrize(
    ("function", "exit_code", "stderr_suffix"),
    [
        (exiting_function, 3, "could not send its result.\n"),
        (
            exiting_with_message_function,
            1,
            "Standard error output of the process (at most the last 4096 characters):\n"
            "bad config\n",
        ),
        (
            exiting_with_long_stderr_function,
            1,
            "(at most the last 4096 characters):\n" + "x" * 4093 + "END",
        ),
    ],
)
def test_function_that_exits_reports_its_exit_code(function, exit_code, stderr_suffix):
    src = inspect.getsource(function)
    succeeded, result = run_in_sandbox_subprocess(src, {})
    assert not succeeded
    assert result.startswith(
        f"The function execution process exited with code {exit_code} before it could report"
    )
    assert result.endswith(stderr_suffix)


@pytest.mark.skipif(sys.platform == "win32", reason="SIGKILL is not available on Windows")
def test_function_killed_by_a_signal_reports_the_signal():
    src = inspect.getsource(self_killing_function)
    succeeded, result = run_in_sandbox_subprocess(src, {})
    assert not succeeded
    assert result.startswith("The function execution has been terminated with a signal 9. ")
    assert "EXECUTOR_MAX_CPU_TIME_LIMIT" in result
    assert "EXECUTOR_MAX_MEMORY_LIMIT" in result


@pytest.mark.parametrize(
    ("function", "error"),
    [
        (corrupt_result_function, "JSONDecodeError: "),
        (non_object_result_function, "expected a JSON object, got int"),
        (deleted_result_function, "FileNotFoundError: "),
    ],
)
def test_unreadable_result_is_reported_as_an_error(function, error):
    src = inspect.getsource(function)
    succeeded, result = run_in_sandbox_subprocess(src, {})
    assert not succeeded
    assert result.startswith(
        "The function execution process exited with code 0, but its result could not be read: "
        + error
    )


def test_unreadable_result_includes_the_standard_error_output():
    src = inspect.getsource(corrupt_result_function)
    succeeded, result = run_in_sandbox_subprocess(src, {})
    assert not succeeded
    assert result.endswith(
        "Standard error output of the process (at most the last 4096 characters):\nwriter stopped"
    )


def test_deeply_nested_result_is_reported_as_an_error(monkeypatch, tmp_path):
    monkeypatch.setattr(tempfile, "tempdir", str(tmp_path))
    src = inspect.getsource(deeply_nested_result_function)
    succeeded, result = run_in_sandbox_subprocess(src, {})
    assert not succeeded
    assert result.startswith(
        "The function execution process exited with code 0, but its result could not be read: "
        "RecursionError: "
    )
    assert list(tmp_path.iterdir()) == []


def test_unserializable_result_is_reported_as_an_error():
    src = inspect.getsource(set_returning_function)
    succeeded, result = run_in_sandbox_subprocess(src, {})
    assert not succeeded
    assert "TypeError: Object of type set is not JSON serializable" in result


def _run_and_report_temporary_files(monkeypatch, tmp_path_factory, seconds, timeout):
    temp_dir = tmp_path_factory.mktemp("executor_temp")
    report_path = tmp_path_factory.mktemp("report") / "dirs.json"
    monkeypatch.setattr(tempfile, "tempdir", str(temp_dir))
    monkeypatch.setenv("EXECUTOR_TIMEOUT", timeout)
    src = inspect.getsource(temporary_files_location_function)
    succeeded, result = run_in_sandbox_subprocess(
        src, {"report_path": str(report_path), "seconds": seconds}
    )
    reported_dirs = [Path(d).resolve() for d in json.loads(report_path.read_text())]
    assert reported_dirs == [temp_dir.resolve()] * 2
    assert list(temp_dir.iterdir()) == []
    return succeeded, result


def test_temporary_files_are_removed_after_a_run(monkeypatch, tmp_path_factory):
    succeeded, result = _run_and_report_temporary_files(
        monkeypatch, tmp_path_factory, seconds=0, timeout="20"
    )
    assert succeeded
    assert result == 3


def test_temporary_files_are_removed_after_a_timeout(monkeypatch, tmp_path_factory):
    succeeded, result = _run_and_report_temporary_files(
        monkeypatch, tmp_path_factory, seconds=30, timeout="5"
    )
    assert not succeeded
    assert result.startswith("The function execution has timed out and has been canceled")


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


def test_whitespace_only_line_inside_string_is_kept():
    src = 'def padded():\n    return """a\n    \nb"""\n'
    succeeded, result = run_in_sandbox_subprocess(src, {})
    assert (succeeded, result) == (True, "a\n    \nb")


def test_source_that_does_not_tokenize_is_rejected_as_invalid():
    with pytest.raises(ValueError, match="Invalid Python source code"):
        run_in_sandbox_subprocess('def unterminated():\n    return """a\n', {})


def test_non_callable_function():
    # run_in_sandbox_subprocess now expects a string containing function source.
    with pytest.raises(TypeError):
        run_in_sandbox_subprocess(12345, {})


def test_runner_script_skips_limits_without_resource_module(monkeypatch, capsys):
    monkeypatch.setitem(sys.modules, "resource", None)
    script = _generate_runner_script(
        inspect.getsource(simple_function), cpu_time_limit=1, memory_limit=1
    )
    namespace = {"__name__": "runner"}

    exec(script, namespace)
    namespace["_limit_resources"]()

    assert capsys.readouterr().err == ""
    assert namespace["simple_function"](x=1, y=2) == 3
