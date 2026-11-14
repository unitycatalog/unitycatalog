import os

import pytest

from unitycatalog.ai.core.envs.base import _EnvironmentVariable


def test_int_env_var(monkeypatch):
    var = _EnvironmentVariable("TEST_INT", int, 42, "Test integer environment variable.")
    assert var.get() == 42

    monkeypatch.setenv("TEST_INT", "100")
    assert var.get() == 100

    monkeypatch.setenv("TEST_INT", "not_an_int")
    with pytest.raises(
        ValueError, match="Failed to convert 'not_an_int' to <class 'int'> for TEST_INT"
    ):
        var.get()


def test_float_env_var(monkeypatch):
    var = _EnvironmentVariable("TEST_FLOAT", float, 3.14, "Test float environment variable.")
    assert var.get() == 3.14

    monkeypatch.setenv("TEST_FLOAT", "2.718")
    assert abs(var.get() - 2.718) < 1e-6

    monkeypatch.setenv("TEST_FLOAT", "not_a_float")
    with pytest.raises(
        ValueError, match="Failed to convert 'not_a_float' to <class 'float'> for TEST_FLOAT"
    ):
        var.get()


def test_string_env_var(monkeypatch):
    var = _EnvironmentVariable("TEST_STR", str, "default", "Test string environment variable.")
    assert var.get() == "default"

    monkeypatch.setenv("TEST_STR", "hello")
    assert var.get() == "hello"


def test_list_env_var_comma_sep(monkeypatch):
    var = _EnvironmentVariable(
        "TEST_LIST",
        list,
        [1, 2, 3],
        "Test list environment variable using comma separation.",
        element_type=int,
    )
    assert var.get() == [1, 2, 3]

    monkeypatch.setenv("TEST_LIST", "4, 5, 6")
    assert var.get() == [4, 5, 6]


def test_list_env_var_json(monkeypatch):
    var = _EnvironmentVariable(
        "TEST_LIST_JSON",
        list,
        ["a", "b"],
        "Test list environment variable using JSON.",
        element_type=str,
    )
    monkeypatch.setenv("TEST_LIST_JSON", '["x", "y", "z"]')
    assert var.get() == ["x", "y", "z"]

    monkeypatch.setenv("TEST_LIST_JSON", "p, q, r")
    assert var.get() == ["p", "q", "r"]


def test_parameterized_list_env_var(monkeypatch):
    # ``list[str]`` must behave the same as ``list`` with ``element_type=str``:
    # the origin type drives the list handling and the type argument the element
    # type. Before the generic was normalized, ``list[str] is list`` was False,
    # so ``get()`` fell through to ``list[str](raw_value)`` and split the string
    # into individual characters.
    var = _EnvironmentVariable(
        "TEST_LIST_STR",
        list[str],
        ["a", "b"],
        "Test list[str] environment variable.",
    )
    assert var.type is list
    assert var.element_type is str
    assert var.get() == ["a", "b"]

    monkeypatch.setenv("TEST_LIST_STR", "sys, socket, os")
    assert var.get() == ["sys", "socket", "os"]

    monkeypatch.setenv("TEST_LIST_STR", '["numpy", "pandas"]')
    assert var.get() == ["numpy", "pandas"]

    var.set(["json", "typing"])
    assert var.get() == ["json", "typing"]

    # The type argument overrides the element type for casting.
    int_var = _EnvironmentVariable(
        "TEST_LIST_INT",
        list[int],
        [1, 2, 3],
        "Test list[int] environment variable.",
    )
    assert int_var.type is list
    assert int_var.element_type is int
    assert int_var.get() == [1, 2, 3]

    monkeypatch.setenv("TEST_LIST_INT", "4, 5, 6")
    assert int_var.get() == [4, 5, 6]


def test_executor_disallowed_modules_returns_full_module_names(monkeypatch):
    from unitycatalog.ai.core.envs.executor_env_vars import EXECUTOR_DISALLOWED_MODULES

    monkeypatch.setenv("EXECUTOR_DISALLOWED_MODULES", "sys, socket, os")
    modules = EXECUTOR_DISALLOWED_MODULES.get()
    # Membership checks in the executor sandbox rely on whole module names; a
    # character-split list (["s", "y", "s", ...]) would never match "os".
    assert modules == ["sys", "socket", "os"]
    assert "os" in modules


def test_set_and_remove(monkeypatch):
    var = _EnvironmentVariable("TEST_ENV", int, 0, "Test set and remove operations.")
    assert var.get() == 0

    var.set(123)
    assert os.getenv("TEST_ENV") == "123"
    assert var.get() == 123

    var.remove()
    assert var.get() == 0


def test_repr():
    var = _EnvironmentVariable("TEST_REPR", str, "default", "Test __repr__ method.")
    rep = repr(var)
    assert "TEST_REPR" in rep
    assert "default" in rep
