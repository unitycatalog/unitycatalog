import pytest

from unitycatalog.ai.core.utils.execution_utils import load_function_from_string


def test_load_function_default_registration():
    func_str = """
def add_numbers(a: int, b: int) -> int:
    \"\"\"Adds two numbers.\"\"\"
    return a + b
"""
    test_namespace = {}
    func = load_function_from_string(func_str, register_function=True, namespace=test_namespace)
    assert callable(func)
    assert func.__name__ == "add_numbers"
    assert "add_numbers" in test_namespace
    assert test_namespace["add_numbers"] is func
    assert func(2, 3) == 5


def test_load_function_no_registration():
    func_str = """
def multiply_numbers(a: int, b: int) -> int:
    \"\"\"Multiplies two numbers.\"\"\"
    return a * b
"""
    globals().pop("multiply_numbers", None)

    func = load_function_from_string(func_str, register_function=False)
    assert callable(func)
    assert func.__name__ == "multiply_numbers"
    assert globals().get("multiply_numbers") is None
    assert func(3, 4) == 12


def test_load_function_invalid_string():
    invalid_str = "This is not a function definition"
    with pytest.raises(ValueError, match="Function definition not found in the provided string."):
        load_function_from_string(invalid_str)


def test_load_function_ast_name_extraction():
    func_str = """
def greet(name: str) -> str:
    \"\"\"Greet someone.\"\"\"
    return f"Hello, {name}!"
"""
    globals().pop("greet", None)
    func = load_function_from_string(func_str, register_function=False)
    assert func.__name__ == "greet"
    assert func("Alice") == "Hello, Alice!"
    assert globals().get("greet") is None
