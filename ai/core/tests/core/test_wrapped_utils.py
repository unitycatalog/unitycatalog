import textwrap

import pydantic
import pytest
from packaging.version import parse

from unitycatalog.ai.core.utils.callable_utils import (
    extract_wrapped_functions,
    generate_wrapped_sql_function_body,
)
from unitycatalog.ai.core.utils.callable_utils_oss import generate_wrapped_function_info
from unitycatalog.ai.test_utils.function_utils import (
    int_func_no_doc,
    int_func_with_doc,
    str_func_no_doc,
    str_func_with_doc,
    wrap_func_no_doc,
    wrap_func_with_doc,
)

skip_pydantic_v1 = pytest.mark.skipif(
    parse(pydantic.VERSION) < parse("2.0"), reason="Test requires Pydantic 2.x"
)

# ---------------------------
# General Functionality
# ---------------------------


def test_extract_wrapped_functions():
    function_content_no_doc = extract_wrapped_functions([int_func_no_doc, str_func_no_doc])

    assert function_content_no_doc == (
        "def int_func_no_doc(a):\n    return a + 10\n\n"
        'def str_func_no_doc(b):\n    return f"str value: {b}"\n'
    )
    function_content_with_doc = extract_wrapped_functions([int_func_with_doc, str_func_with_doc])

    assert function_content_with_doc == (
        'def int_func_with_doc(a: int) -> int:\n    """\n    A function that takes an integer and returns an integer.\n\n    Args:\n        a: An integer.\n\n    Returns:\n        An integer.\n    """\n    return a + 10\n\n'
        'def str_func_with_doc(b: str) -> str:\n    """\n    A function that takes a string and returns a string.\n\n    Args:\n        b: A string.\n\n    Returns:\n        A string.\n    """\n    return f"str value: {b}"\n'
    )


# --------------------------
# Tests for FunctionInfo-based wrapping utilities (UnityCatalog client)
# --------------------------


@skip_pydantic_v1
def test_generated_wrapped_function_info_no_doc():
    func_info = generate_wrapped_function_info(
        wrap_func_no_doc,
        [int_func_no_doc, str_func_no_doc],
    )
    assert func_info.callable_name == "wrap_func_no_doc"
    assert (
        func_info.routine_definition
        == """
def int_func_no_doc(a):
    return a + 10

def str_func_no_doc(b):
    return f"str value: {b}"

func1 = int_func_no_doc(a)
func2 = str_func_no_doc(b)

return f"{func1} {func2}"
""".strip()
    )
    assert func_info.data_type == "STRING"
    assert func_info.full_data_type == "STRING"
    func_param_info = [info.to_dict() for info in func_info.parameters]
    assert func_param_info[0]["name"] == "a"
    assert func_param_info[0]["type_name"] == "LONG"
    assert func_param_info[0]["type_text"] == "LONG"
    assert func_param_info[0]["comment"] == "An integer."
    assert func_param_info[1]["name"] == "b"
    assert func_param_info[1]["type_name"] == "STRING"
    assert func_param_info[1]["type_text"] == "STRING"
    assert func_param_info[1]["comment"] == "A string."
    assert func_info.comment == "A function that takes two arguments and returns a string."


@skip_pydantic_v1
def test_generated_wrapped_function_info_with_doc():
    func_info = generate_wrapped_function_info(
        wrap_func_with_doc,
        [int_func_with_doc, str_func_with_doc],
    )
    assert func_info.callable_name == "wrap_func_with_doc"
    assert (
        func_info.routine_definition
        == """
def int_func_with_doc(a: int) -> int:
    \"\"\"
    A function that takes an integer and returns an integer.

    Args:
        a: An integer.

    Returns:
        An integer.
    \"\"\"
    return a + 10

def str_func_with_doc(b: str) -> str:
    \"\"\"
    A function that takes a string and returns a string.

    Args:
        b: A string.

    Returns:
        A string.
    \"\"\"
    return f"str value: {b}"

func1 = int_func_with_doc(a)
func2 = str_func_with_doc(b)

return f"{func1} {func2}"
""".strip()
    )
    assert func_info.data_type == "STRING"
    assert func_info.full_data_type == "STRING"
    func_param_info = [info.to_dict() for info in func_info.parameters]
    assert func_param_info[0]["name"] == "a"
    assert func_param_info[0]["type_name"] == "LONG"
    assert func_param_info[0]["type_text"] == "LONG"
    assert func_param_info[0]["comment"] == "An integer."
    assert func_param_info[1]["name"] == "b"
    assert func_param_info[1]["type_name"] == "STRING"
    assert func_param_info[1]["type_name"] == "STRING"
    assert func_param_info[1]["comment"] == "A string."
    assert func_info.comment == "A function that takes two arguments and returns a string."


# --------------------------
# Tests for sql body-based wrapping utilities (Databricks client)
# --------------------------


def test_generate_wrapped_sql_function_body_no_doc():
    expected = textwrap.dedent("""\
        CREATE FUNCTION `test_catalog`.`test_schema`.`wrap_func_no_doc`(a LONG COMMENT 'An integer.', b STRING COMMENT 'A string.')
        RETURNS STRING
        LANGUAGE PYTHON
        COMMENT 'A function that takes two arguments and returns a string.'
        AS $$
            def int_func_no_doc(a):
                return a + 10

            def str_func_no_doc(b):
                return f"str value: {b}"

            func1 = int_func_no_doc(a)
            func2 = str_func_no_doc(b)

            return f"{func1} {func2}"
        $$;
    """).strip()

    wrapped_sql_body_no_doc = generate_wrapped_sql_function_body(
        wrap_func_no_doc, [int_func_no_doc, str_func_no_doc], "test_catalog", "test_schema"
    ).strip()

    assert wrapped_sql_body_no_doc == expected


def test_generate_wrapped_sql_function_body_with_doc():
    expected = textwrap.dedent("""\
        CREATE FUNCTION `test_catalog`.`test_schema`.`wrap_func_with_doc`(a LONG COMMENT 'An integer.', b STRING COMMENT 'A string.')
        RETURNS STRING
        LANGUAGE PYTHON
        COMMENT 'A function that takes two arguments and returns a string.'
        AS $$
            def int_func_with_doc(a: int) -> int:
                \"\"\"
                A function that takes an integer and returns an integer.

                Args:
                    a: An integer.

                Returns:
                    An integer.
                \"\"\"
                return a + 10

            def str_func_with_doc(b: str) -> str:
                \"\"\"
                A function that takes a string and returns a string.

                Args:
                    b: A string.

                Returns:
                    A string.
                \"\"\"
                return f"str value: {b}"

            func1 = int_func_with_doc(a)
            func2 = str_func_with_doc(b)

            return f"{func1} {func2}"
        $$;
    """).strip()

    wrapped_sql_body_with_doc = generate_wrapped_sql_function_body(
        wrap_func_with_doc, [int_func_with_doc, str_func_with_doc], "test_catalog", "test_schema"
    ).strip()

    assert wrapped_sql_body_with_doc == expected
