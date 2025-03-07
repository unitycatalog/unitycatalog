import re
import textwrap
import warnings
from typing import Any, Dict, List, Optional, Tuple, Union

import pytest

from unitycatalog.ai.core.types import Variant
from unitycatalog.ai.core.utils.callable_utils import (
    _parse_routine_definition,
    _parse_sql_data_type,
    dynamically_construct_python_function,
    generate_sql_function_body,
)
from unitycatalog.ai.core.utils.type_utils import SQL_TYPE_TO_PYTHON_TYPE_MAPPING

# ---------------------------
# Tests for Simple Functions and Docstrings
# ---------------------------


def test_simple_function_no_docstring():
    def simple_func(a: int, b: int) -> int:
        """
        Simple addition
        Args:
            a: Parameter a
            b: Parameter b
        """
        return a + b

    sql_body = generate_sql_function_body(simple_func, "test_catalog", "test_schema", True)

    expected_sql = """
CREATE OR REPLACE FUNCTION `test_catalog`.`test_schema`.`simple_func`(a LONG COMMENT 'Parameter a', b LONG COMMENT 'Parameter b')
RETURNS LONG
LANGUAGE PYTHON
COMMENT 'Simple addition'
AS $$
    return a + b
$$;
    """
    assert sql_body.strip() == expected_sql.strip()


def test_function_with_multiline_docstring():
    def multiline_docstring_func(a: int, b: int) -> str:
        """
        A function with a multiline docstring.

        This docstring spans multiple lines and
        describes the function in detail.

        Args:
            a: The first number
            b: The second number

        Returns:
            str: The string representation of the sum of a and b
        """
        return str(a + b)

    sql_body = generate_sql_function_body(multiline_docstring_func, "test_catalog", "test_schema")

    expected_sql = """
CREATE FUNCTION `test_catalog`.`test_schema`.`multiline_docstring_func`(a LONG COMMENT 'The first number', b LONG COMMENT 'The second number')
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'A function with a multiline docstring. This docstring spans multiple lines and describes the function in detail.'
AS $$
    return str(a + b)
$$;
    """
    assert sql_body.strip() == expected_sql.strip()


def test_function_with_detailed_docstring():
    def detailed_func(a: int, b: int) -> int:
        """
        A detailed function example.

        Args:
            a: The first number
            b: The second number

        Returns:
            int: The sum of a and b

        Raises:
            ValueError: If a or b are negative
        """
        if a < 0 or b < 0:
            raise ValueError("Both numbers must be non-negative")
        return a + b

    sql_body = generate_sql_function_body(detailed_func, "test_catalog", "test_schema", True)

    expected_sql = """
CREATE OR REPLACE FUNCTION `test_catalog`.`test_schema`.`detailed_func`(a LONG COMMENT 'The first number', b LONG COMMENT 'The second number')
RETURNS LONG
LANGUAGE PYTHON
COMMENT 'A detailed function example.'
AS $$
    if a < 0 or b < 0:
        raise ValueError("Both numbers must be non-negative")
    return a + b
$$;
"""
    assert sql_body.strip() == expected_sql.strip()


def test_function_with_google_docstring():
    def my_function(a: int, b: str) -> float:
        """
        This function adds the length of a string to an integer.

        Args:
            a (int): The integer to add to.
            b (str): The string to get the length of.

        Returns:
            float: The sum of the integer and the length of the string.
        """
        return a + len(b)

    sql_body = generate_sql_function_body(my_function, "test_catalog", "test_schema")

    expected_sql = """
CREATE FUNCTION `test_catalog`.`test_schema`.`my_function`(a LONG COMMENT 'The integer to add to.', b STRING COMMENT 'The string to get the length of.')
RETURNS DOUBLE
LANGUAGE PYTHON
COMMENT 'This function adds the length of a string to an integer.'
AS $$
    return a + len(b)
$$;
    """
    assert sql_body.strip() == expected_sql.strip()


def test_function_with_multiline_argument_description():
    def my_multiline_arg_function(a: int, b: str) -> str:
        """
        This function has a multi-line argument list.

        Args:
            a: The first argument, which is an integer.
               The integer is guaranteed to be positive.
            b: The second argument, which is a string.
               The string should be more than 100 characters long.

        Returns:
            str: A string that concatenates the integer and string.
        """
        return f"{a}-{b}"

    sql_body = generate_sql_function_body(
        my_multiline_arg_function, "test_catalog", "test_schema", True
    )

    expected_sql = """
CREATE OR REPLACE FUNCTION `test_catalog`.`test_schema`.`my_multiline_arg_function`(a LONG COMMENT 'The first argument, which is an integer. The integer is guaranteed to be positive.', b STRING COMMENT 'The second argument, which is a string. The string should be more than 100 characters long.')
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'This function has a multi-line argument list.'
AS $$
    return f"{a}-{b}"
$$;
    """
    assert sql_body.strip() == expected_sql.strip()


def test_function_with_extra_docstring_params_ignored():
    def func_with_extra_param_in_docstring(a: int) -> str:
        """
        A function with extra parameter in docstring.

        Args:
            a: The first argument
            b: An extra parameter not in function signature

        Returns:
            str: The string representation of the first argument
        """
        return str(a)

    # We expect the generated SQL to ignore 'b' since it's not in the function signature
    with pytest.warns(
        UserWarning,
        match="The following parameters are documented in the docstring but not present in the function signature: b",
    ) as record:
        sql_body = generate_sql_function_body(
            func_with_extra_param_in_docstring, "test_catalog", "test_schema"
        )

    expected_sql = """
CREATE FUNCTION `test_catalog`.`test_schema`.`func_with_extra_param_in_docstring`(a LONG COMMENT 'The first argument')
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'A function with extra parameter in docstring.'
AS $$
    return str(a)
$$;
    """

    assert sql_body.strip() == expected_sql.strip()

    assert len(record) == 1


# ---------------------------
# Tests for environment dependency specifications
# ---------------------------


def test_environment_dependencies_formatting():
    def func_with_env_deps(a: int) -> str:
        """
        A function that requires specific environment dependencies.

        Args:
            a: An integer

        Returns:
            str: A string representation of the integer
        """
        return str(a)

    sql_body = generate_sql_function_body(
        func_with_env_deps, "test_catalog", "test_schema", True, ["numpy", "pandas"]
    )

    expected_sql = """
CREATE OR REPLACE FUNCTION `test_catalog`.`test_schema`.`func_with_env_deps`(a LONG COMMENT 'An integer')
RETURNS STRING
LANGUAGE PYTHON
ENVIRONMENT (dependencies = '["numpy", "pandas"]', environment_version = 'None')
COMMENT 'A function that requires specific environment dependencies.'
AS $$
    return str(a)
$$;
    """

    assert sql_body.strip() == expected_sql.strip()


def test_environment_dependencies_formatting_env_specified():
    def func_with_env_deps(a: int) -> str:
        """
        A function that requires specific environment dependencies.

        Args:
            a: An integer

        Returns:
            str: A string representation of the integer
        """
        return str(a)

    sql_body = generate_sql_function_body(
        func_with_env_deps, "test_catalog", "test_schema", True, ["numpy", "pandas"], "1"
    )

    expected_sql = """
CREATE OR REPLACE FUNCTION `test_catalog`.`test_schema`.`func_with_env_deps`(a LONG COMMENT 'An integer')
RETURNS STRING
LANGUAGE PYTHON
ENVIRONMENT (dependencies = '["numpy", "pandas"]', environment_version = '1')
COMMENT 'A function that requires specific environment dependencies.'
AS $$
    return str(a)
$$;
    """

    assert sql_body.strip() == expected_sql.strip()


def test_environment_version_formatting_no_deps():
    def func_with_env_deps(a: int) -> str:
        """
        A function that requires specific environment dependencies.

        Args:
            a: An integer

        Returns:
            str: A string representation of the integer
        """
        return str(a)

    sql_body = generate_sql_function_body(
        func_with_env_deps, "test_catalog", "test_schema", True, environment_version="1"
    )

    expected_sql = """
CREATE OR REPLACE FUNCTION `test_catalog`.`test_schema`.`func_with_env_deps`(a LONG COMMENT 'An integer')
RETURNS STRING
LANGUAGE PYTHON
ENVIRONMENT (environment_version = '1')
COMMENT 'A function that requires specific environment dependencies.'
AS $$
    return str(a)
$$;
    """

    assert sql_body.strip() == expected_sql.strip()


# ---------------------------
# Tests for Nested Functions and Classes
# ---------------------------


def test_function_with_nested():
    def outer_func(x: int, y: int) -> str:
        """
        A function that demonstrates nested functions.

        Args:
            x: The x parameter
            y: The y parameter

        Returns:
            str: A string representation of the sum of x and y
        """

        def inner_func(a: int) -> int:
            return a + y

        return str(inner_func(x))

    sql_body = generate_sql_function_body(outer_func, "test_catalog", "test_schema")

    expected_sql = """
CREATE FUNCTION `test_catalog`.`test_schema`.`outer_func`(x LONG COMMENT 'The x parameter', y LONG COMMENT 'The y parameter')
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'A function that demonstrates nested functions.'
AS $$
    def inner_func(a: int) -> int:
        return a + y

    return str(inner_func(x))
$$;
    """
    assert sql_body.strip() == expected_sql.strip()


def test_function_with_class():
    def func_with_class(a: int) -> str:
        """
        A function that defines a class inside.

        Args:
            a: The parameter a

        Returns:
            str: A string representation of the object created
        """

        class Example:
            def __init__(self, val: int):
                self.val = val

            def double(self) -> int:
                return self.val * 2

        obj = Example(a)
        return str(obj.double())

    sql_body = generate_sql_function_body(func_with_class, "test_catalog", "test_schema", True)

    expected_sql = """
CREATE OR REPLACE FUNCTION `test_catalog`.`test_schema`.`func_with_class`(a LONG COMMENT 'The parameter a')
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'A function that defines a class inside.'
AS $$
    class Example:
        def __init__(self, val: int):
            self.val = val

        def double(self) -> int:
            return self.val * 2

    obj = Example(a)
    return str(obj.double())
$$;
    """
    assert sql_body.strip() == expected_sql.strip()


def test_function_with_lambda():
    def lambda_func(x: int) -> str:
        """
        A function with a lambda expression.

        Args:
            x: The input value

        Returns:
            str: A string representation of the lambda result
        """
        square = lambda a: a * a
        return str(square(x))

    sql_body = generate_sql_function_body(lambda_func, "test_catalog", "test_schema", True)

    expected_sql = """
CREATE OR REPLACE FUNCTION `test_catalog`.`test_schema`.`lambda_func`(x LONG COMMENT 'The input value')
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'A function with a lambda expression.'
AS $$
    square = lambda a: a * a
    return str(square(x))
$$;
    """
    assert sql_body.strip() == expected_sql.strip()


def test_function_with_heavily_nested_structure():
    def func_with_heavily_nested(a: List[Dict[str, List[Dict[str, int]]]]) -> str:
        """
        A function that accepts a heavily nested structure of lists and dictionaries.

        Args:
            a: A list of dictionaries where the key is a string and the value is a list of dictionaries
               with string keys and integer values.

        Returns:
            str: A string representation of the nested structure
        """
        return str(a)

    sql_body = generate_sql_function_body(func_with_heavily_nested, "test_catalog", "test_schema")

    expected_sql = """
CREATE FUNCTION `test_catalog`.`test_schema`.`func_with_heavily_nested`(a ARRAY<MAP<STRING, ARRAY<MAP<STRING, LONG>>>> COMMENT 'A list of dictionaries where the key is a string and the value is a list of dictionaries with string keys and integer values.')
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'A function that accepts a heavily nested structure of lists and dictionaries.'
AS $$
    return str(a)
$$;
    """

    assert sql_body.strip() == expected_sql.strip()


# ---------------------------
# Tests for Decorators
# ---------------------------


def test_function_with_decorator():
    @staticmethod
    def decorated_func(a: int, b: int) -> int:
        """
        A static method decorated function.

        Args:
            a: First integer
            b: Second integer

        Returns:
            int: Sum of a and b
        """
        return a + b

    sql_body = generate_sql_function_body(decorated_func, "test_catalog", "test_schema", True)

    expected_sql = """
CREATE OR REPLACE FUNCTION `test_catalog`.`test_schema`.`decorated_func`(a LONG COMMENT 'First integer', b LONG COMMENT 'Second integer')
RETURNS LONG
LANGUAGE PYTHON
COMMENT 'A static method decorated function.'
AS $$
    return a + b
$$;
    """
    assert sql_body.strip() == expected_sql.strip()


# ---------------------------
# Tests for Error Handling
# ---------------------------


def test_function_with_try_except():
    def try_except_func(a: int, b: int) -> int:
        """
        A function with try-except block.

        Args:
            a: First number
            b: Second number

        Returns:
            int: Sum of a and b
        """
        try:
            return a + b
        except Exception as e:
            raise ValueError(f"Invalid operation") from e

    sql_body = generate_sql_function_body(try_except_func, "test_catalog", "test_schema")

    expected_sql = """
CREATE FUNCTION `test_catalog`.`test_schema`.`try_except_func`(a LONG COMMENT 'First number', b LONG COMMENT 'Second number')
RETURNS LONG
LANGUAGE PYTHON
COMMENT 'A function with try-except block.'
AS $$
    try:
        return a + b
    except Exception as e:
        raise ValueError(f"Invalid operation") from e
$$;
    """
    assert sql_body.strip() == expected_sql.strip()


def test_function_with_multiple_return_paths():
    def multiple_return_func(a: int) -> str:
        """
        A function with multiple return paths.
        Args:
            a: An integer
        """
        if a > 0:
            return "Positive"
        else:
            return "Negative"

    sql_body = generate_sql_function_body(multiple_return_func, "test_catalog", "test_schema", True)

    expected_sql = """
CREATE OR REPLACE FUNCTION `test_catalog`.`test_schema`.`multiple_return_func`(a LONG COMMENT 'An integer')
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'A function with multiple return paths.'
AS $$
    if a > 0:
        return "Positive"
    else:
        return "Negative"
$$;
    """
    assert sql_body.strip() == expected_sql.strip()


def test_function_returning_any():
    def func_returning_any(a: int) -> Any:
        """
        A function that returns Any type.

        Args:
            a: An integer
        """
        return a

    with pytest.raises(
        ValueError,
        match="Error in return type for function 'func_returning_any': typing.Any. 'Any' type is not supported. Please specify a concrete return type.",
    ):
        generate_sql_function_body(func_returning_any, "test_catalog", "test_schema")


def test_function_returning_union():
    def func_returning_union(a: int) -> Union[int, str]:
        """
        A function that returns a Union of int and str.

        Args:
            a: An integer

        Returns:
            Union[int, str]: Either an integer or a string
        """
        if a > 0:
            return a
        return str(a)

    with pytest.raises(
        ValueError,
        match=re.escape(
            "Error in return type for function 'func_returning_union': typing.Union[int, str]. Union types are not supported in return types."
        ),
    ):
        generate_sql_function_body(func_returning_union, "test_catalog", "test_schema")


# ---------------------------
# Tests for Input Types
# ---------------------------


def test_function_with_list_input():
    def func_with_list(a: List[int]) -> str:
        """
        A function that accepts a list of integers.

        Args:
            a: A list of integers

        Returns:
            str: A string representation of the list
        """
        return str(a)

    sql_body = generate_sql_function_body(func_with_list, "test_catalog", "test_schema", True)

    expected_sql = """
CREATE OR REPLACE FUNCTION `test_catalog`.`test_schema`.`func_with_list`(a ARRAY<LONG> COMMENT 'A list of integers')
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'A function that accepts a list of integers.'
AS $$
    return str(a)
$$;
    """
    assert sql_body.strip() == expected_sql.strip()


def test_function_with_map_input():
    def func_with_map(a: Dict[str, int]) -> str:
        """
        A function that accepts a map with string keys and integer values.

        Args:
            a: A map with string keys and integer values

        Returns:
            str: A string representation of the map
        """
        return str(a)

    sql_body = generate_sql_function_body(func_with_map, "test_catalog", "test_schema")

    expected_sql = """
CREATE FUNCTION `test_catalog`.`test_schema`.`func_with_map`(a MAP<STRING, LONG> COMMENT 'A map with string keys and integer values')
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'A function that accepts a map with string keys and integer values.'
AS $$
    return str(a)
$$;
    """
    assert sql_body.strip() == expected_sql.strip()


def test_function_with_dict_list_input():
    def func_with_dict_list(a: Dict[str, List[str]]) -> str:
        """
        A function that accepts a dictionary with string keys and list of string values.

        Args:
            a: A dictionary with string keys and list of string values

        Returns:
            str: A string representation of the dictionary
        """
        return str(a)

    sql_body = generate_sql_function_body(func_with_dict_list, "test_catalog", "test_schema", True)

    expected_sql = """
CREATE OR REPLACE FUNCTION `test_catalog`.`test_schema`.`func_with_dict_list`(a MAP<STRING, ARRAY<STRING>> COMMENT 'A dictionary with string keys and list of string values')
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'A function that accepts a dictionary with string keys and list of string values.'
AS $$
    return str(a)
$$;
    """
    assert sql_body.strip() == expected_sql.strip()


def test_function_with_list_of_dict_input():
    def func_with_list_of_map(a: List[Dict[str, int]]) -> str:
        """
        A function that accepts a list of maps with string keys and integer values.

        Args:
            a: A list of maps with string keys and integer values

        Returns:
            str: A string representation of the list of maps
        """
        return str(a)

    sql_body = generate_sql_function_body(func_with_list_of_map, "test_catalog", "test_schema")

    expected_sql = """
CREATE FUNCTION `test_catalog`.`test_schema`.`func_with_list_of_map`(a ARRAY<MAP<STRING, LONG>> COMMENT 'A list of maps with string keys and integer values')
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'A function that accepts a list of maps with string keys and integer values.'
AS $$
    return str(a)
$$;
    """
    assert sql_body.strip() == expected_sql.strip()


def test_function_with_variant_param():
    def func_with_variant(a: Variant) -> str:
        """
        A function that accepts a VARIANT parameter.

        Args:
            a: A variant parameter.

        Returns:
            str: The string representation of the variant.
        """
        return str(a)

    sql_body = generate_sql_function_body(func_with_variant, "test_catalog", "test_schema", True)
    expected_sql = """
CREATE OR REPLACE FUNCTION `test_catalog`.`test_schema`.`func_with_variant`(a VARIANT COMMENT 'A variant parameter.')
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'A function that accepts a VARIANT parameter.'
AS $$
    return str(a)
$$;
    """
    assert sql_body.strip() == expected_sql.strip()


def test_function_with_list_of_variant():
    from typing import List

    def func_list_of_variant(a: List[Variant]) -> str:
        """
        A function that accepts a list of VARIANTs.

        Args:
            a: A list of variant values.

        Returns:
            str: The string representation of the list.
        """
        return str(a)

    sql_body = generate_sql_function_body(func_list_of_variant, "test_catalog", "test_schema", True)
    expected_sql = """
CREATE OR REPLACE FUNCTION `test_catalog`.`test_schema`.`func_list_of_variant`(a ARRAY<VARIANT> COMMENT 'A list of variant values.')
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'A function that accepts a list of VARIANTs.'
AS $$
    return str(a)
$$;
    """
    assert sql_body.strip() == expected_sql.strip()


def test_function_with_dict_of_variant():
    from typing import Dict

    def func_dict_of_variant(a: Dict[str, Variant]) -> str:
        """
        A function that accepts a dictionary with string keys and VARIANT values.

        Args:
            a: A dictionary of variant values.

        Returns:
            str: The string representation of the dictionary.
        """
        return str(a)

    sql_body = generate_sql_function_body(func_dict_of_variant, "test_catalog", "test_schema", True)
    expected_sql = """
CREATE OR REPLACE FUNCTION `test_catalog`.`test_schema`.`func_dict_of_variant`(a MAP<STRING, VARIANT> COMMENT 'A dictionary of variant values.')
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'A function that accepts a dictionary with string keys and VARIANT values.'
AS $$
    return str(a)
$$;
    """
    assert sql_body.strip() == expected_sql.strip()


# ---------------------------
# Tests for Return Types
# ---------------------------


def test_function_with_list_return():
    def func_with_list_return() -> List[int]:
        """
        A function that returns a list of integers.

        Returns:
            list: A list of integers
        """
        return [1, 2, 3]

    sql_body = generate_sql_function_body(
        func_with_list_return, "test_catalog", "test_schema", True
    )

    expected_sql = """
CREATE OR REPLACE FUNCTION `test_catalog`.`test_schema`.`func_with_list_return`()
RETURNS ARRAY<LONG>
LANGUAGE PYTHON
COMMENT 'A function that returns a list of integers.'
AS $$
    return [1, 2, 3]
$$;
    """
    assert sql_body.strip() == expected_sql.strip()


def test_function_with_map_return():
    def func_with_map_return() -> Dict[str, int]:
        """
        A function that returns a map with string keys and integer values.

        Returns:
            dict: A map with string keys and integer values
        """
        return {"a": 1, "b": 2}

    sql_body = generate_sql_function_body(func_with_map_return, "test_catalog", "test_schema")

    expected_sql = """
CREATE FUNCTION `test_catalog`.`test_schema`.`func_with_map_return`()
RETURNS MAP<STRING, LONG>
LANGUAGE PYTHON
COMMENT 'A function that returns a map with string keys and integer values.'
AS $$
    return {"a": 1, "b": 2}
$$;
    """
    assert sql_body.strip() == expected_sql.strip()


def test_function_with_complex_return_type():
    def complex_return_func() -> dict:
        """
        A function with a complex return type.

        Returns:
            dict: A dictionary with a string key and a list of integers as value
        """
        return {"numbers": [1, 2, 3]}

    sql_body = generate_sql_function_body(complex_return_func, "test_catalog", "test_schema", True)

    expected_sql = """
CREATE OR REPLACE FUNCTION `test_catalog`.`test_schema`.`complex_return_func`()
RETURNS MAP
LANGUAGE PYTHON
COMMENT 'A function with a complex return type.'
AS $$
    return {"numbers": [1, 2, 3]}
$$;
    """
    assert sql_body.strip() == expected_sql.strip()


def test_function_with_variant_return():
    def func_variant_return() -> Variant:
        """
        A function that returns a VARIANT.

        Returns:
            Variant: A variant value.
        """
        return {"key": 123}

    sql_body = generate_sql_function_body(func_variant_return, "test_catalog", "test_schema", True)
    expected_sql = """
CREATE OR REPLACE FUNCTION `test_catalog`.`test_schema`.`func_variant_return`()
RETURNS VARIANT
LANGUAGE PYTHON
COMMENT 'A function that returns a VARIANT.'
AS $$
    return {"key": 123}
$$;
    """
    assert sql_body.strip() == expected_sql.strip()


# ---------------------------
# Tests for Missing or Incomplete Docstrings
# ---------------------------


def test_function_without_docstring():
    """Test that a function without a docstring raises an exception."""

    def func_without_docstring(a: int, b: int) -> int:
        return a + b

    with pytest.raises(
        ValueError,
        match="Function 'func_without_docstring' must have a docstring with a description.",
    ):
        generate_sql_function_body(func_without_docstring, "test_catalog", "test_schema")


def test_function_with_empty_docstring():
    """Test that a function with an empty docstring raises an exception."""

    def func_with_empty_docstring(a: int, b: int) -> int:
        """ """
        return a + b

    with pytest.raises(
        ValueError,
        match="Function 'func_with_empty_docstring' must have a docstring with a description.",
    ):
        generate_sql_function_body(func_with_empty_docstring, "test_catalog", "test_schema")


def test_function_with_docstring_no_description():
    """Test that a function with no description in the docstring raises an exception."""

    def func_with_no_description(a: int, b: int) -> int:
        """
        Args:
            a: First integer.
            b: Second integer.

        Returns:
            int: Sum of a and b.
        """
        return a + b

    with pytest.raises(
        ValueError,
        match="Function description is missing in the docstring. Please provide a function description.",
    ):
        generate_sql_function_body(func_with_no_description, "test_catalog", "test_schema")


# ---------------------------
# Tests for Invalid Types
# ---------------------------


def test_function_with_invalid_list_type():
    def func_with_invalid_list(a: List[Any]) -> str:
        """
        A function that accepts a list of any type.

        Args:
            a: A list of any type

        Returns:
            str: A string representation of the list
        """
        return str(a)

    with pytest.raises(
        ValueError,
        match=re.escape(
            "Error in parameter 'a': List type requires a specific element type. Please define the internal type for the list, e.g., List[int]. Original error: Unsupported Python type: typing.Any is not allowed. Please specify a concrete type."
        ),
    ):
        generate_sql_function_body(func_with_invalid_list, "test_catalog", "test_schema")


def test_function_with_invalid_map_type():
    def func_with_invalid_map(a: Dict[str, Any]) -> str:
        """
        A function that accepts a map with string keys and any values.

        Args:
            a: A map with string keys and any values

        Returns:
            str: A string representation of the map
        """
        return str(a)

    with pytest.raises(
        ValueError,
        match=re.escape(
            "Error in parameter 'a': Dict type requires both key and value types. Please define the internal types for the dict, e.g., Dict[str, int]. Original error: Unsupported Python type: typing.Any is not allowed. Please specify a concrete type."
        ),
    ):
        generate_sql_function_body(func_with_invalid_map, "test_catalog", "test_schema")


def test_function_with_invalid_list_return():
    def func_with_invalid_list_return() -> List[Any]:
        """
        A function that returns a list of any type.

        Returns:
            list: A list of any type
        """
        return [1, "string", True]

    with pytest.raises(
        ValueError,
        match=re.escape(
            "Error in return type for function 'func_with_invalid_list_return': typing.List[typing.Any]. Unsupported return type: typing.List[typing.Any]."
        ),
    ):
        generate_sql_function_body(func_with_invalid_list_return, "test_catalog", "test_schema")


def test_function_with_invalid_map_return():
    def func_with_invalid_map_return() -> Dict[str, Any]:
        """
        A function that returns a map with string keys and any values.

        Returns:
            dict: A map with string keys and any values
        """
        return {"a": 1, "b": "string"}

    with pytest.raises(
        ValueError,
        match=re.escape(
            "Error in return type for function 'func_with_invalid_map_return': typing.Dict[str, typing.Any]. Unsupported return type: typing.Dict[str, typing.Any]."
        ),
    ):
        generate_sql_function_body(func_with_invalid_map_return, "test_catalog", "test_schema")


def test_function_with_plain_list_type():
    def func_with_plain_list_type(a: List) -> str:
        """
        A function with a plain List as a parameter type.

        Args:
            a: A plain list without inner types

        Returns:
            str: A string representation of the list
        """
        return str(a)

    with pytest.raises(
        ValueError,
        match=re.escape(
            "Error in parameter 'a': List type requires a specific element type. Please define the internal type for the list, e.g., List[int]. Original error: Unsupported Python type: typing.List or typing.Tuple requires an element type."
        ),
    ):
        generate_sql_function_body(func_with_plain_list_type, "test_catalog", "test_schema")


def test_function_with_plain_dict_type():
    def func_with_plain_dict_type(a: Dict) -> str:
        """
        A function with a plain Dict as a parameter type.

        Args:
            a: A plain dict without inner types

        Returns:
            str: A string representation of the dict
        """
        return str(a)

    with pytest.raises(
        ValueError,
        match=re.escape(
            "Dict type requires both key and value types. Please define the internal types for the dict, e.g., Dict[str, int]. Original error: Unsupported Python type: typing.Dict requires key and value types."
        ),
    ):
        generate_sql_function_body(func_with_plain_dict_type, "test_catalog", "test_schema")


def test_function_with_plain_list_return_type():
    def func_with_plain_list_return() -> List:
        """
        A function with a plain List as a return type.

        Returns:
            list: A plain list without inner types
        """
        return [1, 2, 3]

    with pytest.raises(
        ValueError,
        match=re.escape(
            "Error in return type for function 'func_with_plain_list_return': typing.List. Please define the inner types, e.g., List[int], Tuple[str, int], Dict[str, int]."
        ),
    ):
        generate_sql_function_body(func_with_plain_list_return, "test_catalog", "test_schema")


def test_function_with_plain_dict_return_type():
    def func_with_plain_dict_return() -> Dict:
        """
        A function with a plain Dict as a return type.

        Returns:
            dict: A plain dict without inner types
        """
        return {"key": "value"}

    with pytest.raises(
        ValueError,
        match=re.escape(
            "Error in return type for function 'func_with_plain_dict_return': typing.Dict. Please define the inner types, e.g., List[int], Tuple[str, int], Dict[str, int]."
        ),
    ):
        generate_sql_function_body(func_with_plain_dict_return, "test_catalog", "test_schema")


def test_function_with_unsupported_return_type():
    class CustomType:
        pass

    def unsupported_return_type_func() -> CustomType:
        return CustomType()

    with pytest.raises(ValueError, match="Error in return type"):
        generate_sql_function_body(unsupported_return_type_func, "test_catalog", "test_schema")


def test_function_with_unsupported_param_type():
    def unsupported_param_type_func(a: object) -> str:
        """
        Unsupported type hint

        Args:
            a: An object
        """
        return str(a)

    with pytest.raises(ValueError, match="Error in parameter 'a'"):
        generate_sql_function_body(unsupported_param_type_func, "test_catalog", "test_schema")


def test_function_without_return_type_hints():
    def no_return_type_hint_func(a: int, b: int):
        """
        No return type hint
        Args:
            a: First integer
            b: Second integer
        """
        return a + b

    with pytest.raises(
        ValueError, match="Return type for function 'no_return_type_hint_func' is not defined"
    ):
        generate_sql_function_body(no_return_type_hint_func, "test_catalog", "test_schema")


def test_function_without_arg_type_hints():
    def no_arg_type_hint_func(a, b) -> int:
        """
        No arg type hints

        Args:
            a: First integer
            b: Second integer
        """
        return a + b

    with pytest.raises(ValueError, match="Missing type hint for parameter: a"):
        generate_sql_function_body(no_arg_type_hint_func, "test_catalog", "test_schema")


# ---------------------------
# Tests for Optional Parameters and Defaults
# ---------------------------


def test_function_with_optional_default_values():
    def func_with_optional(a: int, b: int = 10, c: str = "default") -> str:
        """
        A function that demonstrates optional parameters with default values.

        Args:
            a: Required integer.
            b: Optional integer with default 10.
            c: Optional string with default "default".

        Returns:
            str: A concatenated string of the inputs.
        """
        return f"{a}-{b}-{c}"

    sql_body = generate_sql_function_body(func_with_optional, "test_catalog", "test_schema")

    expected_sql = """
CREATE FUNCTION `test_catalog`.`test_schema`.`func_with_optional`(a LONG COMMENT 'Required integer.', b LONG DEFAULT 10 COMMENT 'Optional integer with default 10.', c STRING DEFAULT 'default' COMMENT 'Optional string with default "default".')
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'A function that demonstrates optional parameters with default values.'
AS $$
    return f"{a}-{b}-{c}"
$$;
    """
    assert sql_body.strip() == expected_sql.strip()


def test_function_with_mixed_required_and_default_values():
    def func_with_mixed(a: int, b: int = 5, c: int = 20) -> int:
        """
        A function with both required and optional parameters.

        Args:
            a: Required parameter.
            b: Optional parameter with default value 5.
            c: Optional parameter with default value 20.

        Returns:
            int: The sum of a, b, and c.
        """
        return a + b + c

    sql_body = generate_sql_function_body(func_with_mixed, "test_catalog", "test_schema", True)

    expected_sql = """
CREATE OR REPLACE FUNCTION `test_catalog`.`test_schema`.`func_with_mixed`(a LONG COMMENT 'Required parameter.', b LONG DEFAULT 5 COMMENT 'Optional parameter with default value 5.', c LONG DEFAULT 20 COMMENT 'Optional parameter with default value 20.')
RETURNS LONG
LANGUAGE PYTHON
COMMENT 'A function with both required and optional parameters.'
AS $$
    return a + b + c
$$;
    """
    assert sql_body.strip() == expected_sql.strip()


def test_function_with_default_string_value():
    def func_with_default_str(a: str = "hello") -> str:
        """
        A function with an optional string parameter.

        Args:
            a: Optional string parameter with default value "hello".

        Returns:
            str: The string 'hello' or the input value.
        """
        return a

    sql_body = generate_sql_function_body(func_with_default_str, "test_catalog", "test_schema")

    expected_sql = """
CREATE FUNCTION `test_catalog`.`test_schema`.`func_with_default_str`(a STRING DEFAULT 'hello' COMMENT 'Optional string parameter with default value "hello".')
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'A function with an optional string parameter.'
AS $$
    return a
$$;
    """
    assert sql_body.strip() == expected_sql.strip()


def test_function_with_default_string_value_using_single_quote():
    def func_with_default_str(a: str = "hello") -> str:
        """
        A function with an optional string parameter.
        Args:
            a: Optional string parameter with default value 'hello'.

        Returns:
            str: The string 'hello' or the input value.
        """
        return a

    sql_body = generate_sql_function_body(
        func_with_default_str, "test_catalog", "test_schema", True
    )

    expected_sql = """
CREATE OR REPLACE FUNCTION `test_catalog`.`test_schema`.`func_with_default_str`(a STRING DEFAULT 'hello' COMMENT 'Optional string parameter with default value "hello".')
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'A function with an optional string parameter.'
AS $$
    return a
$$;
    """
    assert sql_body.strip() == expected_sql.strip()


def test_function_with_default_numeric_and_string():
    def func_with_numeric_and_string(a: int = 5, b: str = "foo") -> str:
        """
        A function with both numeric and string default parameters.

        Args:
            a: Optional integer with default value 5.
            b: Optional string with default value "foo".

        Returns:
            str: A concatenation of the string 'b' repeated 'a' times.
        """
        return b * a

    sql_body = generate_sql_function_body(
        func_with_numeric_and_string, "test_catalog", "test_schema"
    )

    expected_sql = """
CREATE FUNCTION `test_catalog`.`test_schema`.`func_with_numeric_and_string`(a LONG DEFAULT 5 COMMENT 'Optional integer with default value 5.', b STRING DEFAULT 'foo' COMMENT 'Optional string with default value "foo".')
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'A function with both numeric and string default parameters.'
AS $$
    return b * a
$$;
    """
    assert sql_body.strip() == expected_sql.strip()


def test_function_with_optional_parameter():
    def func_with_optional_param(a: Optional[int] = None, b: str = "default") -> str:
        """
        A function with an optional integer parameter and a string parameter.

        Args:
            a: Optional integer parameter, default None.
            b: Optional string parameter, default 'default'.

        Returns:
            str: Concatenated string representation of parameters.
        """
        return f"{a}-{b}"

    sql_body = generate_sql_function_body(func_with_optional_param, "test_catalog", "test_schema")

    expected_sql = """
CREATE FUNCTION `test_catalog`.`test_schema`.`func_with_optional_param`(a LONG DEFAULT NULL COMMENT 'Optional integer parameter, default None.', b STRING DEFAULT 'default' COMMENT 'Optional string parameter, default "default".')
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'A function with an optional integer parameter and a string parameter.'
AS $$
    return f"{a}-{b}"
$$;
    """
    assert sql_body.strip() == expected_sql.strip()


# ---------------------------
# Tests for Forbidden Default Types
# ---------------------------


def test_parameter_with_default_list():
    def func_with_default_list(a: List[int] = [1, 2, 3]) -> int:  # noqa: B006
        """
        Function with a default list parameter.

        Args:
            a: The list parameter
        """
        return sum(a)

    with pytest.raises(ValueError, match="Parameter 'a' of type '.*' cannot have a default value."):
        generate_sql_function_body(func_with_default_list, "test_catalog", "test_schema")


def test_parameter_with_default_dict():
    def func_with_default_dict(a: Dict[str, int] = {"one": 1}) -> int:  # noqa: B006
        """
        Function with a default dict parameter.

        Args:
            a: The dictionary parameter
        """
        return a["one"]

    with pytest.raises(ValueError, match="Parameter 'a' of type '.*' cannot have a default value."):
        generate_sql_function_body(func_with_default_dict, "test_catalog", "test_schema")


def test_parameter_with_default_tuple():
    def func_with_default_tuple(a: Tuple[int] = (1, 2)) -> int:
        """
        Function with a default tuple parameter.

        Args:
            a: The tuple parameter
        """
        return sum(a)

    with pytest.raises(ValueError, match="Parameter 'a' of type '.*' cannot have a default value."):
        generate_sql_function_body(func_with_default_tuple, "test_catalog", "test_schema")


# ---------------------------
# Tests for Mismatched Default Types
# ---------------------------


def test_parameter_with_disallowed_scalar_default():
    def func_with_wrong_default(a: int = "10") -> int:
        """
        Function with a wrong type default parameter.

        Args:
            a: The integer parameter
        """
        return a * 2

    with pytest.raises(
        ValueError, match="Default value for parameter 'a' does not match the type hint"
    ):
        generate_sql_function_body(func_with_wrong_default, "test_catalog", "test_schema")


# ---------------------------
# Tests for Forbidden Parameters
# ---------------------------


def test_function_with_self():
    """Test that functions using 'self' raise an exception."""

    def func_with_self(self, a: int) -> int:
        """
        Example function with 'self'.

        Args:
            a: The integer parameter
        """
        return a * 2

    with pytest.raises(
        ValueError, match="Parameter 'self' is not allowed in the function signature"
    ):
        generate_sql_function_body(func_with_self, "test_catalog", "test_schema")


def test_function_with_cls():
    """Test that functions using 'cls' raise an exception."""

    def func_with_cls(cls, a: int) -> int:
        """
        Example function with 'cls'.

        Args:
            a: The integer parameter
        """
        return a + 5

    with pytest.raises(
        ValueError, match="Parameter 'cls' is not allowed in the function signature"
    ):
        generate_sql_function_body(func_with_cls, "test_catalog", "test_schema")


# ---------------------------
# Tests for Forbidden *args and **kwargs
# ---------------------------


def test_function_with_args():
    def func_with_args(*args) -> int:
        """
        Function that incorrectly uses *args.

        Args:
            args: Additional positional arguments
        """
        return sum(args)

    with pytest.raises(
        ValueError,
        match=re.escape(
            "Parameter 'args' is a var-positional (*args) parameter, which is not supported in SQL functions."
        ),
    ):
        generate_sql_function_body(func_with_args, "catalog", "schema")


def test_function_with_kwargs():
    def func_with_kwargs(**kwargs) -> int:
        """
        Function that incorrectly uses **kwargs.

        Args:
            kwargs: Additional keyword arguments
        """
        return sum(kwargs.values())

    with pytest.raises(
        ValueError,
        match=re.escape(
            "Parameter 'kwargs' is a var-keyword (**kwargs) parameter, which is not supported in SQL functions."
        ),
    ):
        generate_sql_function_body(func_with_kwargs, "catalog", "schema")


def test_function_with_mixed_args():
    def func_with_mixed(a: int, *args, **kwargs) -> int:
        """
        Function that incorrectly uses both *args and **kwargs.

        Args:
            a: The first parameter
            args: Additional positional arguments
            kwargs: Additional keyword arguments
        """
        return a + sum(args) + sum(kwargs.values())

    with pytest.raises(
        ValueError,
        match=re.escape(
            "Parameter 'args' is a var-positional (*args) parameter, which is not supported in SQL functions."
        ),
    ):
        generate_sql_function_body(func_with_mixed, "catalog", "schema")


# ---------------------------
# Tests for Indentation Handling
# ---------------------------


def test_function_with_2_space_indentation():
    # fmt: off
    def two_space_indented_func(a: int) -> str:
        """
        A function with 2-space indentation.

        Args:
            a: The parameter

        Returns:
            str: The string representation of the parameter
        """
        
        def nested_func(b: int) -> int:
          return b + a
        
        return str(nested_func(a))
    # fmt: on
    # Generating SQL from the function
    sql_body = generate_sql_function_body(
        two_space_indented_func, "test_catalog", "test_schema", True
    )

    # Expected SQL with 2-space indentation for function body and nested function
    expected_sql = """
CREATE OR REPLACE FUNCTION `test_catalog`.`test_schema`.`two_space_indented_func`(a LONG COMMENT 'The parameter')
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'A function with 2-space indentation.'
AS $$
    def nested_func(b: int) -> int:
      return b + a

    return str(nested_func(a))
$$;
    """
    assert sql_body.strip() == expected_sql.strip()


# ---------------------------
# Tests for Functions with Imports
# ---------------------------


def test_function_with_imports():
    def func_with_import(a: int) -> str:
        """
        A function that imports a module and returns a result.

        Args:
            a: The input parameter

        Returns:
            str: A string representation of a result
        """
        import math

        return str(math.sqrt(a))

    sql_body = generate_sql_function_body(func_with_import, "test_catalog", "test_schema")

    expected_sql = """
CREATE FUNCTION `test_catalog`.`test_schema`.`func_with_import`(a LONG COMMENT 'The input parameter')
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'A function that imports a module and returns a result.'
AS $$
    import math

    return str(math.sqrt(a))
$$;
    """
    assert sql_body.strip() == expected_sql.strip()


# ---------------------------
# Tests for Warning Logic
# ---------------------------


import pytest


def test_warning_extra_params_in_docstring():
    def func_with_extra_doc_params(a: int) -> str:
        """
        Function with extra parameters in docstring.

        Args:
            a: The integer parameter.
            b: Extra parameter not in function signature.

        Returns:
            str: The string representation of 'a'.
        """
        return str(a)

    with pytest.warns(
        UserWarning,
        match="The following parameters are documented in the docstring but not present in the function signature: b",
    ) as record:
        generate_sql_function_body(func_with_extra_doc_params, "test_catalog", "test_schema")

    assert len(record) == 1


def test_warning_missing_params_in_docstring():
    def func_with_missing_doc_params(a: int, b: str) -> str:
        """
        Function with missing parameters in docstring.

        Args:
            a: The integer parameter.

        Returns:
            str: The string representation of 'a' and 'b'.
        """
        return f"{a}-{b}"

    with pytest.warns(
        UserWarning,
        match="The following parameters are present in the function signature but not documented in the docstring: b",
    ) as record:
        generate_sql_function_body(func_with_missing_doc_params, "test_catalog", "test_schema")

    assert len(record) == 1


def test_warning_doc_params_but_no_signature_params():
    def func_with_doc_params_but_no_signature() -> str:
        """
        Function with docstring parameters but no signature parameters.

        Args:
            a: The parameter which does not exist in the signature.

        Returns:
            str: A default string.
        """
        return "default"

    expected_warning_1 = (
        "In function 'func_with_doc_params_but_no_signature': "
        "The following parameters are documented in the docstring but not present in the function signature: a"
    )
    expected_warning_2 = (
        "In function 'func_with_doc_params_but_no_signature': "
        "Docstring defines parameters, but the function has no parameters in its signature."
    )

    combined_match = f"({expected_warning_1})|({expected_warning_2})"

    with pytest.warns(UserWarning, match=combined_match) as record:
        generate_sql_function_body(
            func_with_doc_params_but_no_signature, "test_catalog", "test_schema"
        )

    assert len(record) == 2


def test_warning_signature_params_but_no_doc_params():
    def func_with_signature_params_but_no_doc(a: int, b: int) -> int:
        """
        Function with signature parameters but no docstring parameters.

        Returns:
            int: The sum of 'a' and 'b'.
        """
        return a + b

    expected_warning_1 = (
        "In function 'func_with_signature_params_but_no_doc': "
        "The following parameters are present in the function signature but not documented in the docstring: a, b"
    )
    expected_warning_2 = (
        "In function 'func_with_signature_params_but_no_doc': "
        "Function has parameters in its signature, but the docstring does not document any parameters."
    )

    combined_match = f"({expected_warning_1})|({expected_warning_2})"

    with pytest.warns(UserWarning, match=combined_match) as record:
        generate_sql_function_body(
            func_with_signature_params_but_no_doc, "test_catalog", "test_schema"
        )

    assert len(record) == 2


def test_no_warnings_when_consistent():
    def consistent_func(a: int, b: str) -> str:
        """
        Consistent function.

        Args:
            a: The integer parameter.
            b: The string parameter.

        Returns:
            str: The concatenation of 'a' and 'b'.
        """
        return f"{a}-{b}"

    with warnings.catch_warnings(record=True) as record:
        warnings.simplefilter("always")
        generate_sql_function_body(consistent_func, "test_catalog", "test_schema")

    assert len(record) == 0


class FakeParam:
    def __init__(self, name: str, type_text: str):
        self.name = name
        self.type_text = type_text


# ---------------------------
# Parametrized Tests for Return Type Parsing
# ---------------------------


@pytest.mark.parametrize(
    "input_type, expected",
    [
        ("bigint", SQL_TYPE_TO_PYTHON_TYPE_MAPPING.get("BIGINT", int).__name__),
        ("ARRAY<INT>", "list[int]"),
        ("MAP<STRING, ARRAY<STRING>>", "dict[str, list[str]]"),
        ("MAP<STRING, MAP<STRING, DOUBLE>>", "dict[str, dict[str, float]]"),
        ("STRUCT<field1:INT, field2:STRING>", "dict"),
    ],
)
def test_parse_full_sql_data_type_for_return_types(input_type: str, expected: str):
    result = _parse_sql_data_type(input_type)
    assert result == expected


# ---------------------------
# Parametrized Tests for Parameter Type Extraction
# ---------------------------


@pytest.mark.parametrize(
    "param_name, type_text, expected",
    [
        (
            "e",
            "ARRAY<STRING>",
            "e: list[str]",
        ),
        (
            "f",
            "MAP<STRING, LONG>",
            "f: dict[str, int]",
        ),
        (
            "h",
            "MAP<STRING, ARRAY<LONG>>",
            "h: dict[str, list[int]]",
        ),
        (
            "i",
            "MAP<STRING, ARRAY<MAP<STRING, ARRAY<LONG>>>>",
            "i: dict[str, list[dict[str, list[int]]]]",
        ),
    ],
)
def test_extract_collection_types(param_name: str, type_text: str, expected: str):
    fake_param = FakeParam(param_name, type_text)
    result = f"{fake_param.name}: {_parse_sql_data_type(fake_param.type_text)}"
    assert result == expected


class DummyParameter:
    def __init__(self, name: str, type_json: str, comment: str):
        self.name = name
        self.type_json = type_json
        self.comment = comment


class DummyInputParams:
    def __init__(self, parameters):
        self.parameters = parameters


class DummyRoutineBody:
    def __init__(self, value: str):
        self.value = value


class DummyFunctionInfo:
    pass


def build_expected_definition(func_name: str, routine_definition: str, comment: str) -> str:
    """
    Given the function name, the raw routine definition, and an overall comment,
    build the complete expected function definition string.

    The expected definition has:
      - A header: "def <func_name>() -> str:" (since full_data_type "STRING" maps to "str")
      - A reconstructed docstring that uses the provided comment and a "Returns:" section
        that shows "str" (the parsed return type).
      - A function body that is normalized by our _parse_routine_definition helper.

    Returns:
        The expected full function definition as a string.
    """
    header = f"def {func_name}() -> str:"
    # Build the docstring.
    wrapped_comment = textwrap.fill(comment.strip(), width=100)
    # Here we assume that if no explicit returns text is stored, we use the parsed type "str".
    wrapped_return = textwrap.fill(
        "str", width=100, initial_indent="    ", subsequent_indent="    "
    )
    doc_lines = [
        wrapped_comment,
        "",
        "Returns:",
        wrapped_return,
    ]
    indented_doc = "\n".join("    " + line for line in doc_lines)
    docstring = f'    """\n{indented_doc}\n    """\n'
    # Use our helper _parse_routine_definition to normalize the routine definition.
    expected_body = _parse_routine_definition(routine_definition)
    expected_def = f"{header}\n{docstring}{expected_body}\n"
    return expected_def


test_cases = [
    # 1. Already properly indented with 4 spaces.
    ("    return a + b", "    return a + b"),
    # 2. Indented with 2 spaces.
    ("  return a + b", "    return a + b"),
    # 3. Nested definition: outer indent 2, nested line indent 6, outer again indent 2.
    (
        "  def _helper(x):\n      return x * 2\n  return _helper(a)",
        "    def _helper(x):\n        return x * 2\n    return _helper(a)",
    ),
    # 4. Inconsistent indentation: first line indent 2, second line flush left, third line indent 4.
    (
        "  line one\nline two\n    line three",
        # Because one nonempty line is flush left, baseline becomes 0, so every nonempty line gets fixed 4-space indent.
        "        line one\n    line two\n            line three",
    ),
    # 5. Contains a blank line; nonempty lines have indents 2 and 4, so baseline = 2.
    ("  line one\n\n    line three\n", "    line one\n\n        line three"),
    # 6. Nested definitions: outer indent 2, inner indent 4, inner's body indent 6, then outer again indent 2.
    (
        "  def outer():\n    def inner():\n      return 1\n    return inner()",
        "    def outer():\n        def inner():\n            return 1\n        return inner()",
    ),
    # 7. Already flush left.
    ("def foo():\n    return 'bar'", "    def foo():\n        return 'bar'"),
    # 8. Trailing whitespace.
    ("    return a + b    \n   ", "    return a + b"),
    # 9. Uses a tab.
    ("\treturn a + b", "    return a + b"),
    # 10. All flush left.
    ("return a + b", "    return a + b"),
]


@pytest.mark.parametrize("input_body, expected_normalized", test_cases)
def test_parse_routine_definition_indentation(input_body, expected_normalized):
    # Expand tabs to spaces
    input_body = "\n".join(line.expandtabs(4) for line in input_body.splitlines())
    result = _parse_routine_definition(input_body)
    assert result == expected_normalized, f"Expected:\n{expected_normalized}\nGot:\n{result}"


@pytest.mark.parametrize("input_body, expected_normalized", test_cases)
def test_dynamically_construct_python_function_indentation(input_body, expected_normalized):
    # Create a dummy FunctionInfo object.
    dummy = DummyFunctionInfo()
    dummy.name = "test_func"
    dummy.input_params = DummyInputParams([])  # No parameters.
    dummy.full_data_type = "STRING"  # Maps to "str"
    dummy.comment = "Test function"
    dummy.routine_body = DummyRoutineBody("EXTERNAL")
    dummy.routine_definition = input_body

    reconstructed = dynamically_construct_python_function(dummy)
    expected_def = build_expected_definition("test_func", input_body, "Test function")
    assert reconstructed == expected_def, f"Expected:\n{expected_def}\nGot:\n{reconstructed}"
