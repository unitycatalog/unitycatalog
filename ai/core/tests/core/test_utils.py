import datetime
import decimal
import math
import sys
from io import StringIO
from typing import Dict, List, Optional, Tuple, Union

import pytest
from databricks.sdk.service.catalog import (
    FunctionInfo,
    FunctionParameterInfo,
    FunctionParameterInfos,
)
from pydantic import BaseModel

from unitycatalog.ai.core.utils.function_processing_utils import (
    FullFunctionName,
    generate_function_input_params_schema,
    sanitize_string_inputs_of_function_params,
    uc_type_json_to_pydantic_type,
)
from unitycatalog.ai.core.utils.type_utils import (
    column_type_to_python_type,
    convert_timedelta_to_interval_str,
)
from unitycatalog.ai.core.utils.validation_utils import is_base64_encoded


def test_full_function_name():
    result = FullFunctionName.validate_full_function_name("catalog.schema.function")
    assert result.catalog == "catalog"
    assert result.schema == "schema"
    assert result.function == "function"

    with pytest.raises(ValueError, match=r"Invalid function name"):
        FullFunctionName.validate_full_function_name("catalog.schema.function.extra")


def test_column_type_to_python_type_errors():
    with pytest.raises(ValueError, match=r"Unsupported column type"):
        column_type_to_python_type("INVALID_TYPE")


def test_is_base64_encoded():
    assert is_base64_encoded("aGVsbG8=")
    assert not is_base64_encoded("hello")


@pytest.mark.parametrize(
    ("time_val", "expected"),
    [
        (
            datetime.timedelta(days=1, seconds=1, microseconds=123456),
            "INTERVAL '1 0:0:1.123456' DAY TO SECOND",
        ),
        (datetime.timedelta(hours=100), "INTERVAL '4 4:0:0.0' DAY TO SECOND"),
        (datetime.timedelta(days=1, hours=10, minutes=3), "INTERVAL '1 10:3:0.0' DAY TO SECOND"),
    ],
)
def test_convert_timedelta_to_interval_str(time_val, expected):
    assert convert_timedelta_to_interval_str(time_val) == expected


@pytest.mark.parametrize(
    ("uc_type_json", "expected_type_or_input"),
    [
        (
            {"type": "array", "elementType": "byte", "containsNull": True},
            Union[List[Optional[int]], Tuple[Optional[int], ...]],
        ),
        (
            {
                "type": "struct",
                "fields": [
                    {
                        "name": "a",
                        "type": "short",
                        "nullable": False,
                        "metadata": {"comment": "short field"},
                    },
                    {
                        "name": "b",
                        "type": {
                            "type": "map",
                            "keyType": "string",
                            "valueType": "float",
                            "valueContainsNull": True,
                        },
                        "nullable": True,
                        "metadata": {},
                    },
                    {"name": "c", "type": "integer", "nullable": False, "metadata": {}},
                ],
            },
            {"a": 123, "b": {"key": 123.0}, "c": 123},
        ),
        ("binary", bytes),
        ("interval day to second", Union[datetime.timedelta, str]),
        ("timestamp", Union[str, datetime.datetime]),
        ("timestamp_ntz", Union[str, datetime.datetime]),
        ("date", Union[str, datetime.date]),
        (
            {
                "type": "map",
                "keyType": "string",
                "valueType": {"type": "array", "elementType": "integer", "containsNull": True},
                "valueContainsNull": True,
            },
            Dict[str, Optional[Union[List[Optional[int]], Tuple[Optional[int], ...]]]],
        ),
        ("decimal(10,2)", Union[float, decimal.Decimal]),
        ("double", float),
        ("float", float),
        ("long", int),
        ("boolean", bool),
        ("integer", int),
        ("short", int),
        ("string", str),
        (
            {
                "type": "array",
                "elementType": {"type": "array", "elementType": "string", "containsNull": False},
                "containsNull": False,
            },
            Union[
                List[Union[List[str], Tuple[str, ...]]],
                Tuple[Union[List[str], Tuple[str, ...]], ...],
            ],
        ),
    ],
)
def test_uc_type_json_to_pydantic_type(uc_type_json, expected_type_or_input):
    param_type = uc_type_json_to_pydantic_type(uc_type_json).pydantic_type
    if isinstance(param_type, type(BaseModel)):
        param_type(**expected_type_or_input)
    else:
        assert param_type == expected_type_or_input


def generate_function_info(parameters: List[Dict], catalog="catalog", schema="schema"):
    return FunctionInfo(
        catalog_name=catalog,
        schema_name=schema,
        name="test",
        input_params=FunctionParameterInfos(
            parameters=[FunctionParameterInfo(**param) for param in parameters]
        ),
    )


@pytest.mark.parametrize(
    ("function_info", "valid_inputs"),
    [
        # create a function info with only one input parameter
        # type_name represents the SQL type of the parameter
        # type_json represents the detailed information about the parameter type
        # test valid inputs are accepted by the function info
        (
            generate_function_info(
                [
                    {
                        "name": "a",
                        "type_text": "struct<a:smallint,b:map<string,float>,c:int>",
                        "type_json": '{"name":"a","type":{"type":"struct","fields":[{"name":"a","type":"short","nullable":false,"metadata":{"comment":"short field"}},{"name":"b","type":{"type":"map","keyType":"string","valueType":"float","valueContainsNull":true},"nullable":true,"metadata":{}},{"name":"c","type":"integer","nullable":false,"metadata":{}}]},"nullable":true,"metadata":{}}',
                        "type_name": "STRUCT",
                        "type_precision": 0,
                        "type_scale": 0,
                        "position": 0,
                        "parameter_type": "PARAM",
                    }
                ]
            ),
            [
                {"a": {"a": 123, "b": {"key": 123.0}, "c": 123}},
                {"a": {"a": 123, "b": {"key": None}, "c": 123}},
                {"a": {"a": 123, "b": None, "c": 234}},
                {"a": {"a": 0, "c": 234}},
            ],
        ),
        (
            generate_function_info(
                [
                    {
                        "name": "b",
                        "type_text": "array<tinyint>",
                        "type_json": '{"name":"b","type":{"type":"array","elementType":"byte","containsNull":true},"nullable":true,"metadata":{}}',
                        "type_name": "ARRAY",
                        "type_precision": 0,
                        "type_scale": 0,
                        "position": 1,
                        "parameter_type": "PARAM",
                    }
                ]
            ),
            [{"b": [100] * 10}, {}, {"b": None}],
        ),
        (
            generate_function_info(
                [
                    {
                        "name": "c",
                        "type_text": "binary",
                        "type_json": '{"name":"c","type":"binary","nullable":true,"metadata":{"comment":"BINARY field"}}',
                        "type_name": "BINARY",
                        "type_precision": 0,
                        "type_scale": 0,
                        "position": 2,
                        "parameter_type": "PARAM",
                        "comment": "BINARY field",
                    }
                ]
            ),
            [{"c": b"binary"}, {"c": "aGVsbG8="}, {}],
        ),
        (
            generate_function_info(
                [
                    {
                        "name": "d",
                        "type_text": "interval day to second",
                        "type_json": '{"name":"d","type":"interval day to second","nullable":true,"metadata":{}}',
                        "type_name": "INTERVAL",
                        "type_precision": 0,
                        "type_scale": 0,
                        "position": 3,
                        "parameter_type": "PARAM",
                    }
                ]
            ),
            [{"d": datetime.timedelta(days=1, seconds=1, microseconds=123456)}, {"d": None}],
        ),
        (
            generate_function_info(
                [
                    {
                        "name": "e",
                        "type_text": "timestamp",
                        "type_json": '{"name":"e","type":"timestamp","nullable":true,"metadata":{}}',
                        "type_name": "TIMESTAMP",
                        "type_precision": 0,
                        "type_scale": 0,
                        "position": 4,
                        "parameter_type": "PARAM",
                    }
                ]
            ),
            [{"e": datetime.datetime(2024, 9, 11)}, {"e": "2024-09-11T00:00:00"}, {"e": None}],
        ),
        (
            generate_function_info(
                [
                    {
                        "name": "f",
                        "type_text": "timestamp_ntz",
                        "type_json": '{"name":"f","type":"timestamp_ntz","nullable":true,"metadata":{}}',
                        "type_name": "TIMESTAMP_NTZ",
                        "type_precision": 0,
                        "type_scale": 0,
                        "position": 5,
                        "parameter_type": "PARAM",
                    }
                ]
            ),
            [{"f": datetime.datetime(2024, 9, 11)}, {"f": "2024-09-11T00:00:00"}, {"f": None}],
        ),
        (
            generate_function_info(
                [
                    {
                        "name": "g",
                        "type_text": "date",
                        "type_json": '{"name":"g","type":"date","nullable":true,"metadata":{}}',
                        "type_name": "DATE",
                        "type_precision": 0,
                        "type_scale": 0,
                        "position": 6,
                        "parameter_type": "PARAM",
                    }
                ]
            ),
            [{"g": datetime.date(2024, 9, 11)}, {"g": "2024-09-11"}, {"g": None}],
        ),
        (
            generate_function_info(
                [
                    {
                        "name": "h",
                        "type_text": "map<string,array<int>>",
                        "type_json": '{"name":"h","type":{"type":"map","keyType":"string","valueType":{"type":"array","elementType":"integer","containsNull":true},"valueContainsNull":true},"nullable":true,"metadata":{}}',
                        "type_name": "MAP",
                        "type_precision": 0,
                        "type_scale": 0,
                        "position": 7,
                        "parameter_type": "PARAM",
                    }
                ]
            ),
            [{"h": {"key": [1, 2, 3], "key2": None}}, {"h": {"key": None}}, {"h": None}],
        ),
        (
            generate_function_info(
                [
                    {
                        "name": "i",
                        "type_text": "decimal(10,2)",
                        "type_json": '{"name":"i","type":"decimal(10,2)","nullable":true,"metadata":{}}',
                        "type_name": "DECIMAL",
                        "type_precision": 0,
                        "type_scale": 0,
                        "position": 8,
                        "parameter_type": "PARAM",
                    },
                    {
                        "name": "j",
                        "type_text": "double",
                        "type_json": '{"name":"j","type":"double","nullable":true,"metadata":{}}',
                        "type_name": "DOUBLE",
                        "type_precision": 0,
                        "type_scale": 0,
                        "position": 9,
                        "parameter_type": "PARAM",
                    },
                    {
                        "name": "k",
                        "type_text": "float",
                        "type_json": '{"name":"k","type":"float","nullable":true,"metadata":{}}',
                        "type_name": "FLOAT",
                        "type_precision": 0,
                        "type_scale": 0,
                        "position": 10,
                        "parameter_type": "PARAM",
                    },
                    {
                        "name": "l",
                        "type_text": "bigint",
                        "type_json": '{"name":"l","type":"long","nullable":true,"metadata":{}}',
                        "type_name": "LONG",
                        "type_precision": 0,
                        "type_scale": 0,
                        "position": 11,
                        "parameter_type": "PARAM",
                    },
                    {
                        "name": "m",
                        "type_text": "boolean",
                        "type_json": '{"name":"m","type":"boolean","nullable":true,"metadata":{}}',
                        "type_name": "BOOLEAN",
                        "type_precision": 0,
                        "type_scale": 0,
                        "position": 12,
                        "parameter_type": "PARAM",
                    },
                    {
                        "name": "o",
                        "type_text": "int",
                        "type_json": '{"name":"o","type":"integer","nullable":true,"metadata":{}}',
                        "type_name": "INT",
                        "type_precision": 0,
                        "type_scale": 0,
                        "position": 14,
                        "parameter_type": "PARAM",
                    },
                    {
                        "name": "p",
                        "type_text": "smallint",
                        "type_json": '{"name":"p","type":"short","nullable":true,"metadata":{}}',
                        "type_name": "SHORT",
                        "type_precision": 0,
                        "type_scale": 0,
                        "position": 15,
                        "parameter_type": "PARAM",
                    },
                    {
                        "name": "q",
                        "type_text": "array<array<string>>",
                        "type_json": '{"name":"q","type":{"type":"array","elementType":{"type":"array","elementType":"string","containsNull":true},"containsNull":true},"nullable":true,"metadata":{}}',
                        "type_name": "ARRAY",
                        "type_precision": 0,
                        "type_scale": 0,
                        "position": 16,
                        "parameter_type": "PARAM",
                    },
                    {
                        "name": "r",
                        "type_text": "string",
                        "type_json": '{"name":"r","type":"string","nullable":true,"metadata":{"EXISTS_DEFAULT":"\\"123\\"","default":"\\"123\\"","CURRENT_DEFAULT":"\\"123\\""}}',
                        "type_name": "STRING",
                        "type_precision": 0,
                        "type_scale": 0,
                        "position": 17,
                        "parameter_type": "PARAM",
                        "parameter_default": '"123"',
                    },
                ]
            ),
            [
                {
                    "i": 1.234,
                    "j": 1.234,
                    "k": 1.234,
                    "l": 123,
                    "m": True,
                    "o": 123,
                    "p": 123,
                    "q": [["a", "b"], ["c"], ["d", None], None],
                    "r": "123",
                },
                {},
            ],
        ),
    ],
)
def test_generate_function_input_params_schema(function_info, valid_inputs):
    pydantic_model = generate_function_input_params_schema(function_info).pydantic_model
    for data in valid_inputs:
        pydantic_model(**data)


def test_generate_function_input_params_schema_with_non_ascii_chars():
    function_info = generate_function_info(
        [
            {
                "name": "x",
                "type_text": "smallint",
                "type_json": '{"name":"p","type":"short","nullable":true,"metadata":{}}',
                "type_name": "SHORT",
                "type_precision": 0,
                "type_scale": 0,
                "position": 15,
                "parameter_type": "PARAM",
            }
        ],
        catalog="カタログ",
        schema="スキーマ",
    )
    pydantic_model = generate_function_input_params_schema(function_info).pydantic_model
    pydantic_model(**{"x": 123})


def unescape_sql_string(value: str) -> str:
    """
    Unescapes a string as it would be when parsed by the SQL engine.
    """
    # Replace doubled backslashes with single backslash
    unescaped = value.replace("\\\\", "\\")
    # Replace double single quotes with a single quote
    unescaped = unescaped.replace("''", "'")
    # Replace escaped newlines, and tabs
    unescaped = unescaped.replace("\\n", "\n").replace("\\t", "\t")
    return unescaped


# Mocking the UDF execution environment
def mock_execute_function(code: str) -> str:
    """
    Mocks the execution of the user-defined function that executes the provided code string.
    """
    sys_stdout = sys.stdout
    redirected_output = StringIO()
    sys.stdout = redirected_output
    exec(code)
    sys.stdout = sys_stdout
    return redirected_output.getvalue()


# Test cases
test_cases = [
    # 1. Simple print statement
    ("print('Hello, world!')", "Hello, world!\n"),
    # 2. Code with single quotes
    ("print('It\\'s a sunny day')", "It's a sunny day\n"),
    # 3. Code with double quotes
    ('print("He said, \\"Hi!\\"")', 'He said, "Hi!"\n'),
    # 4. Code with backslashes
    (r"print('C:\\path\\into\\dir')", "C:\\path\\into\\dir\n"),
    # 5. Multi-line code with newlines
    ("for i in range(3):\n    print(i)", "0\n1\n2\n"),
    # 6. Code with tabs and indents
    ("def greet(name):\n    print(f'Hello, {name}!')\ngreet('Alice')", "Hello, Alice!\n"),
    # 7. Code with special characters
    ("print('Special chars: !@#$%^&*()')", "Special chars: !@#$%^&*()\n"),
    # 8. Unicode characters
    ("print('Unicode test: ü, é, 漢字')", "Unicode test: ü, é, 漢字\n"),
    # 9. Code with comments
    ("# This is a comment\nprint('Comment test')", "Comment test\n"),
    # 10. Code raising an exception
    (
        "try:\n    raise ValueError('Test error')\nexcept Exception as e:\n    print(f'Caught an error: {e}')",
        "Caught an error: Test error\n",
    ),
    # 11. Code with triple quotes
    ('print("""Triple quote test""")', "Triple quote test\n"),
    # 12. Code with raw strings
    ("print('Raw string: \\\\n new line')", "Raw string:  new line\n"),
    # 13. Empty code string
    ("", ""),
    # 14. Code with carriage return
    ("print('Line1\\\\rLine2')", "Line1\\rLine2\n"),
    # 15. Code with multiple special characters
    (r"print('Mix: \\\\ \\\' \\\" \\\\n \\\\t')", """Mix: \\\\ \\\' \\" \\ \\\\\t\n"""),
    # 16. Code with encoding declarations (Note: encoding declarations should be in the first or second line)
    ("# -*- coding: utf-8 -*-\nprint('Encoding test')", "Encoding test\n"),
    # 17. Code importing a standard library
    ("import math\nprint(math.pi)", f"{math.pi}\n"),
    # 18. Code with nested functions
    (
        "def outer():\n    def inner():\n        return 'Nested'\n    return inner()\nprint(outer())",
        "Nested\n",
    ),
    # 19. Code with list comprehensions
    ("squares = [x**2 for x in range(5)]\nprint(squares)", "[0, 1, 4, 9, 16]\n"),
    # 20. Code with multi-line strings
    ("multi_line = '''Line1\nLine2\nLine3'''\nprint(multi_line)", "Line1\nLine2\nLine3\n"),
]


@pytest.mark.parametrize("code_input, expected_output", test_cases)
def test_code_execution(code_input: str, expected_output: str):
    # Escape the code string as it would be for SQL inclusion
    escaped_code = sanitize_string_inputs_of_function_params({"code": code_input})
    # Simulate SQL parsing which unescapes the string
    code_to_execute = unescape_sql_string(escaped_code["code"])
    # Execute the code
    output = mock_execute_function(code_to_execute)
    # Assert the output matches the expected output
    assert output == expected_output
