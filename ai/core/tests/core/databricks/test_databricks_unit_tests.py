import datetime
import os
import re
from decimal import Decimal
from typing import Dict, List, Union
from unittest.mock import MagicMock, patch

import pytest
from databricks.sdk.service.catalog import (
    ColumnTypeName,
    CreateFunctionParameterStyle,
    CreateFunctionRoutineBody,
    CreateFunctionSecurityType,
    CreateFunctionSqlDataAccess,
    DependencyList,
    FunctionInfo,
    FunctionParameterInfo,
    FunctionParameterInfos,
)

from unitycatalog.ai.core.databricks import (
    DatabricksFunctionClient,
    extract_function_name,
    get_execute_function_sql_command,
    retry_on_session_expiration,
)
from unitycatalog.ai.core.envs.databricks_env_vars import UCAI_DATABRICKS_SESSION_RETRY_MAX_ATTEMPTS
from unitycatalog.ai.test_utils.client_utils import client  # noqa: F401
from unitycatalog.ai.test_utils.function_utils import (
    CATALOG,
    random_func_name,
)

SCHEMA = os.environ.get("SCHEMA", "ucai_core_test")


def test_get_function_errors(client: DatabricksFunctionClient):
    with pytest.raises(ValueError, match=r"Invalid function name"):
        client.get_function("test")

    with pytest.raises(ValueError, match=r"function name cannot include *"):
        client.get_function("catalog.schema.*")

    with pytest.raises(ValueError, match=r"function name cannot include *"):
        client.get_function("catalog.schema.some_func*")


@pytest.mark.parametrize(
    ("sql_body", "function_name"),
    [
        (
            "CREATE OR REPLACE FUNCTION a.b.test(s STRING) RETURNS STRING LANGUAGE PYTHON AS $$ return s $$",
            "a.b.test",
        ),
        (
            "CREATE TEMPORARY FUNCTION a.b.test(s STRING) RETURNS STRING LANGUAGE PYTHON AS $$ return s $$",
            "a.b.test",
        ),
        ("CREATE FUNCTION IF NOT EXISTS a.b.test() RETURN 123", "a.b.test"),
        (
            "CREATE FUNCTION `some-catalog`.`some-schema`.`test_function`() RETURN 123",
            "some-catalog.some-schema.test_function",
        ),
        (
            "CREATE FUNCTION `奇怪的catalog`.`some-schema`.test_function() RETURN 123",
            "奇怪的catalog.some-schema.test_function",
        ),
    ],
)
def test_extract_function_name(sql_body, function_name):
    assert extract_function_name(sql_body) == function_name


@pytest.mark.parametrize(
    ("sql_body"),
    [
        "CREATE OR REPLACE FUNCTION test(s STRING) RETURNS STRING LANGUAGE PYTHON AS $$ return s $$",
        "CREATE FUNCTION a.test(s STRING) RETURNS STRING LANGUAGE PYTHON AS $$ return s $$",
        "CREATE TEMPORARY FUNCTION IF NOT EXISTS test(s STRING) RETURNS STRING LANGUAGE PYTHON AS $$ return s $$",
    ],
)
def test_invalid_function_name(sql_body):
    with pytest.raises(ValueError, match=r"Could not extract function name from the sql body"):
        extract_function_name(sql_body)


@pytest.mark.parametrize(
    "sql_body",
    [
        "CREATE OR REPLACE FUNCTION (s STRING) RETURNS STRING LANGUAGE PYTHON AS $$ return s $$",
        "CREATE FUNCTION RETURNS STRING LANGUAGE PYTHON AS $$ return s $$",
        "CREATE FUNCTION a.b. RETURNS STRING LANGUAGE PYTHON AS $$ return s $$",
        "UPDATE FUNCTION a.b.test(s STRING) RETURNS STRING LANGUAGE PYTHON AS $$ return s $$",
    ],
)
def test_extract_function_name_error(sql_body):
    with pytest.raises(ValueError, match="Could not extract function name from the sql body."):
        extract_function_name(sql_body)


@pytest.mark.parametrize(
    ("param_value", "param_info"),
    [
        (
            [1, 2, 3],
            FunctionParameterInfo(
                "param", type_name=ColumnTypeName.ARRAY, type_text="array<int>", position=0
            ),
        ),
        (
            ("a", "b"),
            FunctionParameterInfo(
                "param", type_name=ColumnTypeName.ARRAY, type_text="array<string>", position=1
            ),
        ),
        (
            "SEVMTE8=",
            FunctionParameterInfo(
                "param", type_name=ColumnTypeName.BINARY, type_text="binary", position=2
            ),
        ),
        (
            True,
            FunctionParameterInfo(
                "param", type_name=ColumnTypeName.BOOLEAN, type_text="boolean", position=3
            ),
        ),
        (
            123456,
            FunctionParameterInfo(
                "param", type_name=ColumnTypeName.BYTE, type_text="byte", position=4
            ),
        ),
        (
            "some_char",
            FunctionParameterInfo(
                "param", type_name=ColumnTypeName.CHAR, type_text="char", position=5
            ),
        ),
        (
            datetime.date(2024, 8, 19),
            FunctionParameterInfo(
                "param", type_name=ColumnTypeName.DATE, type_text="date", position=6
            ),
        ),
        (
            "2024-08-19",
            FunctionParameterInfo(
                "param", type_name=ColumnTypeName.DATE, type_text="date", position=7
            ),
        ),
        (
            123.45,
            FunctionParameterInfo(
                "param", type_name=ColumnTypeName.DECIMAL, type_text="decimal", position=8
            ),
        ),
        (
            Decimal("123.45"),
            FunctionParameterInfo(
                "param", type_name=ColumnTypeName.DECIMAL, type_text="decimal", position=9
            ),
        ),
        (
            123.45,
            FunctionParameterInfo(
                "param", type_name=ColumnTypeName.DOUBLE, type_text="double", position=10
            ),
        ),
        (
            123.45,
            FunctionParameterInfo(
                "param", type_name=ColumnTypeName.FLOAT, type_text="float", position=11
            ),
        ),
        (
            123,
            FunctionParameterInfo(
                "param", type_name=ColumnTypeName.INT, type_text="int", position=12
            ),
        ),
        (
            datetime.timedelta(days=1, hours=3),
            FunctionParameterInfo(
                "param",
                type_name=ColumnTypeName.INTERVAL,
                type_text="interval day to second",
                position=13,
            ),
        ),
        (
            "INTERVAL '1 3:00:00' DAY TO SECOND",
            FunctionParameterInfo(
                "param",
                type_name=ColumnTypeName.INTERVAL,
                type_text="interval day to second",
                position=14,
            ),
        ),
        (
            123,
            FunctionParameterInfo(
                "param", type_name=ColumnTypeName.LONG, type_text="long", position=15
            ),
        ),
        (
            {"a": 1, "b": 2},
            FunctionParameterInfo(
                "param", type_name=ColumnTypeName.MAP, type_text="map<string, int>", position=16
            ),
        ),
        (
            {"a": [1, 2, 3], "b": [4, 5, 6]},
            FunctionParameterInfo(
                "param",
                type_name=ColumnTypeName.MAP,
                type_text="map<string, array<int>>",
                position=17,
            ),
        ),
        (
            123,
            FunctionParameterInfo(
                "param", type_name=ColumnTypeName.SHORT, type_text="short", position=18
            ),
        ),
        (
            "some_string",
            FunctionParameterInfo(
                "param", type_name=ColumnTypeName.STRING, type_text="string", position=19
            ),
        ),
        (
            {"spark": 123},
            FunctionParameterInfo(
                "param", type_name=ColumnTypeName.STRUCT, type_text="struct<spark:int>", position=20
            ),
        ),
        (
            datetime.datetime(2024, 8, 19, 11, 2, 3),
            FunctionParameterInfo(
                "param", type_name=ColumnTypeName.TIMESTAMP, type_text="timestamp", position=21
            ),
        ),
        (
            "2024-08-19T11:02:03",
            FunctionParameterInfo(
                "param", type_name=ColumnTypeName.TIMESTAMP, type_text="timestamp", position=22
            ),
        ),
        (
            datetime.datetime(2024, 8, 19, 11, 2, 3),
            FunctionParameterInfo(
                "param",
                type_name=ColumnTypeName.TIMESTAMP_NTZ,
                type_text="timestamp_ntz",
                position=23,
            ),
        ),
        (
            "2024-08-19T11:02:03",
            FunctionParameterInfo(
                "param",
                type_name=ColumnTypeName.TIMESTAMP_NTZ,
                type_text="timestamp_ntz",
                position=24,
            ),
        ),
    ],
)
def test_validate_param_type(client: DatabricksFunctionClient, param_value, param_info):
    client._validate_param_type(param_value, param_info)


def test_validate_param_type_errors(client: DatabricksFunctionClient):
    with pytest.raises(ValueError, match=r"Parameter a should be of type STRING"):
        client._validate_param_type(
            123,
            FunctionParameterInfo(
                "a", type_name=ColumnTypeName.STRING, type_text="string", position=0
            ),
        )

    with pytest.raises(ValueError, match=r"Invalid datetime string"):
        client._validate_param_type(
            "2024/08/19",
            FunctionParameterInfo(
                "param", type_name=ColumnTypeName.DATE, type_text="date", position=0
            ),
        )

    with pytest.raises(
        ValueError, match=r"python timedelta can only be used for day-time interval"
    ):
        client._validate_param_type(
            datetime.timedelta(days=1),
            FunctionParameterInfo(
                "param",
                type_name=ColumnTypeName.INTERVAL,
                type_text="interval year to month",
                position=0,
            ),
        )

    with pytest.raises(ValueError, match=r"Invalid interval string"):
        client._validate_param_type(
            "INTERVAL '10-0' YEAR TO MONTH",
            FunctionParameterInfo(
                "param",
                type_name=ColumnTypeName.INTERVAL,
                type_text="interval day to second",
                position=0,
            ),
        )

    with pytest.raises(
        ValueError, match=r"The string input for column type BINARY must be base64 encoded"
    ):
        client._validate_param_type(
            "some_string",
            FunctionParameterInfo(
                "param",
                type_name=ColumnTypeName.BINARY,
                type_text="binary",
                position=0,
            ),
        )


@pytest.fixture
def good_function_info():
    func_name = random_func_name(schema=SCHEMA).split(".")[-1]
    return FunctionInfo(
        catalog_name=CATALOG,
        schema_name=SCHEMA,
        name=func_name,
        input_params=FunctionParameterInfos(
            parameters=[
                FunctionParameterInfo(
                    "a", type_name=ColumnTypeName.INT, type_text="int", position=0
                ),
                FunctionParameterInfo(
                    "b", type_name=ColumnTypeName.STRING, type_text="string", position=1
                ),
            ]
        ),
        data_type=ColumnTypeName.STRING,
        external_language="Python",
        comment="test function",
        routine_body=CreateFunctionRoutineBody.EXTERNAL,
        routine_definition="return str(a) + b",
        full_data_type="STRING",
        return_params=FunctionParameterInfos(),
        routine_dependencies=DependencyList(),
        parameter_style=CreateFunctionParameterStyle.S,
        is_deterministic=False,
        sql_data_access=CreateFunctionSqlDataAccess.NO_SQL,
        is_null_call=False,
        security_type=CreateFunctionSecurityType.DEFINER,
        specific_name=func_name,
    )


def test_create_python_function_with_invalid_arguments(client: DatabricksFunctionClient):
    def invalid_func(self, x: int) -> str:
        """
        Function with 'self' in the argument.

        Args:
            x: An integer to convert to a string.
        """
        return str(x)

    with pytest.raises(
        ValueError, match="Parameter 'self' is not allowed in the function signature."
    ):
        client.create_python_function(func=invalid_func, catalog=CATALOG, schema=SCHEMA)

    def another_invalid_func(cls, x: int) -> str:
        """
        Function with 'cls' in the argument.

        Args:
            x: An integer to convert to a string.
        """
        return str(x)

    with pytest.raises(
        ValueError, match="Parameter 'cls' is not allowed in the function signature."
    ):
        client.create_python_function(func=another_invalid_func, catalog=CATALOG, schema=SCHEMA)


def test_create_python_function_missing_return_type(client: DatabricksFunctionClient):
    def missing_return_type_func(a: int, b: int):
        """A function that lacks a return type."""
        return a + b

    with pytest.raises(
        ValueError,
        match="Return type for function 'missing_return_type_func' is not defined. Please provide a return type.",
    ):
        client.create_python_function(func=missing_return_type_func, catalog=CATALOG, schema=SCHEMA)


def test_create_python_function_not_callable(client: DatabricksFunctionClient):
    scalar = 42

    with pytest.raises(ValueError, match="The provided function is not callable"):
        client.create_python_function(func=scalar, catalog=CATALOG, schema=SCHEMA)


def test_function_with_invalid_list_return_type(client: DatabricksFunctionClient):
    def func_with_invalid_list_return(a: int) -> List:
        """A function returning a list without specifying the element type."""
        return list(range(a))

    with pytest.raises(
        ValueError,
        match=re.escape(
            "Error in return type for function 'func_with_invalid_list_return': typing.List. Please define the inner types, e.g., List[int], Tuple[str, int], Dict[str, int]."
        ),
    ):
        client.create_python_function(
            func=func_with_invalid_list_return, catalog=CATALOG, schema=SCHEMA
        )


def test_function_with_invalid_dict_return_type(client: DatabricksFunctionClient):
    def func_with_invalid_dict_return(a: int) -> Dict:
        """A function returning a dict without specifying key and value types."""
        return {f"key_{i}": i for i in range(a)}

    with pytest.raises(
        ValueError,
        match=re.escape(
            "Error in return type for function 'func_with_invalid_dict_return': typing.Dict. Please define the inner types, e.g., List[int], Tuple[str, int], Dict[str, int]."
        ),
    ):
        client.create_python_function(
            func=func_with_invalid_dict_return, catalog=CATALOG, schema=SCHEMA
        )


def test_function_with_union_return_type(client: DatabricksFunctionClient):
    def func_with_union_return(a: int) -> Union[str, int]:
        """A function returning a union type."""
        return a if a % 2 == 0 else str(a)

    with pytest.raises(
        ValueError,
        match=re.escape(
            "Error in return type for function 'func_with_union_return': typing.Union[str, int]. Union types are not supported in return types."
        ),
    ):
        client.create_python_function(func=func_with_union_return, catalog=CATALOG, schema=SCHEMA)


class MockClient:
    def __init__(self):
        self.call_count = 0
        self.refresh_count = 0
        self._is_default_client = True

    @retry_on_session_expiration
    def mock_function(self):
        if self.call_count < 2:
            self.call_count += 1
            raise Exception("session_id is no longer usable")
        return "Success"

    def refresh_client_and_session(self):
        self.refresh_count += 1


def test_retry_on_session_expiration_decorator():
    client = MockClient()

    result = client.mock_function()

    assert result == "Success"
    assert client.call_count == 2
    assert client.refresh_count == 2


@patch("time.sleep", return_value=None)
def test_retry_on_session_expiration_decorator_exceeds_attempts(mock_sleep):
    client = MockClient()
    client._is_default_client = True

    @retry_on_session_expiration
    def mock_function_always_fail(self):
        self.call_count += 1
        raise Exception("session_id is no longer usable")

    client.mock_function = mock_function_always_fail.__get__(client)

    with pytest.raises(RuntimeError, match="Failed to execute mock_function_always_fail after"):
        client.mock_function()

    assert client.call_count == int(UCAI_DATABRICKS_SESSION_RETRY_MAX_ATTEMPTS.get())


@pytest.fixture
def mock_function_info():
    mock_function_info = MagicMock()
    mock_function_info.full_name = "catalog.schema.function_name"
    mock_function_info.catalog_name = "catalog"
    mock_function_info.schema_name = "schema"
    mock_function_info.name = "function_name"
    mock_function_info.data_type = "SCALAR"
    mock_function_info.input_params.parameters = []
    return mock_function_info


@pytest.fixture
def mock_spark_session():
    with patch("databricks.connect.session.DatabricksSession") as mock_session:
        mock_spark_session = mock_session.builder.getOrCreate.return_value
        yield mock_spark_session


@pytest.fixture
def mock_workspace_client():
    with patch("databricks.sdk.WorkspaceClient") as mock_workspace_client:
        mock_client_instance = mock_workspace_client.return_value
        yield mock_client_instance


def test_execute_function_success(mock_workspace_client, mock_spark_session, mock_function_info):
    class MockResult:
        def collect(self):
            return [[42]]

    mock_spark_session.sql.return_value = MockResult()

    client = DatabricksFunctionClient(client=mock_workspace_client)

    client.set_default_spark_session = MagicMock()
    client.spark = mock_spark_session

    client.get_function = MagicMock(return_value=mock_function_info)

    result = client.execute_function("catalog.schema.function_name")

    assert result.value == "42"
    assert result.format == "SCALAR"
    assert not result.truncated

    expected_sql = "SELECT `catalog`.`schema`.`function_name`()"
    mock_spark_session.sql.assert_called_with(sqlQuery=expected_sql)


def test_execute_function_with_retry(mock_workspace_client, mock_spark_session, mock_function_info):
    with patch("time.sleep", return_value=None):
        call_count = {"count": 0}

        def side_effect(*args, **kwargs):
            call_count["count"] += 1
            if call_count["count"] == 1:
                raise Exception("session_id is no longer usable")
            else:

                class MockResult:
                    def collect(self):
                        return [[42]]

                return MockResult()

        mock_spark_session.sql.side_effect = side_effect

        client = DatabricksFunctionClient(client=mock_workspace_client)
        client.set_default_spark_session = MagicMock()
        client.spark = mock_spark_session
        client._is_default_client = True

        client.get_function = MagicMock(return_value=mock_function_info)
        client.refresh_client_and_session = MagicMock()

        result = client.execute_function("catalog.schema.function_name")

        assert result.value == "42"
        assert result.format == "SCALAR"
        assert not result.truncated

        client.refresh_client_and_session.assert_called_once()
        expected_sql = "SELECT `catalog`.`schema`.`function_name`()"
        mock_spark_session.sql.assert_called_with(sqlQuery=expected_sql)
        assert mock_spark_session.sql.call_count == 2
        assert client.refresh_client_and_session.call_count == 1


def test_create_function_with_retry(mock_workspace_client, mock_spark_session):
    with patch("time.sleep", return_value=None) as mock_time_sleep:
        sql_function_body = f"""CREATE FUNCTION catalog.schema.test(s STRING)
RETURNS STRING
LANGUAGE PYTHON
AS $$
    return s
$$
"""

        mock_spark_session.sql.side_effect = [Exception("session_id is no longer usable"), None]

        client = DatabricksFunctionClient(client=mock_workspace_client)
        client._is_default_client = True
        client.set_default_spark_session = MagicMock()
        client.spark = mock_spark_session

        mock_function_info = MagicMock()
        client.get_function = MagicMock(return_value=mock_function_info)
        client.refresh_client_and_session = MagicMock()

        result = client.create_function(sql_function_body=sql_function_body)

        assert result == mock_function_info

        client.refresh_client_and_session.assert_called_once()

        assert mock_spark_session.sql.call_count == 2
        mock_spark_session.sql.assert_called_with(sql_function_body)

        expected_function_name = extract_function_name(sql_function_body)
        client.get_function.assert_called_with(expected_function_name)

        assert mock_time_sleep.call_count == 1


def test_create_function_retry_exceeds_attempts(mock_workspace_client, mock_spark_session):
    with patch("time.sleep", return_value=None) as mock_time_sleep:
        sql_function_body = """CREATE FUNCTION `catalog`.`schema`.`function_name`()
RETURNS INT
AS $$ return 1 $$"""

        mock_spark_session.sql.side_effect = Exception("session_id is no longer usable")

        client = DatabricksFunctionClient(client=mock_workspace_client)
        client._is_default_client = True
        client.set_default_spark_session = MagicMock()
        client.spark = mock_spark_session
        client.refresh_client_and_session = MagicMock()

        with pytest.raises(RuntimeError, match="Failed to execute create_function after"):
            client.create_function(sql_function_body=sql_function_body)

        max_attempts = int(UCAI_DATABRICKS_SESSION_RETRY_MAX_ATTEMPTS.get())
        assert client.refresh_client_and_session.call_count == max_attempts - 1
        assert mock_spark_session.sql.call_count == max_attempts
        assert mock_time_sleep.call_count == max_attempts - 1


def test_no_retry_with_custom_client(mock_workspace_client, mock_spark_session, mock_function_info):
    mock_spark_session.sql.side_effect = Exception("session_id is no longer usable")

    client = DatabricksFunctionClient(client=mock_workspace_client)
    client.set_default_spark_session = MagicMock()
    client.spark = mock_spark_session
    client._is_default_client = False

    client.get_function = MagicMock(return_value=mock_function_info)

    client.refresh_client_and_session = MagicMock()

    with pytest.raises(
        RuntimeError,
        match="Failed to execute _execute_uc_functions_with_serverless due to session expiration. "
        "Unable to automatically refresh session when using a custom client.",
    ):
        client.execute_function("catalog.schema.function_name")

    client.refresh_client_and_session.assert_not_called()

    assert mock_spark_session.sql.call_count == 1


def create_mock_function_info():
    return FunctionInfo(
        catalog_name="catalog",
        schema_name="schema",
        name="mock_function",
        data_type=ColumnTypeName.STRING,
        input_params=FunctionParameterInfos(
            parameters=[
                FunctionParameterInfo(
                    name="a",
                    type_name=ColumnTypeName.INT,
                    type_text="int",
                    position=0,
                ),
                FunctionParameterInfo(
                    name="b",
                    type_name=ColumnTypeName.STRING,
                    type_text="string",
                    position=1,
                ),
            ]
        ),
    )


test_cases_string_inputs = [
    (
        {"a": 1, "b": "test"},
        "SELECT `catalog`.`schema`.`mock_function`('1','test')",
    ),
    # String with single quotes
    (
        {"a": 1, "b": "O'Reilly"},
        "SELECT `catalog`.`schema`.`mock_function`('1','O'Reilly')",
    ),
    # String with backslashes
    (
        {"a": 1, "b": "C:\\Program Files\\App"},
        "SELECT `catalog`.`schema`.`mock_function`('1','C:\\Program Files\\App')",
    ),
    # String with newlines
    (
        {"a": 1, "b": "Line1\nLine2"},
        "SELECT `catalog`.`schema`.`mock_function`('1','Line1\\nLine2')",
    ),
    # String with tabs
    (
        {"a": 1, "b": "Column1\tColumn2"},
        "SELECT `catalog`.`schema`.`mock_function`('1','Column1\tColumn2')",
    ),
    # String with various special characters
    (
        {"a": 1, "b": "Special chars: !@#$%^&*()_+-=[]{}|;':,./<>?"},
        "SELECT `catalog`.`schema`.`mock_function`('1','Special chars: !@#$%^&*()_+-=[]{}|;':,./<>?')",
    ),
    # String with Unicode characters
    (
        {"a": 1, "b": "Unicode test: ü, é, 漢字"},
        "SELECT `catalog`.`schema`.`mock_function`('1','Unicode test: ü, é, 漢字')",
    ),
    # String with double quotes
    (
        {"a": 1, "b": 'He said, "Hello"'},
        """SELECT `catalog`.`schema`.`mock_function`('1',\'He said, "Hello"\')""",
    ),
    # String with backslashes and quotes
    (
        {"a": 1, "b": "Path: C:\\User\\'name'\\\"docs\""},
        "SELECT `catalog`.`schema`.`mock_function`('1','Path: C:\\User\\'name'\\\"docs\"')",
    ),
    # String with code-like content (simulating GenAI code)
    (
        {"a": 1, "b": "def func():\n    print('Hello, world!')"},
        "SELECT `catalog`.`schema`.`mock_function`('1','def func():\\n    print(\"Hello, world!\")')",
    ),
    # String with multiline code and special characters
    (
        {"a": 1, "b": "if a > 0:\n    print('Positive')\nelse:\n    print('Non-positive')"},
        "SELECT `catalog`.`schema`.`mock_function`('1','if a > 0:\\n    print(\"Positive\")\\nelse:\\n    print(\"Non-positive\")')",
    ),
]


@pytest.mark.parametrize("parameters, expected_sql", test_cases_string_inputs)
def test_get_execute_function_sql_command_string_inputs(parameters, expected_sql):
    function_info = create_mock_function_info()

    sql_command = get_execute_function_sql_command(function_info, parameters)

    assert sql_command.replace("\r\n", "\n") == expected_sql.replace("\r\n", "\n")


def test_execute_function_with_mock_string_input(
    mock_workspace_client, mock_spark_session, mock_function_info
):
    mock_function_info.input_params = FunctionParameterInfos(
        parameters=[
            FunctionParameterInfo(
                name="a",
                type_name=ColumnTypeName.INT,
                type_text="int",
                position=0,
            ),
            FunctionParameterInfo(
                name="b",
                type_name=ColumnTypeName.STRING,
                type_text="string",
                position=1,
            ),
        ]
    )
    mock_function_info.catalog_name = "catalog"
    mock_function_info.schema_name = "schema"
    mock_function_info.name = "mock_function"

    parameters = {
        "a": 1,
        "b": "def func():\n    print('Hello, world!')",
    }
    expected_sql = f"SELECT `catalog`.`schema`.`mock_function`(:a,:b)"

    client = DatabricksFunctionClient(client=mock_workspace_client)

    client.set_default_spark_session = MagicMock()
    client.spark = mock_spark_session
    client.get_function = MagicMock(return_value=mock_function_info)

    mock_result = MagicMock()
    mock_result.collect.return_value = [[f"1-{parameters['b']}"]]
    mock_spark_session.sql.return_value = mock_result

    result = client.execute_function("catalog.schema.mock_function", parameters=parameters)

    mock_spark_session.sql.assert_called_once_with(sqlQuery=expected_sql, args=parameters)

    expected_result = f"1-{parameters['b']}"
    assert result.value == expected_result


def test_execute_function_with_genai_code_input(
    mock_workspace_client, mock_spark_session, mock_function_info
):
    mock_function_info.input_params = FunctionParameterInfos(
        parameters=[
            FunctionParameterInfo(
                name="a",
                type_name=ColumnTypeName.INT,
                type_text="int",
                position=0,
            ),
            FunctionParameterInfo(
                name="b",
                type_name=ColumnTypeName.STRING,
                type_text="string",
                position=1,
            ),
        ]
    )
    mock_function_info.catalog_name = "catalog"
    mock_function_info.schema_name = "schema"
    mock_function_info.name = "mock_function"

    genai_code = """def greet(name):
    print(f"Hello, {name}!")

greet("World")"""

    parameters = {
        "a": 1,
        "b": genai_code,
    }
    expected_sql = f"SELECT `catalog`.`schema`.`mock_function`(:a,:b)"

    client = DatabricksFunctionClient(client=mock_workspace_client)

    client.set_default_spark_session = MagicMock()
    client.spark = mock_spark_session

    client.get_function = MagicMock(return_value=mock_function_info)

    mock_result = MagicMock()
    mock_result.collect.return_value = [[f"1-{genai_code}"]]
    mock_spark_session.sql.return_value = mock_result

    result = client.execute_function("catalog.schema.mock_function", parameters=parameters)

    mock_spark_session.sql.assert_called_once_with(sqlQuery=expected_sql, args=parameters)

    assert result.value == f"1-{genai_code}"
