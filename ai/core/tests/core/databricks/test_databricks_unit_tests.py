import datetime
import json
import logging
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
    SessionExpirationException,
    dynamically_construct_python_function,
    extract_function_name,
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
        (
            "CREATE OR REPLACE FUNCTION a.b.dep(s STRING) RETURNS STRING LANGUAGE PYTHON ENVIRONMENT (dependencies = ['some-dependency']) AS $$ return s $$",
            "a.b.dep",
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
        (
            {"key": "value", "list": [1, 2, 3]},
            FunctionParameterInfo(
                "param",
                type_name=ColumnTypeName.VARIANT,
                type_text="variant",
                position=25,
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

    with pytest.raises(ValueError, match=r"VARIANT dictionary keys must be strings"):
        client._validate_param_type(
            {1: "a", 2: "b"},
            FunctionParameterInfo(
                "param",
                type_name=ColumnTypeName.VARIANT,
                type_text="variant",
                position=0,
            ),
        )

    with pytest.raises(ValueError, match=r"Unsupported type for VARIANT"):
        client._validate_param_type(
            tuple([1, 2, 3]),
            FunctionParameterInfo(
                "param",
                type_name=ColumnTypeName.VARIANT,
                type_text="variant",
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
            raise SessionExpirationException("session_id is no longer usable")
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
        raise SessionExpirationException("session_id is no longer usable")

    client.mock_function = mock_function_always_fail.__get__(client)

    with pytest.raises(RuntimeError, match="Failed to execute mock_function_always_fail after"):
        client.mock_function()

    assert client.call_count == UCAI_DATABRICKS_SESSION_RETRY_MAX_ATTEMPTS.get()


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

    client.set_spark_session = MagicMock()
    client.spark = mock_spark_session

    client.get_function = MagicMock(return_value=mock_function_info)

    result = client.execute_function("catalog.schema.function_name")

    assert result.value == "42"
    assert result.format == "SCALAR"
    assert not result.truncated

    expected_sql = "SELECT `catalog`.`schema`.`function_name`()"
    mock_spark_session.sql.assert_called_with(sqlQuery=expected_sql, args=None)


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
        client.set_spark_session = MagicMock()
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
        mock_spark_session.sql.assert_called_with(sqlQuery=expected_sql, args=None)
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

        mock_spark_session.sql.side_effect = [
            SessionExpirationException("session_id is no longer usable"),
            None,
        ]

        client = DatabricksFunctionClient(client=mock_workspace_client)
        client._is_default_client = True
        client.set_spark_session = MagicMock()
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

        mock_spark_session.sql.side_effect = SessionExpirationException(
            "session_id is no longer usable"
        )

        client = DatabricksFunctionClient(client=mock_workspace_client)
        client._is_default_client = True
        client.set_spark_session = MagicMock()
        client.spark = mock_spark_session
        client.refresh_client_and_session = MagicMock()

        with pytest.raises(RuntimeError, match="Failed to execute create_function after"):
            client.create_function(sql_function_body=sql_function_body)

        max_attempts = UCAI_DATABRICKS_SESSION_RETRY_MAX_ATTEMPTS.get()
        assert client.refresh_client_and_session.call_count == max_attempts - 1
        assert mock_spark_session.sql.call_count == max_attempts
        assert mock_time_sleep.call_count == max_attempts - 1


def test_no_retry_with_custom_client(mock_workspace_client, mock_spark_session, mock_function_info):
    mock_spark_session.sql.side_effect = SessionExpirationException(
        "session_id is no longer usable"
    )

    client = DatabricksFunctionClient(client=mock_workspace_client)
    client.set_spark_session = MagicMock()
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

    client.set_spark_session = MagicMock()
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

    client.set_spark_session = MagicMock()
    client.spark = mock_spark_session

    client.get_function = MagicMock(return_value=mock_function_info)

    mock_result = MagicMock()
    mock_result.collect.return_value = [[f"1-{genai_code}"]]
    mock_spark_session.sql.return_value = mock_result

    result = client.execute_function("catalog.schema.mock_function", parameters=parameters)

    mock_spark_session.sql.assert_called_once_with(sqlQuery=expected_sql, args=parameters)

    assert result.value == f"1-{genai_code}"


def test_execute_function_with_variant_input(
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
                type_name=ColumnTypeName.VARIANT,
                type_text="variant",
                position=1,
            ),
        ]
    )
    mock_function_info.catalog_name = "catalog"
    mock_function_info.schema_name = "schema"
    mock_function_info.name = "mock_function"

    variant_value = {"key": "value", "list": [1, 2, 3]}
    parameters = {
        "a": 10,
        "b": variant_value,
    }
    expected_sql = 'SELECT `catalog`.`schema`.`mock_function`(:a,parse_json(\'{"key": "value", "list": [1, 2, 3]}\'))'
    expected_args = {"a": 10}

    client = DatabricksFunctionClient(client=mock_workspace_client)
    client.set_spark_session = MagicMock()
    client.spark = mock_spark_session
    client.get_function = MagicMock(return_value=mock_function_info)

    result_str = f"10-{json.dumps(variant_value)}"
    mock_result = MagicMock()
    mock_result.collect.return_value = [[result_str]]
    mock_spark_session.sql.return_value = mock_result

    result = client.execute_function("catalog.schema.mock_function", parameters=parameters)

    mock_spark_session.sql.assert_called_once_with(sqlQuery=expected_sql, args=expected_args)
    assert result.value == result_str


def test_execute_function_warnings_missing_descriptions(mock_workspace_client, mock_spark_session):
    import warnings

    # Create a FunctionInfo object without parameter or function descriptions
    func_info = FunctionInfo(
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
                    comment=None,
                ),
                FunctionParameterInfo(
                    name="b",
                    type_name=ColumnTypeName.STRING,
                    type_text="string",
                    position=1,
                    comment="",
                ),
            ]
        ),
        comment=None,
    )

    client = DatabricksFunctionClient(client=mock_workspace_client)
    client.set_spark_session = MagicMock()
    client.spark = mock_spark_session
    client.get_function = MagicMock(return_value=func_info)

    mock_result = MagicMock()
    mock_result.collect.return_value = [["test_result"]]
    mock_spark_session.sql.return_value = mock_result

    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")

        client.execute_function("catalog.schema.mock_function", parameters={"a": 1, "b": "test"})

        assert len(w) == 2

        messages = [str(warning.message) for warning in w]

        assert any(
            "The following parameters do not have descriptions: a, b" in msg for msg in messages
        ), "Warning about missing parameter descriptions was not issued."

        assert any(
            "The function mock_function does not have a description." in msg for msg in messages
        ), "Warning about missing function description was not issued."


def test_create_python_function_with_dependencies(client: DatabricksFunctionClient):
    def sample_func(a: int, b: str) -> str:
        """
        Sample function that concatenates an integer and a string.

        Args:
            a (int): An integer value.
            b (str): A string value.

        Returns:
            str: The concatenated result.
        """
        return f"{a}-{b}"

    dependencies = ["numpy", "pandas"]

    expected_sql_body = (
        "CREATE OR REPLACE FUNCTION catalog.schema.sample_func(a INT, b STRING) "
        "RETURNS STRING "
        "LANGUAGE PYTHON "
        "ENVIRONMENT (dependencies = ['numpy', 'pandas'], environment_version = 'None') "
        "AS $$\n"
        "import numpy\n"
        "import pandas\n"
        "def sample_func(a: int, b: str) -> str:\n"
        '    return f"{a}-{b}"\n'
        "$$;"
    )

    with patch(
        "unitycatalog.ai.core.databricks.generate_sql_function_body",
        return_value=expected_sql_body,
    ) as mock_generate_sql:
        mock_function_info = create_mock_function_info()
        client.create_function = MagicMock(return_value=mock_function_info)

        result = client.create_python_function(
            func=sample_func,
            catalog="catalog",
            schema="schema",
            dependencies=dependencies,
        )

        mock_generate_sql.assert_called_once_with(
            sample_func,
            "catalog",
            "schema",
            False,
            dependencies,
            "None",
        )

        client.create_function.assert_called_once_with(sql_function_body=expected_sql_body)

        assert result == mock_function_info


def test_create_python_function_with_environment_version(client: DatabricksFunctionClient):
    def another_sample_func(x: float, y: float) -> float:
        """
        Function to add two floating-point numbers.

        Args:
            x (float): First number.
            y (float): Second number.

        Returns:
            float: The sum of x and y.
        """
        return x + y

    environment_version = "1"

    expected_sql_body = (
        "CREATE OR REPLACE FUNCTION catalog.schema.another_sample_func(x DOUBLE, y DOUBLE) "
        "RETURNS DOUBLE "
        "LANGUAGE PYTHON "
        "ENVIRONMENT (environment_version = '1') "
        "AS $$\n"
        "def another_sample_func(x: float, y: float) -> float:\n"
        "    return x + y\n"
        "$$;"
    )

    with patch(
        "unitycatalog.ai.core.databricks.generate_sql_function_body",
        return_value=expected_sql_body,
    ) as mock_generate_sql:
        mock_function_info = create_mock_function_info()
        client.create_function = MagicMock(return_value=mock_function_info)

        result = client.create_python_function(
            func=another_sample_func,
            catalog="catalog",
            schema="schema",
            environment_version=environment_version,
        )

        mock_generate_sql.assert_called_once_with(
            another_sample_func,
            "catalog",
            "schema",
            False,
            None,
            environment_version,
        )

        client.create_function.assert_called_once_with(sql_function_body=expected_sql_body)

        assert result == mock_function_info


def test_workspace_provided_issues_warning(mock_workspace_client, caplog):
    with (
        caplog.at_level(logging.WARNING),
        patch(
            "unitycatalog.ai.core.databricks.get_default_databricks_workspace_client",
            return_value=MagicMock(),
        ),
        patch(
            "unitycatalog.ai.core.databricks.DatabricksFunctionClient.set_spark_session",
            lambda self: None,
        ),
    ):
        DatabricksFunctionClient(client=mock_workspace_client, warehouse_id="id")

    assert "The argument `warehouse_id` was specified" in caplog.text


def dummy_primary(a: int, b: str) -> str:
    """
    Dummy primary function.

    Args:
        a: int
        b: str

    Returns:
        A string.
    """
    return a + b


def dummy_func1(a):
    """Dummy wrapped function 1."""
    return a + 10


def dummy_func2(b):
    """Dummy wrapped function 2."""
    return f"{b}!"


def test_create_wrapped_function_databricks(mock_workspace_client, mock_spark_session):
    dummy_sql_body = (
        "CREATE FUNCTION cat.sch.dummy_primary() RETURNS STRING LANGUAGE PYTHON AS $$ dummy SQL $$;"
    )

    with patch(
        "unitycatalog.ai.core.databricks.generate_wrapped_sql_function_body",
        return_value=dummy_sql_body,
    ) as mock_gen_sql:
        with patch.object(
            DatabricksFunctionClient, "create_function", return_value="dummy_func_info"
        ) as mock_create_func:
            client = DatabricksFunctionClient(client=mock_workspace_client)
            client.set_spark_session = MagicMock()
            client.spark = mock_spark_session

            result = client.create_wrapped_function(
                primary_func=dummy_primary,
                functions=[dummy_func1, dummy_func2],
                catalog="cat",
                schema="sch",
                replace=True,
            )

            mock_gen_sql.assert_called_once_with(
                primary_func=dummy_primary,
                functions=[dummy_func1, dummy_func2],
                catalog="cat",
                schema="sch",
                replace=True,
                dependencies=None,
                environment_version="None",
            )
            mock_create_func.assert_called_once_with(sql_function_body=dummy_sql_body)
            assert result == "dummy_func_info"


def test_create_wrapped_function_with_dependencies(mock_workspace_client, mock_spark_session):
    dummy_sql_body = "CREATE FUNCTION cat.sch.dummy_primary() RETURNS STRING LANGUAGE PYTHON ENVIRONMENT (dependencies = '[\"scipy==1.15.0\"]', environment_version = '1')AS $$ dummy SQL $$;"

    with patch(
        "unitycatalog.ai.core.databricks.generate_wrapped_sql_function_body",
        return_value=dummy_sql_body,
    ) as mock_gen_sql:
        with patch.object(
            DatabricksFunctionClient, "create_function", return_value="dummy_func_info"
        ) as mock_create_func:
            client = DatabricksFunctionClient(client=mock_workspace_client)
            client.set_spark_session = MagicMock()
            client.spark = mock_spark_session

            result = client.create_wrapped_function(
                primary_func=dummy_primary,
                functions=[dummy_func1, dummy_func2],
                catalog="cat",
                schema="sch",
                replace=True,
                dependencies=["scipy==1.15.0"],
                environment_version="1",
            )

            mock_gen_sql.assert_called_once_with(
                primary_func=dummy_primary,
                functions=[dummy_func1, dummy_func2],
                catalog="cat",
                schema="sch",
                replace=True,
                dependencies=["scipy==1.15.0"],
                environment_version="1",
            )
            mock_create_func.assert_called_once_with(sql_function_body=dummy_sql_body)
            assert result == "dummy_func_info"


def test_create_wrapped_function_invalid_primary_databricks(client: DatabricksFunctionClient):
    with pytest.raises(ValueError, match="The provided primary function is not callable."):
        client.create_wrapped_function(
            primary_func="not_callable",
            functions=[dummy_func1, dummy_func2],
            catalog="cat",
            schema="sch",
            replace=False,
        )


def test_execute_function_with_default_params_databricks(mock_workspace_client, mock_spark_session):
    dummy_func_info = FunctionInfo(
        catalog_name=CATALOG,
        schema_name=SCHEMA,
        name="func_with_defaults",
        data_type=ColumnTypeName.STRING,
        input_params=FunctionParameterInfos(
            parameters=[
                FunctionParameterInfo(
                    name="a",
                    type_name=ColumnTypeName.INT,
                    type_text="int",
                    position=0,
                    parameter_default=None,
                ),
                FunctionParameterInfo(
                    name="b",
                    type_name=ColumnTypeName.STRING,
                    type_text="string",
                    position=1,
                    parameter_default="'default'",
                ),
            ]
        ),
        external_language="PYTHON",
        comment="Test function with defaults",
        routine_body=CreateFunctionRoutineBody.EXTERNAL,
        routine_definition="return str(a) + ' ' + b",
        full_data_type="STRING",
        return_params=FunctionParameterInfos(parameters=[]),
        routine_dependencies=DependencyList(),
        parameter_style=CreateFunctionParameterStyle.S,
        is_deterministic=True,
        sql_data_access=CreateFunctionSqlDataAccess.NO_SQL,
        is_null_call=False,
        security_type=CreateFunctionSecurityType.DEFINER,
        specific_name="func_with_defaults",
    )

    client = DatabricksFunctionClient(client=mock_workspace_client)
    client.set_spark_session = MagicMock()
    client.spark = mock_spark_session
    client.get_function = MagicMock(return_value=dummy_func_info)

    mock_result = MagicMock()
    mock_result.collect.return_value = [["10 default"]]
    mock_spark_session.sql.return_value = mock_result

    result = client.execute_function(f"{CATALOG}.{SCHEMA}.func_with_defaults", parameters={"a": 10})
    assert result.value == "10 default"

    mock_result.collect.return_value = [["20 test"]]
    result = client.execute_function(
        f"{CATALOG}.{SCHEMA}.func_with_defaults", parameters={"a": 20, "b": "test"}
    )
    assert result.value == "20 test"


def test_execute_function_with_all_defaults_databricks(mock_workspace_client, mock_spark_session):
    dummy_func_info = FunctionInfo(
        catalog_name=CATALOG,
        schema_name=SCHEMA,
        name="func_with_all_defaults",
        data_type=ColumnTypeName.STRING,
        input_params=FunctionParameterInfos(
            parameters=[
                FunctionParameterInfo(
                    name="a",
                    type_name=ColumnTypeName.INT,
                    type_text="int",
                    position=0,
                    parameter_default="1",
                ),
                FunctionParameterInfo(
                    name="b",
                    type_name=ColumnTypeName.STRING,
                    type_text="string",
                    position=1,
                    parameter_default="'default'",
                ),
                FunctionParameterInfo(
                    name="c",
                    type_name=ColumnTypeName.DOUBLE,
                    type_text="double",
                    position=2,
                    parameter_default="3.14",
                ),
                FunctionParameterInfo(
                    name="d",
                    type_name=ColumnTypeName.BOOLEAN,
                    type_text="boolean",
                    position=3,
                    parameter_default="True",
                ),
            ]
        ),
        external_language="PYTHON",
        comment="Test function with all defaults",
        routine_body=CreateFunctionRoutineBody.EXTERNAL,
        routine_definition="return str(a) + ' ' + b + ' ' + str(c) + ' ' + str(d)",
        full_data_type="STRING",
        return_params=FunctionParameterInfos(parameters=[]),
        routine_dependencies=DependencyList(),
        parameter_style=CreateFunctionParameterStyle.S,
        is_deterministic=True,
        sql_data_access=CreateFunctionSqlDataAccess.NO_SQL,
        is_null_call=False,
        security_type=CreateFunctionSecurityType.DEFINER,
        specific_name="func_with_all_defaults",
    )

    client = DatabricksFunctionClient(client=mock_workspace_client)
    client.set_spark_session = MagicMock()
    client.spark = mock_spark_session
    client.get_function = MagicMock(return_value=dummy_func_info)

    mock_result = MagicMock()
    mock_result.collect.return_value = [["1 default 3.14 True"]]
    mock_spark_session.sql.return_value = mock_result

    result = client.execute_function(f"{CATALOG}.{SCHEMA}.func_with_all_defaults", parameters={})
    assert result.value == "1 default 3.14 True"

    mock_result.collect.return_value = [["10 test 2.71 False"]]
    result = client.execute_function(
        f"{CATALOG}.{SCHEMA}.func_with_all_defaults",
        parameters={"a": 10, "b": "test", "c": 2.71, "d": False},
    )
    assert result.value == "10 test 2.71 False"


def test_execute_function_no_params_databricks(mock_workspace_client, mock_spark_session):
    dummy_func_info = FunctionInfo(
        catalog_name=CATALOG,
        schema_name=SCHEMA,
        name="func_no_params",
        data_type=ColumnTypeName.STRING,
        input_params=FunctionParameterInfos(parameters=[]),
        external_language="PYTHON",
        comment="Test function with no parameters",
        routine_body=CreateFunctionRoutineBody.EXTERNAL,
        routine_definition="return 'No parameters here!'",
        full_data_type="STRING",
        return_params=FunctionParameterInfos(parameters=[]),
        routine_dependencies=DependencyList(),
        parameter_style=CreateFunctionParameterStyle.S,
        is_deterministic=True,
        sql_data_access=CreateFunctionSqlDataAccess.NO_SQL,
        is_null_call=False,
        security_type=CreateFunctionSecurityType.DEFINER,
        specific_name="func_no_params",
    )

    client = DatabricksFunctionClient(client=mock_workspace_client)
    client.set_spark_session = MagicMock()
    client.spark = mock_spark_session
    client.get_function = MagicMock(return_value=dummy_func_info)

    mock_result = MagicMock()
    mock_result.collect.return_value = [["No parameters here!"]]
    mock_spark_session.sql.return_value = mock_result

    result = client.execute_function(f"{CATALOG}.{SCHEMA}.func_no_params", parameters={})
    assert result.value == "No parameters here!"

    with pytest.raises(ValueError, match="Function does not have input parameters, but parameters"):
        client.execute_function(
            f"{CATALOG}.{SCHEMA}.func_no_params", parameters={"unexpected": "value"}
        )


def test_execute_function_with_none_default_databricks(mock_workspace_client, mock_spark_session):
    dummy_func_info = FunctionInfo(
        catalog_name=CATALOG,
        schema_name=SCHEMA,
        name="null_func",
        data_type=ColumnTypeName.STRING,
        input_params=FunctionParameterInfos(
            parameters=[
                FunctionParameterInfo(
                    name="a",
                    type_name=ColumnTypeName.STRING,
                    type_text="string",
                    position=0,
                    parameter_default="NULL",
                )
            ]
        ),
        external_language="PYTHON",
        comment="Test function with None default",
        routine_body=CreateFunctionRoutineBody.EXTERNAL,
        routine_definition="return str(a)",
        full_data_type="STRING",
        return_params=FunctionParameterInfos(parameters=[]),
        routine_dependencies=DependencyList(),
        parameter_style=CreateFunctionParameterStyle.S,
        is_deterministic=True,
        sql_data_access=CreateFunctionSqlDataAccess.NO_SQL,
        is_null_call=False,
        security_type=CreateFunctionSecurityType.DEFINER,
        specific_name="null_func",
    )

    client = DatabricksFunctionClient(client=mock_workspace_client)
    client.set_spark_session = MagicMock()
    client.spark = mock_spark_session
    client.get_function = MagicMock(return_value=dummy_func_info)

    mock_result = MagicMock()
    mock_result.collect.return_value = [["None"]]
    mock_spark_session.sql.return_value = mock_result

    result = client.execute_function(f"{CATALOG}.{SCHEMA}.null_func", parameters={})
    assert result.error is None, f"Execution error: {result.error}"
    assert result.value == "None"


class DummyParameter:
    def __init__(self, name: str, type_text: str, comment: str):
        self.name = name
        self.type_text = type_text
        self.comment = comment


class DummyInputParams:
    def __init__(self, parameters):
        self.parameters = parameters


class DummyType:
    def __init__(self, value: str, name: str):
        self.value = value
        self.__name__ = name


class DummyFunctionInfo:
    pass


def test_get_python_callable_valid(client: DatabricksFunctionClient):
    fake_function_info = MagicMock()
    fake_function_info.name = "my_func"
    fake_function_info.routine_body = DummyType("EXTERNAL", "EXTERNAL")
    fake_function_info.routine_definition = "return x * 2"
    fake_function_info.comment = "A function that doubles the input"

    fake_param = MagicMock()
    fake_param.name = "x"
    fake_param.type_text = "int"
    fake_param.type_json = (
        '{"name": "x", "type": "int", "nullable": true, "metadata": {"comment": "an int"}}'
    )
    fake_param.comment = "an int"
    fake_function_info.input_params = DummyInputParams([fake_param])
    fake_function_info.full_data_type = "STRING"

    client.get_function = MagicMock(return_value=fake_function_info)

    result = client.get_function_source("my_func")
    assert "def my_func(x: int) -> str:" in result
    assert '"""' in result
    assert "A function that doubles the input" in result
    assert "Args:" in result
    assert "x: an int" in result
    assert "return x * 2" in result


def test_get_python_callable_non_external(client: DatabricksFunctionClient):
    fake_function_info = MagicMock()
    fake_function_info.name = "non_external_func"
    fake_function_info.routine_body = MagicMock(value="SQL")
    fake_function_info.routine_definition = "SELECT 1"
    fake_function_info.input_params = MagicMock(parameters=[])
    fake_function_info.data_type = MagicMock(value="STRING")

    client.get_function = MagicMock(return_value=fake_function_info)

    with pytest.raises(ValueError, match="is not an EXTERNAL Python function"):
        client.get_function_source("non_external_func")


def test_get_python_callable_multiline(client: DatabricksFunctionClient):
    fake_function_info = MagicMock()
    fake_function_info.name = "multi_line_func"
    fake_function_info.routine_body = DummyType("EXTERNAL", "EXTERNAL")
    fake_function_info.routine_definition = "line1\nline2\nline3"
    fake_function_info.input_params = DummyInputParams([])  # No parameters.
    fake_function_info.full_data_type = "STRING"
    fake_function_info.comment = ""

    client.get_function = MagicMock(return_value=fake_function_info)

    result = client.get_function_source("multi_line_func")
    assert "def multi_line_func() -> str:" in result
    assert "line1" in result
    assert "line2" in result
    assert "line3" in result


@pytest.fixture
def complex_function_info():
    # Build parameters for a complex function.
    a = DummyParameter(
        "a",
        "LONG",
        "an int",
    )
    b = DummyParameter(
        "b",
        "DOUBLE",
        "a float",
    )
    c = DummyParameter(
        "c",
        "STRING",
        "a string",
    )
    d = DummyParameter(
        "d",
        "BOOLEAN",
        "some bool",
    )
    e = DummyParameter(
        "e",
        "ARRAY<STRING>",
        "a list of str",
    )
    f = DummyParameter(
        "f",
        "MAP<STRING, LONG>",
        "a dict of str to int",
    )
    g = DummyParameter(
        "g",
        "VARIANT",
        "a variant",
    )
    h = DummyParameter(
        "h",
        "MAP<STRING, ARRAY<LONG>>",
        "a complex type",
    )
    i = DummyParameter(
        "i",
        "MAP<STRING, ARRAY<MAP<STRING, ARRAY<LONG>>>>",
        "a very complex type",
    )

    dummy_params = DummyInputParams([a, b, c, d, e, f, g, h, i])

    func_info = DummyFunctionInfo()
    func_info.name = "test_func"
    func_info.input_params = dummy_params
    func_info.routine_body = DummyType("EXTERNAL", "EXTERNAL")
    func_info.routine_definition = (
        "    def _internal(g: float) -> int:\n"
        "        return g + c\n\n"
        "    return str(a+b+_internal(4.5)) + c + str(d) + str(e) + str(f) + str(g)"
    )
    func_info.full_data_type = "MAP<STRING, ARRAY<STRING>>"
    func_info.comment = "Just doing some testing here"
    return func_info


def test_reconstruct_callable_complex_function(complex_function_info):
    reconstructed = dynamically_construct_python_function(complex_function_info)

    expected_header = (
        "test_func(a: int, b: float, c: str, d: bool, e: list[str], f: dict[str, int], "
        "g: Variant, h: dict[str, list[int]], i: dict[str, list[dict[str, list[int]]]]) -> dict[str, list[str]]"
    )
    assert expected_header in reconstructed

    assert "Just doing some testing here" in reconstructed
    assert "a: an int" in reconstructed
    assert "b: a float" in reconstructed
    assert "c: a string" in reconstructed
    assert "d: some bool" in reconstructed
    assert "e: a list of str" in reconstructed
    assert "f: a dict of str to int" in reconstructed
    assert "g: a variant" in reconstructed
    assert "h: a complex type" in reconstructed
    assert "i: a very complex type" in reconstructed

    assert "def _internal(g: float) -> int:" in reconstructed
    assert "return str(a+b+_internal(4.5))" in reconstructed


def test_local_execution_mode_warning(caplog):
    with (
        caplog.at_level(logging.WARNING),
        patch(
            "unitycatalog.ai.core.databricks.get_default_databricks_workspace_client",
            return_value=MagicMock(),
        ),
        patch(
            "unitycatalog.ai.core.databricks.DatabricksFunctionClient.set_spark_session",
            lambda self: None,
        ),
    ):
        DatabricksFunctionClient(execution_mode="local")
    assert "You are running in 'local' execution mode, which is intended" in caplog.text


@pytest.mark.parametrize(
    "execution_mode",
    ["LOCAL", "remote", "ServerLess", "invalid"],
)
def test_invalid_execution_mode(execution_mode):
    with (
        patch(
            "unitycatalog.ai.core.databricks.get_default_databricks_workspace_client",
            return_value=MagicMock(),
        ),
        patch(
            "unitycatalog.ai.core.databricks.DatabricksFunctionClient.set_spark_session",
            lambda self: None,
        ),
        pytest.raises(
            ValueError, match=f"Execution mode '{execution_mode}' is not valid. Allowed values"
        ),
    ):
        DatabricksFunctionClient(execution_mode=execution_mode)
