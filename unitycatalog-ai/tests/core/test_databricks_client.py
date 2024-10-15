import base64
import datetime
import os
import re
import time
from decimal import Decimal
from typing import Any, Callable, Dict, List, NamedTuple, Union

import pytest
from databricks.sdk.errors import ResourceDoesNotExist
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

from ucai.core.databricks import (
    DatabricksFunctionClient,
    extract_function_name,
)
from ucai.core.envs.databricks_env_vars import (
    UCAI_DATABRICKS_SERVERLESS_EXECUTION_RESULT_ROW_LIMIT,
    UCAI_DATABRICKS_WAREHOUSE_EXECUTE_FUNCTION_WAIT_TIMEOUT,
    UCAI_DATABRICKS_WAREHOUSE_RETRY_TIMEOUT,
)
from ucai.test_utils.client_utils import (
    client,  # noqa: F401
    requires_databricks,
    serverless_client,  # noqa: F401
)
from ucai.test_utils.function_utils import (
    CATALOG,
    create_python_function_and_cleanup,
    generate_func_name_and_cleanup,
    random_func_name,
)

SCHEMA = os.environ.get("SCHEMA", "ucai_core_test")


class FunctionInputOutput(NamedTuple):
    sql_body: str
    inputs: List[Dict[str, Any]]
    output: str


class PythonFunctionInputOutput(NamedTuple):
    func: Callable
    inputs: List[Dict[str, Any]]
    output: str


def function_with_struct_input(func_name: str) -> FunctionInputOutput:
    sql_body = f"""CREATE OR REPLACE FUNCTION {func_name}(s STRUCT<a: SHORT NOT NULL COMMENT 'short field', b: MAP<STRING, FLOAT>, c: INT NOT NULL>)
RETURNS STRING
LANGUAGE PYTHON
AS $$
  result = str(s['a']) + ";"
  if s['b']:
    result += ",".join([str(k) + "=>" + str(v) for k, v in s['b'].items()])
  result += ";" + str(s['c'])
  return result
$$
"""
    return FunctionInputOutput(
        sql_body=sql_body,
        inputs=[{"s": {"a": 1, "b": {"2": 2, "3.0": 3.0}, "c": 4}}],
        output="1;2=>2.0,3.0=>3.0;4",
    )


def function_with_array_input(func_name: str) -> FunctionInputOutput:
    sql_body = f"""CREATE FUNCTION {func_name}(s ARRAY<BYTE>)
RETURNS STRING
LANGUAGE PYTHON
AS $$
    return ",".join(str(i) for i in s)
$$
"""
    return FunctionInputOutput(
        sql_body=sql_body,
        inputs=[{"s": [1, 2, 3]}],
        output="1,2,3",
    )


def function_with_string_input(func_name: str) -> FunctionInputOutput:
    sql_body = f"""CREATE FUNCTION {func_name}(s STRING)
RETURNS STRING
LANGUAGE PYTHON
AS $$
    return s
$$
"""
    return FunctionInputOutput(
        sql_body=sql_body,
        inputs=[{"s": "abc"}],
        output="abc",
    )


def function_with_binary_input(func_name: str) -> FunctionInputOutput:
    sql_body = f"""CREATE FUNCTION {func_name}(s BINARY)
RETURNS STRING
LANGUAGE PYTHON
AS $$
  return s.decode('utf-8')
$$
"""
    return FunctionInputOutput(
        sql_body=sql_body,
        inputs=[
            {"s": base64.b64encode(b"Hello").decode("utf-8")},
            {"s": "SGVsbG8="},
            {"s": b"Hello"},
        ],
        output="Hello",
    )


def function_with_interval_input(func_name: str) -> FunctionInputOutput:
    sql_body = f"""CREATE FUNCTION {func_name}(s INTERVAL DAY TO SECOND)
RETURNS STRING
LANGUAGE PYTHON
AS $$
  import datetime

  return (datetime.datetime(2024, 8, 19) - s).isoformat()
$$
"""
    return FunctionInputOutput(
        sql_body=sql_body,
        inputs=[
            {"s": "INTERVAL '0 0:16:40.123456' DAY TO SECOND"},
            {"s": datetime.timedelta(days=0, hours=0, minutes=16, seconds=40, microseconds=123456)},
            {"s": datetime.timedelta(days=0, seconds=1000, microseconds=123456)},
        ],
        output="2024-08-18T23:43:19.876544",
    )


def function_with_timestamp_input(func_name: str) -> FunctionInputOutput:
    sql_body = f"""CREATE FUNCTION {func_name}(x TIMESTAMP, y TIMESTAMP_NTZ)
RETURNS STRING
LANGUAGE PYTHON
AS $$
  return str(x.isoformat()) + "; " + str(y.isoformat())
$$
"""
    return FunctionInputOutput(
        sql_body=sql_body,
        inputs=[
            {
                "x": datetime.datetime(2024, 8, 19, 11, 2, 3),
                "y": datetime.datetime(2024, 8, 19, 11, 2, 3),
            },
            {"x": "2024-08-19T11:02:03", "y": "2024-08-19T11:02:03"},
        ],
        output="2024-08-19T11:02:03+00:00; 2024-08-19T11:02:03",
    )


def function_with_date_input(func_name: str) -> FunctionInputOutput:
    sql_body = f"""CREATE FUNCTION {func_name}(s DATE)
RETURNS STRING
LANGUAGE PYTHON
AS $$
  return s.isoformat()
$$
"""
    return FunctionInputOutput(
        sql_body=sql_body,
        inputs=[{"s": datetime.date(2024, 8, 19)}, {"s": "2024-08-19"}],
        output="2024-08-19",
    )


def function_with_map_input(func_name: str) -> FunctionInputOutput:
    sql_body = f"""CREATE FUNCTION {func_name}(s MAP<STRING, ARRAY<INT>>)
RETURNS STRING
LANGUAGE PYTHON
AS $$
  result = []
  for x, y in s.items():
     result.append(str(x) + " => " + str(y))
  return ",".join(result)
$$
"""
    return FunctionInputOutput(
        sql_body=sql_body,
        inputs=[{"s": {"a": [1, 2, 3], "b": [4, 5, 6]}}],
        output="a => [1, 2, 3],b => [4, 5, 6]",
    )


def function_with_decimal_input(func_name: str) -> FunctionInputOutput:
    sql_body = f"""CREATE FUNCTION {func_name}(s DECIMAL(10, 2))
RETURNS STRING
LANGUAGE PYTHON
AS $$
    return str(s)
$$
"""
    return FunctionInputOutput(
        sql_body=sql_body,
        inputs=[{"s": 123.45}, {"s": Decimal("123.45")}],
        output="123.45",
    )


def function_with_table_output(func_name: str) -> FunctionInputOutput:
    function_name = func_name.split(".")[-1]
    sql_body = f"""CREATE FUNCTION {func_name}(start DATE, end DATE)
RETURNS TABLE(day_of_week STRING, day DATE)
RETURN SELECT extract(DAYOFWEEK_ISO FROM day), day
            FROM (SELECT sequence({function_name}.start, {function_name}.end)) AS T(days)
                LATERAL VIEW explode(days) AS day
            WHERE extract(DAYOFWEEK_ISO FROM day) BETWEEN 1 AND 5;
"""
    return FunctionInputOutput(
        sql_body=sql_body,
        inputs=[{"start": datetime.date(2024, 1, 1), "end": "2024-01-07"}],
        output="day_of_week,day\n1,2024-01-01\n2,2024-01-02\n3,2024-01-03\n4,2024-01-04\n5,2024-01-05\n",
    )


def python_function_with_dict_input() -> PythonFunctionInputOutput:
    def function_with_dict_input(s: Dict[str, int]) -> int:
        """Python function that sums the values in a dictionary."""
        return sum(s.values())

    return PythonFunctionInputOutput(
        func=function_with_dict_input,
        inputs=[{"s": {"a": 1, "b": 3, "c": 4}}],
        output="8",
    )


def python_function_with_array_input() -> PythonFunctionInputOutput:
    def function_with_array_input(s: List[int]) -> str:
        """Python function with array input"""
        return ",".join(str(i) for i in s)

    return PythonFunctionInputOutput(
        func=function_with_array_input,
        inputs=[{"s": [1, 2, 3]}],
        output="1,2,3",
    )


def python_function_with_string_input() -> PythonFunctionInputOutput:
    def function_with_string_input(s: str) -> str:
        """Python function with string input"""
        return s

    return PythonFunctionInputOutput(
        func=function_with_string_input,
        inputs=[{"s": "abc"}],
        output="abc",
    )


def python_function_with_binary_input() -> PythonFunctionInputOutput:
    def function_with_binary_input(s: bytes) -> str:
        """Python function with binary input"""
        return s.decode("utf-8")

    return PythonFunctionInputOutput(
        func=function_with_binary_input,
        inputs=[
            {"s": base64.b64encode(b"Hello").decode("utf-8")},
            {"s": "SGVsbG8="},
        ],
        output="Hello",
    )


def python_function_with_interval_input() -> PythonFunctionInputOutput:
    def function_with_interval_input(s: datetime.timedelta) -> str:
        """Python function with interval input"""
        import datetime

        return (datetime.datetime(2024, 8, 19) - s).isoformat()

    return PythonFunctionInputOutput(
        func=function_with_interval_input,
        inputs=[
            {"s": datetime.timedelta(days=0, hours=0, minutes=16, seconds=40, microseconds=123456)},
            {"s": datetime.timedelta(days=0, seconds=1000, microseconds=123456)},
        ],
        output="2024-08-18T23:43:19.876544",
    )


def python_function_with_timestamp_input() -> PythonFunctionInputOutput:
    def function_with_timestamp_input(x: datetime.datetime, y: datetime.datetime) -> str:
        """Python function with timestamp input"""
        return str(x.isoformat()) + "; " + str(y.isoformat())

    return PythonFunctionInputOutput(
        func=function_with_timestamp_input,
        inputs=[
            {
                "x": datetime.datetime(2024, 8, 19, 11, 2, 3),
                "y": datetime.datetime(2024, 8, 19, 11, 2, 3),
            },
            {"x": "2024-08-19T11:02:03", "y": "2024-08-19T11:02:03"},
        ],
        output="2024-08-19T11:02:03+00:00; 2024-08-19T11:02:03+00:00",
    )


def python_function_with_date_input() -> PythonFunctionInputOutput:
    def function_with_date_input(s: datetime.date) -> str:
        """Python function with date input"""
        return s.isoformat()

    return PythonFunctionInputOutput(
        func=function_with_date_input,
        inputs=[{"s": datetime.date(2024, 8, 19)}, {"s": "2024-08-19"}],
        output="2024-08-19",
    )


def python_function_with_map_input() -> PythonFunctionInputOutput:
    def function_with_map_input(s: Dict[str, List[int]]) -> str:
        """Python function with map input"""
        result = []
        for key, value in s.items():
            result.append(str(key) + " => " + str(value))
        return ",".join(result)

    return PythonFunctionInputOutput(
        func=function_with_map_input,
        inputs=[{"s": {"a": [1, 2, 3], "b": [4, 5, 6]}}],
        output="a => [1, 2, 3],b => [4, 5, 6]",
    )


def python_function_with_decimal_input() -> PythonFunctionInputOutput:
    def function_with_decimal_input(s: Decimal) -> str:
        """Python function with decimal input."""
        return format(s, ".20g")

    return PythonFunctionInputOutput(
        func=function_with_decimal_input,
        inputs=[{"s": Decimal("123.45123456789457000")}],
        output="123.45123456789457000",
    )


@requires_databricks
@pytest.mark.parametrize(
    "create_function",
    [
        function_with_array_input,
        function_with_struct_input,
        function_with_string_input,
        function_with_binary_input,
        function_with_interval_input,
        function_with_timestamp_input,
        function_with_date_input,
        function_with_map_input,
        function_with_decimal_input,
        function_with_table_output,
    ],
)
def test_create_and_execute_function(
    client: DatabricksFunctionClient, create_function: Callable[[str], FunctionInputOutput]
):
    with generate_func_name_and_cleanup(client, schema=SCHEMA) as func_name:
        function_sample = create_function(func_name)
        client.create_function(sql_function_body=function_sample.sql_body)
        for input_example in function_sample.inputs:
            result = client.execute_function(func_name, input_example)
            assert result.value == function_sample.output


@requires_databricks
@pytest.mark.parametrize(
    "create_function",
    [
        function_with_array_input,
        function_with_struct_input,
        function_with_string_input,
        function_with_binary_input,
        function_with_interval_input,
        function_with_timestamp_input,
        function_with_date_input,
        function_with_map_input,
        function_with_decimal_input,
        function_with_table_output,
    ],
)
def test_create_and_execute_function_using_serverless(
    serverless_client: DatabricksFunctionClient,
    create_function: Callable[[str], FunctionInputOutput],
):
    with generate_func_name_and_cleanup(serverless_client, schema=SCHEMA) as func_name:
        function_sample = create_function(func_name)
        serverless_client.create_function(sql_function_body=function_sample.sql_body)
        for input_example in function_sample.inputs:
            result = serverless_client.execute_function(func_name, input_example)
            assert result.value == function_sample.output


@requires_databricks
@pytest.mark.parametrize(
    "create_function",
    [
        python_function_with_dict_input,
        python_function_with_array_input,
        python_function_with_string_input,
        python_function_with_binary_input,
        python_function_with_interval_input,
        python_function_with_timestamp_input,
        python_function_with_date_input,
        python_function_with_map_input,
        python_function_with_decimal_input,
    ],
)
def test_create_and_execute_python_function(
    client: DatabricksFunctionClient, create_function: Callable[[], PythonFunctionInputOutput]
):
    function_sample = create_function()
    with create_python_function_and_cleanup(
        client, func=function_sample.func, schema=SCHEMA
    ) as func_obj:
        for input_example in function_sample.inputs:
            result = client.execute_function(func_obj.full_function_name, input_example)
            assert result.value == function_sample.output


@requires_databricks
def test_execute_function_using_serverless_row_limit(
    serverless_client: DatabricksFunctionClient,
    monkeypatch,
):
    monkeypatch.setenv(UCAI_DATABRICKS_SERVERLESS_EXECUTION_RESULT_ROW_LIMIT.name, "1")
    with generate_func_name_and_cleanup(serverless_client, schema=SCHEMA) as func_name:
        function_sample = function_with_table_output(func_name)
        serverless_client.create_function(sql_function_body=function_sample.sql_body)
        result = serverless_client.execute_function(func_name, function_sample.inputs[0])
        assert result.value == "day_of_week,day\n1,2024-01-01\n"
        assert result.truncated is True


@requires_databricks
def test_execute_function_with_timeout(client: DatabricksFunctionClient, monkeypatch):
    monkeypatch.setenv(UCAI_DATABRICKS_WAREHOUSE_RETRY_TIMEOUT.name, "5")
    with generate_func_name_and_cleanup(client, schema=SCHEMA) as func_name:
        sql_body = f"""CREATE FUNCTION {func_name}()
RETURNS STRING
LANGUAGE PYTHON
AS $$
    import time

    time.sleep(100)
    return "10"
$$
"""
        client.create_function(sql_function_body=sql_body)
        result = client.execute_function(func_name)
        assert result.error.startswith("Statement execution is still running after 5 seconds")

        monkeypatch.setenv(UCAI_DATABRICKS_WAREHOUSE_RETRY_TIMEOUT.name, "100")
        result = client.execute_function(func_name)
        assert result.value == "10"


@requires_databricks
def test_get_function(client: DatabricksFunctionClient):
    with generate_func_name_and_cleanup(client, schema=SCHEMA) as func_name:
        sql_body = f"""CREATE FUNCTION {func_name}(s STRING)
RETURNS STRING
LANGUAGE PYTHON
AS $$
    return s
    $$
"""
        create_func_info = client.create_function(sql_function_body=sql_body)
        function_info = client.get_function(func_name)
        assert create_func_info == function_info


def test_get_function_errors(client: DatabricksFunctionClient):
    with pytest.raises(ValueError, match=r"Invalid function name"):
        client.get_function("test")

    with pytest.raises(ValueError, match=r"function name cannot include *"):
        client.get_function("catalog.schema.*")

    with pytest.raises(ValueError, match=r"function name cannot include *"):
        client.get_function("catalog.schema.some_func*")


def simple_function(func_name: str) -> str:
    return f"""CREATE FUNCTION {func_name}(s STRING)
RETURNS STRING
LANGUAGE PYTHON
AS $$
    return s
    $$
"""


@requires_databricks
def test_list_functions(client: DatabricksFunctionClient):
    function_infos = client.list_functions(catalog=CATALOG, schema=SCHEMA)

    with generate_func_name_and_cleanup(client, schema=SCHEMA) as func_name:
        create_func_info = client.create_function(sql_function_body=simple_function(func_name))
        function_info = client.get_function(func_name)
        assert create_func_info == function_info

        function_infos = client.list_functions(catalog=CATALOG, schema=SCHEMA)
        assert len([f for f in function_infos if f.full_name == func_name]) == 1

        with generate_func_name_and_cleanup(client, schema=SCHEMA) as func_name_2:
            client.create_function(sql_function_body=simple_function(func_name_2))
            function_infos = client.list_functions(catalog=CATALOG, schema=SCHEMA, max_results=1)
            assert len(function_infos) == 1
            function_info = function_infos[0]
            function_infos = client.list_functions(
                catalog=CATALOG, schema=SCHEMA, max_results=1, page_token=function_infos.token
            )
            assert len(function_infos) == 1
            assert function_infos[0] != function_info


@requires_databricks
def test_delete_function(serverless_client: DatabricksFunctionClient):
    function_name = random_func_name(schema=SCHEMA)
    with pytest.raises(ResourceDoesNotExist, match=rf"'{function_name}' does not exist"):
        serverless_client.delete_function(function_name)

    serverless_client.create_function(sql_function_body=simple_function(function_name))
    serverless_client.get_function(function_name)
    serverless_client.delete_function(function_name)
    with pytest.raises(ResourceDoesNotExist, match=rf"'{function_name}' does not exist"):
        serverless_client.get_function(function_name)


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


@requires_databricks
def test_extra_params_when_executing_function_e2e(client: DatabricksFunctionClient, monkeypatch):
    monkeypatch.setenv(UCAI_DATABRICKS_WAREHOUSE_RETRY_TIMEOUT.name, "5")
    with generate_func_name_and_cleanup(client, schema=SCHEMA) as func_name:
        sql_body = f"""CREATE FUNCTION {func_name}()
RETURNS STRING
LANGUAGE PYTHON
AS $$
    import time

    time.sleep(100)
    return "10"
$$
"""
        client.create_function(sql_function_body=sql_body)
        time1 = time.time()
        # default wait_timeout is 30s
        client.execute_function(func_name)
        time_total1 = time.time() - time1

        monkeypatch.setenv(UCAI_DATABRICKS_WAREHOUSE_EXECUTE_FUNCTION_WAIT_TIMEOUT.name, "10s")
        time2 = time.time()
        client.execute_function(func_name)
        time_total2 = time.time() - time2
        # 30s - 10s = 20s, the time difference should be around 20s
        assert abs(abs(time_total2 - time_total1) - 20) < 5


@requires_databricks
def test_create_and_execute_python_function(client: DatabricksFunctionClient):
    def simple_func(x: int) -> str:
        """Test function that returns the string version of x."""
        return str(x)

    with create_python_function_and_cleanup(client, func=simple_func, schema=SCHEMA) as func_obj:
        result = client.execute_function(func_obj.full_function_name, {"x": 10})
        assert result.value == "10"


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


@requires_databricks
def test_create_python_function_with_complex_body(client: DatabricksFunctionClient):
    def complex_func(a: int, b: int) -> int:
        """A complex function that uses a try-except block and returns the sum."""
        try:
            return a + b
        except Exception as e:
            raise ValueError(f"Failed to add numbers") from e

    with create_python_function_and_cleanup(client, func=complex_func, schema=SCHEMA) as func_obj:
        result = client.execute_function(func_obj.full_function_name, {"a": 1, "b": 2})
        assert result.value == "3"


@requires_databricks
def test_create_python_function_with_docstring_comments(client: DatabricksFunctionClient):
    def documented_func(a: int, b: int) -> int:
        """
        Adds two integers.

        Args:
            a: The first integer.
            b: The second integer.

        Returns:
            int: The sum of a and b.
        """
        return a + b

    with create_python_function_and_cleanup(
        client, func=documented_func, schema=SCHEMA
    ) as func_obj:
        result = client.execute_function(func_obj.full_function_name, {"a": 5, "b": 3})
        assert result.value == "8"


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


@requires_databricks
def test_function_with_list_of_int_return(client: DatabricksFunctionClient):
    def func_returning_list(a: int) -> List[int]:
        """
        A function that returns a list of integers.

        Args:
            a: An integer to generate the list.

        Returns:
            List[int]: A list of integers from 0 to a.
        """
        return list(range(a))

    with create_python_function_and_cleanup(
        client, func=func_returning_list, schema=SCHEMA
    ) as func_obj:
        result = client.execute_function(func_obj.full_function_name, {"a": 3})
        # result wrapped as string is due to sql statement execution response parsing
        assert result.value == '["0","1","2"]'


@requires_databricks
def test_function_with_dict_of_string_to_int_return(client: DatabricksFunctionClient):
    def func_returning_map(a: int) -> Dict[str, int]:
        """
        A function that returns a map from string to integer.

        Args:
            a: The integer to use in generating the map.

        Returns:
            Dict[str, int]: A map of string keys to integer values.
        """
        return {f"key_{i}": i for i in range(a)}

    with create_python_function_and_cleanup(
        client, func=func_returning_map, schema=SCHEMA
    ) as func_obj:
        result = client.execute_function(func_obj.full_function_name, {"a": 3})
        # result wrapped as string is due to sql statement execution response parsing
        assert result.value == '{"key_0":"0","key_1":"1","key_2":"2"}'


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


@requires_databricks
def test_replace_existing_function(client: DatabricksFunctionClient):
    def simple_func(x: int) -> str:
        """Test function that returns the string version of x."""
        return str(x)

    # Create the function for the first time
    with create_python_function_and_cleanup(client, func=simple_func, schema=SCHEMA) as func_obj:
        result = client.execute_function(func_obj.full_function_name, {"x": 42})
        assert result.value == "42"

        # Modify the function definition
        def simple_func(x: int) -> str:
            """Modified function that returns 'Modified: ' plus the string version of x."""
            return f"Modified: {x}"

        # Replace the existing function
        client.create_python_function(
            func=simple_func, catalog=CATALOG, schema=SCHEMA, replace=True
        )

        # Execute the function again to verify it has been replaced
        result = client.execute_function(func_obj.full_function_name, {"x": 42})
        assert result.value == "Modified: 42"


@requires_databricks
def test_create_function_without_replace(client: DatabricksFunctionClient):
    def simple_func(x: int) -> str:
        """Test function that returns the string version of x."""
        return str(x)

    # Create the function for the first time
    with create_python_function_and_cleanup(client, func=simple_func, schema=SCHEMA):
        # Attempt to create the same function again without replace
        with pytest.raises(
            Exception,
            match=f"Cannot create the function `{CATALOG}`.`{SCHEMA}`.`simple_func` because it already exists",
        ):
            client.create_python_function(
                func=simple_func, catalog=CATALOG, schema=SCHEMA, replace=False
            )
