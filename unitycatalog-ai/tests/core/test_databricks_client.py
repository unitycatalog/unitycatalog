import base64
import datetime
import os
import time
from decimal import Decimal
from typing import Any, Callable, Dict, List, NamedTuple

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


@pytest.mark.parametrize(
    ("sql_body", "function_name"),
    [
        (
            "CREATE OR REPLACE FUNCTION test(s STRING) RETURNS STRING LANGUAGE PYTHON AS $$ return s $$",
            "test",
        ),
        (
            "CREATE OR REPLACE FUNCTION a.b.test(s STRING) RETURNS STRING LANGUAGE PYTHON AS $$ return s $$",
            "a.b.test",
        ),
        (
            "CREATE FUNCTION a.test(s STRING) RETURNS STRING LANGUAGE PYTHON AS $$ return s $$",
            "a.test",
        ),
        (
            "CREATE TEMPORARY FUNCTION a.b.test(s STRING) RETURNS STRING LANGUAGE PYTHON AS $$ return s $$",
            "a.b.test",
        ),
        (
            "CREATE TEMPORARY FUNCTION IF NOT EXISTS test(s STRING) RETURNS STRING LANGUAGE PYTHON AS $$ return s $$",
            "test",
        ),
        ("CREATE FUNCTION IF NOT EXISTS a.b.test() RETURN 123", "a.b.test"),
    ],
)
def test_extract_function_name(sql_body, function_name):
    assert extract_function_name(sql_body) == function_name


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
