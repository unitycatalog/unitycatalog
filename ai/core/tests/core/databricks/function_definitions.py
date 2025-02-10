import base64
import datetime
from decimal import Decimal
from typing import Any, Callable, Dict, List, NamedTuple


class FunctionInputOutput(NamedTuple):
    sql_body: str
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


def function_with_scalar_retriever_output(func_name: str) -> FunctionInputOutput:
    retriever_output = str(
        [
            {
                "page_content": "# Technology partners\n## What is Databricks Partner Connect?\n",
                "metadata": {
                    "similarity_score": 0.010178182,
                    "chunk_id": "0217a07ba2fec61865ce408043acf1cf",
                    "url": "https://docs.databricks.com/partner-connect/walkthrough-fivetran.html",
                },
            }
        ]
    )
    sql_body = f"""CREATE FUNCTION {func_name}(query STRING)
RETURNS STRING
LANGUAGE PYTHON
AS $$
    return {retriever_output}
    $$
"""
    return FunctionInputOutput(
        sql_body=sql_body,
        inputs=[{"query": "What is Databricks Partner Connect?"}],
        output=retriever_output,
    )


class PythonFunctionInputOutput(NamedTuple):
    func: Callable
    inputs: List[Dict[str, Any]]
    output: str


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


def simple_sql_function_boy(func_name: str) -> str:
    return f"""CREATE FUNCTION {func_name}(s STRING)
RETURNS STRING
LANGUAGE PYTHON
AS $$
    return s
    $$
"""
