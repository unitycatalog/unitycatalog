import asyncio
import datetime
import decimal
import functools
import json
import logging
import uuid
from contextlib import contextmanager
from typing import Any, AsyncIterator, Callable, Generator, NamedTuple, Optional

from databricks.sdk.service.catalog import (
    ColumnTypeName,
    FunctionInfo,
    FunctionParameterInfo,
    FunctionParameterInfos,
)

from unitycatalog.ai.core.databricks import DatabricksFunctionClient
from unitycatalog.ai.core.utils.function_processing_utils import get_tool_name
from unitycatalog.ai.test_utils.function_utils_oss import param_type_name

CATALOG = "integration_testing"

RETRIEVER_OUTPUT_SCALAR = '[{"page_content": "# Technology partners\\n## What is Databricks Partner Connect?\\n", "metadata": {"similarity_score": 0.010178182, "chunk_id": "0217a07ba2fec61865ce408043acf1cf"}}, {"page_content": "# Technology partners\\n## What is Databricks?\\n", "metadata": {"similarity_score": 0.010178183, "chunk_id": "0217a07ba2fec61865ce408043acf1cd"}}]'
RETRIEVER_OUTPUT_CSV = "page_content,metadata\n\"# Technology partners\n## What is Databricks Partner Connect?\n\",\"{'similarity_score': 0.010178182, 'chunk_id': '0217a07ba2fec61865ce408043acf1cf'}\"\n\"# Technology partners\n## What is Databricks?\n\",\"{'similarity_score': 0.010178183, 'chunk_id': '0217a07ba2fec61865ce408043acf1cd'}\"\n"

RETRIEVER_TABLE_FULL_DATA_TYPE = "(page_content STRING, metadata MAP<STRING, STRING>)"
RETRIEVER_TABLE_RETURN_PARAMS = FunctionParameterInfos(
    parameters=[
        FunctionParameterInfo(
            name="page_content",
            type_text="string",
            type_name=ColumnTypeName.STRING,
            type_json='{"name":"page_content","type":"string","nullable":true,"metadata":{}}',
            position=0,
        ),
        FunctionParameterInfo(
            name="metadata",
            type_text="map<string,string>",
            type_name=ColumnTypeName.MAP,
            type_json='{"name":"metadata","type":{"type":"map","keyType":"string","valueType":"string","valueContainsNull":true},"nullable":true,"metadata":{}}',
            position=1,
        ),
    ]
)

ADDRESS_TYPE_JSON = {
    "type": "struct",
    "fields": [
        {"name": "city", "type": "string", "nullable": False, "metadata": {}},
        {"name": "zip", "type": "integer", "nullable": True, "metadata": {}},
    ],
}

JSON_ARGUMENT_PARAMS = {
    "address": ("struct<city:string,zip:int>", ADDRESS_TYPE_JSON),
    "stops": (
        "array<struct<city:string,zip:int>>",
        {"type": "array", "elementType": ADDRESS_TYPE_JSON, "containsNull": False},
    ),
    "by_name": (
        "map<string,struct<city:string,zip:int>>",
        {
            "type": "map",
            "keyType": "string",
            "valueType": ADDRESS_TYPE_JSON,
            "valueContainsNull": False,
        },
    ),
    "data": ("binary", "binary"),
    "day": ("date", "date"),
    "at": ("timestamp", "timestamp"),
    "amount": ("decimal(10,2)", "decimal(10,2)"),
}

JSON_ARGUMENTS = {
    "address": {"city": "Oslo", "zip": 150},
    "stops": [{"city": "Bergen", "zip": None}, {"city": "Tromso", "zip": 9008}],
    "by_name": {"home": {"city": "Oslo", "zip": 150}},
    "day": "2024-01-02",
    "at": "2024-01-02T03:04:05Z",
    "amount": 12.5,
}

# Python values that a caller can pass in place of the JSON values in JSON_ARGUMENTS.
NATIVE_ARGUMENTS = {
    "day": datetime.date(2024, 1, 2),
    "at": datetime.datetime(2024, 1, 2, 3, 4, 5, tzinfo=datetime.timezone.utc),
    "amount": decimal.Decimal("12.5"),
}

# Base64 text with padding, with "+" and "/", of bytes that are not UTF-8, and empty.
BINARY_ARGUMENTS = ["aGk=", "YWJjZA==", "+/8=", "/wA=", "AAH/", ""]


def generate_function_info_with_params(params: dict[str, tuple[str, Any]]) -> FunctionInfo:
    """
    Returns the FunctionInfo of the function `catalog.schema.test` with the given parameters.

    Args:
        params: The parameters by name, as (type_text, type) where type is the "type" entry
            of the parameter's type json.
    """
    return FunctionInfo(
        catalog_name="catalog",
        schema_name="schema",
        name="test",
        full_name="catalog.schema.test",
        comment="A test function.",
        input_params=FunctionParameterInfos(
            parameters=[
                FunctionParameterInfo(
                    name=name,
                    type_text=type_text,
                    type_name=ColumnTypeName(param_type_name(type_text)),
                    type_json=json.dumps(
                        {"name": name, "type": type_json, "nullable": False, "metadata": {}}
                    ),
                    position=position,
                )
                for position, (name, (type_text, type_json)) in enumerate(params.items())
            ]
        ),
    )


_logger = logging.getLogger(__name__)


def random_func_name(schema: str):
    """
    Generate a random function name in the format of `<catalog>.<schema>.<function_name>`.

    Args:
        schema: The schema name to use in the function name.
    """
    return f"{CATALOG}.{schema}.test_{uuid.uuid4().hex[:4]}"


@contextmanager
def generate_func_name_and_cleanup(client: DatabricksFunctionClient, schema: str):
    func_name = random_func_name(schema)
    try:
        yield func_name
    finally:
        try:
            client.delete_function(func_name)
        except Exception as e:
            _logger.warning(f"Failed to delete function: {e}")


class FunctionObj(NamedTuple):
    full_function_name: str
    comment: str
    tool_name: str


@contextmanager
def create_function_and_cleanup(
    client: DatabricksFunctionClient,
    *,
    schema: str,
    func_name: Optional[str] = None,
    sql_body: Optional[str] = None,
) -> Generator[FunctionObj, None, None]:
    func_name = func_name or random_func_name(schema=schema)
    comment = "Executes Python code and returns its stdout."
    sql_body = (
        sql_body
        or f"""CREATE OR REPLACE FUNCTION {func_name}(number INTEGER COMMENT 'Add a given number to 10.')
RETURNS STRING
LANGUAGE PYTHON
COMMENT '{comment}'
AS $$
    return str(number + 10)
$$
"""
    )
    try:
        client.create_function(sql_function_body=sql_body)
        yield FunctionObj(
            full_function_name=func_name, comment=comment, tool_name=get_tool_name(func_name)
        )
    finally:
        try:
            client.delete_function(func_name)
        except Exception as e:
            _logger.warning(f"Failed to delete function: {e}")


@contextmanager
def create_table_function_and_cleanup(
    client: DatabricksFunctionClient,
    *,
    schema: str,
    func_name: Optional[str] = None,
    sql_body: Optional[str] = None,
) -> Generator[FunctionObj, None, None]:
    func_name = func_name or random_func_name(schema=schema)
    comment = "Executes Python code and returns its stdout."
    sql_body = (
        sql_body
        or f"""CREATE FUNCTION {func_name}(query STRING)
RETURNS TABLE (
    page_content STRING, 
    metadata MAP<STRING, STRING>
)
RETURN SELECT
      chunked_text AS page_content,
      map('doc_uri', url, 'chunk_id', chunk_id) AS metadata
    FROM (
        SELECT
            'testing' AS chunked_text,
            'https://docs.databricks.com/' AS url,
            '1' AS chunk_id
    ) AS subquery_alias;
"""
    )
    try:
        client.create_function(sql_function_body=sql_body)
        yield FunctionObj(
            full_function_name=func_name, comment=comment, tool_name=get_tool_name(func_name)
        )
    finally:
        try:
            client.delete_function(func_name)
        except Exception as e:
            _logger.warning(f"Failed to delete function: {e}")


@contextmanager
def create_python_function_and_cleanup(
    client: DatabricksFunctionClient,
    *,
    schema: str,
    func: Callable[..., Any] = None,
) -> Generator[FunctionObj, None, None]:
    func_name = f"{CATALOG}.{schema}.{func.__name__}"
    try:
        func_info = client.create_python_function(
            func=func, catalog=CATALOG, schema=schema, replace=True
        )
        yield FunctionObj(
            full_function_name=func_name,
            comment=func_info.comment,
            tool_name=get_tool_name(func_name),
        )
    finally:
        try:
            client.delete_function(func_name)
        except Exception as e:
            _logger.warning(f"Failed to delete function: {e}")


@contextmanager
def create_wrapped_function_and_cleanup(
    client: DatabricksFunctionClient,
    *,
    schema: str,
    primary_func: Callable[..., Any],
    functions: list[Callable[..., Any]],
) -> Generator[FunctionObj, None, None]:
    func_name = f"{CATALOG}.{schema}.{primary_func.__name__}"
    try:
        func_info = client.create_wrapped_function(
            primary_func=primary_func,
            functions=functions,
            catalog=CATALOG,
            schema=schema,
            replace=True,
        )
        yield FunctionObj(
            full_function_name=func_name,
            comment=func_info.comment,
            tool_name=get_tool_name(func_name),
        )
    finally:
        try:
            client.delete_function(func_name)
        except Exception as e:
            _logger.warning(f"Failed to delete function {func_name}: {e}")


def int_func_no_doc(a):
    return a + 10


def int_func_with_doc(a: int) -> int:
    """
    A function that takes an integer and returns an integer.

    Args:
        a: An integer.

    Returns:
        An integer.
    """
    return a + 10


def str_func_no_doc(b):
    return f"str value: {b}"


def str_func_with_doc(b: str) -> str:
    """
    A function that takes a string and returns a string.

    Args:
        b: A string.

    Returns:
        A string.
    """
    return f"str value: {b}"


def wrap_func_no_doc(a: int, b: str) -> str:
    """
    A function that takes two arguments and returns a string.

    Args:
        a: An integer.
        b: A string.

    Returns:
        A string.
    """
    func1 = int_func_no_doc(a)
    func2 = str_func_no_doc(b)

    return f"{func1} {func2}"


def wrap_func_with_doc(a: int, b: str) -> str:
    """
    A function that takes two arguments and returns a string.

    Args:
        a: An integer.
        b: A string.

    Returns:
        A string.
    """
    func1 = int_func_with_doc(a)
    func2 = str_func_with_doc(b)

    return f"{func1} {func2}"


async def async_int_func(a: int) -> int:
    """
    An async function that takes an integer and returns an integer.

    Args:
        a: An integer.

    Returns:
        An integer.
    """
    return a + 10


async def async_int_gen(a: int) -> AsyncIterator[int]:
    """
    An async generator function that yields the integers from 0 up to an integer.

    Args:
        a: An integer.

    Yields:
        An integer.
    """
    for i in range(a):
        yield i


def run_sync(async_func):
    @functools.wraps(async_func)
    def wrapper(*args, **kwargs):
        return asyncio.run(async_func(*args, **kwargs))

    return wrapper


async_int_func_partial = functools.partial(async_int_func, 1)

ASYNC_FUNCTION_ERROR = (
    "Function '{}' is an async function. Async functions are not supported in Unity Catalog. "
    "Define it with a regular 'def' instead of 'async def'."
)
ASYNC_WRAPPER_ERROR = (
    "Function '{}' wraps an async function. Async functions are not supported in Unity Catalog. "
    "Define the wrapped function with a regular 'def' instead of 'async def'."
)
