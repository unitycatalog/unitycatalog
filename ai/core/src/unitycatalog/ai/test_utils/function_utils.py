import logging
import uuid
from contextlib import contextmanager
from typing import Any, Callable, Generator, NamedTuple, Optional

from databricks.sdk.service.catalog import (
    ColumnTypeName,
    FunctionParameterInfo,
    FunctionParameterInfos,
)

from unitycatalog.ai.core.databricks import DatabricksFunctionClient
from unitycatalog.ai.core.utils.function_processing_utils import get_tool_name

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
