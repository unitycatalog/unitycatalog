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

RETRIEVER_STRUCT_FULL_DATA_TYPE = (
    "ARRAY<STRUCT<page_content: STRING, id: STRING, metadata: MAP<STRING, STRING>>>"
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
        or f"""CREATE OR REPLACE FUNCTION {func_name}(code STRING COMMENT 'Python code to execute. Remember to print the final result to stdout.')
RETURNS STRING
LANGUAGE PYTHON
COMMENT '{comment}'
AS $$
    import sys
    from io import StringIO
    stdout = StringIO()
    sys.stdout = stdout
    exec(code)
    return stdout.getvalue()
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
