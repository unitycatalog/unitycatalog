import logging
from contextlib import contextmanager
from typing import Any, Callable, Generator, NamedTuple

from unitycatalog.ai.core.client import UnitycatalogFunctionClient
from unitycatalog.ai.core.utils.function_processing_utils import get_tool_name

CATALOG = "integration_testing"

_logger = logging.getLogger(__name__)


class FunctionObj(NamedTuple):
    full_function_name: str
    comment: str
    tool_name: str


@contextmanager
def create_function_and_cleanup_oss(
    client: UnitycatalogFunctionClient,
    *,
    schema: str,
    callable: Callable[..., Any] = None,
) -> Generator[FunctionObj, None, None]:
    def execute_code(code: str) -> str:
        """
        Executes Python code and returns its stdout.

        Args:
            code: Python code to execute. Remember to print the final result to stdout.

        Returns:
            The stdout of the executed code.
        """
        import sys
        from io import StringIO

        stdout = StringIO()
        sys.stdout = stdout
        exec(code)
        return stdout.getvalue()

    func = callable or execute_code

    try:
        func_info = client.create_python_function(
            func=func, catalog=CATALOG, schema=schema, replace=True
        )
        yield FunctionObj(
            full_function_name=func_info.full_name,
            comment=func_info.comment,
            tool_name=get_tool_name(func_info.full_name),
        )
    finally:
        try:
            client.delete_function(func_info.full_name)
        except Exception as e:
            _logger.warning(f"Failed to delete function: {e}")


@contextmanager
def create_retriever_function_and_cleanup_oss(
    client: UnitycatalogFunctionClient,
    *,
    schema: str,
) -> Generator[FunctionObj, None, None]:
    function_name = f"{CATALOG}.{schema}.retriever_function"
    routine_definition = f"""
    CREATE FUNCTION {function_name}()
    RETURNS TABLE (page_content STRING, id STRING, metadata MAP<STRING, STRING>)
    RETURN (
        SELECT *
        FROM VALUES
            ('Sample content 1', '1', map('key', 'value1')),
            ('Sample content 2', '2', map('key', 'value2')),
            ('Sample content 3', '3', map('key', 'value3'))
        AS test_data(page_content, id, metadata)
    );
    """
    try:
        func_info = client.create_function(
            function_name=function_name,
            routine_definition=routine_definition,
            data_type="TABLE_TYPE",
            full_data_type="(page_content STRING, id STRING, metadata MAP<STRING, STRING>)",
            comment="",
            parameters=[],
        )
        yield FunctionObj(
            full_function_name=func_info.full_name,
            comment=func_info.comment,
            tool_name=get_tool_name(func_info.full_name),
        )
    finally:
        try:
            client.delete_function(func_info.full_name)
        except Exception as e:
            _logger.warning(f"Failed to delete function: {e}")
