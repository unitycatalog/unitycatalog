import logging
from contextlib import contextmanager
from typing import Any, Callable, Generator, NamedTuple

from unitycatalog.ai.core.client import UnitycatalogFunctionClient
from unitycatalog.ai.core.utils.function_processing_utils import get_tool_name
from unitycatalog.client import (
    ColumnTypeName,
    FunctionParameterInfo,
    FunctionParameterInfos,
)

CATALOG = "integration_testing"

RETRIEVER_TABLE_RETURN_PARAMS_OSS = FunctionParameterInfos(
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
    def add_numbers(a: int, b: int) -> str:
        """
        Adds two numbers together.

        Args:
            a: First number.
            b: Second number.

        Returns:
            The sum of the provided numbers.
        """
        return str(a + b)

    func = callable or add_numbers

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
