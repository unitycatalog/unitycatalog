import asyncio
import datetime
import decimal
import gc
import importlib.util
import json
import logging
import multiprocessing
import os
import re
import signal
import socket
import subprocess
import sys
import textwrap
import threading
import time
import warnings
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing, contextmanager, suppress
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import SimpleNamespace
from typing import Any, Callable, Dict, List, Optional, Union
from unittest import mock

import pytest
import pytest_asyncio
from pydantic import ValidationError

from unitycatalog.ai.core.client import (
    UnitycatalogClient,
    UnitycatalogFunctionClient,
    _SingleLoopRESTClient,
    validate_input_parameter,
    validate_param,
)
from unitycatalog.ai.core.executor.common import SANDBOX_UNSUPPORTED_MESSAGE
from unitycatalog.ai.core.executor.local import run_in_sandbox, run_in_sandbox_async
from unitycatalog.ai.core.types import Variant
from unitycatalog.ai.core.utils.execution_utils import load_function_from_string
from unitycatalog.ai.test_utils.function_utils import (
    CATALOG,
    async_int_func,
    int_func_no_doc,
    int_func_with_doc,
    random_func_name,
    str_func_no_doc,
    str_func_with_doc,
    wrap_func_no_doc,
    wrap_func_with_doc,
)
from unitycatalog.client import (
    ApiClient,
    CatalogsApi,
    Configuration,
    FunctionInfo,
    FunctionParameterInfo,
    FunctionParameterInfos,
    FunctionsApi,
    SchemasApi,
)
from unitycatalog.client.exceptions import NotFoundException
from unitycatalog.client.rest import RESTClientObject

SCHEMA = "uc_test"
_logger = logging.getLogger(__name__)


def try_delete_function(client: UnitycatalogFunctionClient, function_name: str):
    try:
        client.delete_function(function_name)
    except Exception as e:
        _logger.warning(f"Fail to delete function: {e}")


@contextmanager
def generate_func_name_and_cleanup(client: UnitycatalogFunctionClient):
    func_name = random_func_name(schema=SCHEMA)
    try:
        yield func_name
    finally:
        try_delete_function(client, func_name)


@dataclass
class FunctionObj:
    input_params: List[FunctionParameterInfo]
    data_type: str
    full_data_type: str
    routine_definition: str
    input_data: Dict[str, Any]
    expected_result: str
    comment: str


def simple_function_obj():
    return FunctionObj(
        input_params=[
            FunctionParameterInfo(
                name="x",
                type_name="STRING",
                type_text="string",
                type_json='{"name":"x","type":"string","nullable":false,"metadata":{}}',
                position=0,
            )
        ],
        data_type="STRING",
        full_data_type="STRING",
        routine_definition="return x",
        input_data={"x": "test"},
        expected_result="test",
        comment="test",
    )


def simple_func(a: int, b: int) -> int:
    """
    A simple local function.

    Args:
        a: an int
        b: an int

    Returns:
        int: The sum of a and b.
    """
    return a + b


@pytest_asyncio.fixture
async def uc_client():
    config = Configuration()
    config.host = "http://localhost:8080/api/2.1/unity-catalog"
    uc_api_client = ApiClient(configuration=config)

    uc_client = UnitycatalogFunctionClient(api_client=uc_api_client)
    uc_client.uc.create_catalog(name=CATALOG)
    uc_client.uc.create_schema(name=SCHEMA, catalog_name=CATALOG)

    yield uc_client

    await uc_client.close_async()
    await uc_api_client.close()


@pytest_asyncio.fixture
async def core_client():
    core_catalog_name = "CoreCatalog"
    core_schema_name = "CoreSchema"
    config = Configuration()
    config.host = "http://localhost:8080/api/2.1/unity-catalog"
    uc_api_client = ApiClient(configuration=config)

    core_client = UnitycatalogClient(api_client=uc_api_client)
    core_client.create_catalog(
        name=core_catalog_name, comment="Testing the catalog creation", properties={"test": "test"}
    )
    core_client.create_schema(
        name=core_schema_name,
        catalog_name=core_catalog_name,
        comment="Testing the schema creation",
        properties={"test": "test"},
    )

    yield core_client

    await core_client.catalogs_client.delete_catalog(name=core_catalog_name, force=True)
    await core_client.close_async()
    await uc_api_client.close()


@pytest.mark.asyncio
async def test_handle_invalid_client():
    with pytest.raises(ValueError, match="The 'api_client' must be an instance of"):
        UnitycatalogFunctionClient(api_client="client")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("execution_mode", "start_methods", "should_warn"),
    [
        (None, ["spawn"], True),
        ("sandbox", ["spawn"], True),
        ("sandbox", ["fork", "spawn"], False),
        ("local", ["spawn"], False),
    ],
)
async def test_init_warns_when_sandbox_is_unsupported(
    monkeypatch, caplog, execution_mode, start_methods, should_warn
):
    monkeypatch.setattr(multiprocessing, "get_all_start_methods", lambda: start_methods)
    kwargs = {} if execution_mode is None else {"execution_mode": execution_mode}
    with caplog.at_level(logging.WARNING):
        client = UnitycatalogFunctionClient(
            api_client=ApiClient(configuration=Configuration()), **kwargs
        )
    await client.close_async()

    warnings = [
        r.getMessage()
        for r in caplog.records
        if r.name == "unitycatalog.ai.core.client" and r.levelno == logging.WARNING
    ]
    assert warnings == ([SANDBOX_UNSUPPORTED_MESSAGE] if should_warn else [])


@pytest.mark.asyncio
async def test_create_unitycatalog_client(core_client):
    assert isinstance(core_client.catalogs_client, CatalogsApi)
    assert isinstance(core_client.schemas_client, SchemasApi)
    assert isinstance(core_client.functions_client, FunctionsApi)

    core_client.create_catalog(name="test_catalog", comment="test")
    core_client.create_schema(name="test_schema", catalog_name="test_catalog", comment="test")

    catalog_info = await core_client.catalogs_client.get_catalog(name="test_catalog")

    assert catalog_info.name == "test_catalog"
    assert catalog_info.comment == "test"

    schema_info = await core_client.schemas_client.get_schema(full_name="test_catalog.test_schema")

    assert schema_info.name == "test_schema"
    assert schema_info.catalog_name == "test_catalog"
    assert schema_info.comment == "test"


@pytest.mark.asyncio
async def test_catalog_idempotent_creation(core_client, caplog):
    test_catalog = "CoreCatalog"
    catalog_comment = "Testing the catalog creation"
    properties = {"test": "test"}

    with caplog.at_level(logging.INFO):
        catalog_info = core_client.create_catalog(
            name=test_catalog, comment=catalog_comment, properties=properties
        )

    assert catalog_info.name == test_catalog
    assert catalog_info.comment == catalog_comment
    assert catalog_info.properties == properties

    expected_message = f"The catalog '{test_catalog}' already exists."
    assert expected_message in caplog.text


@pytest.mark.asyncio
async def test_schema_idempotent_creation(core_client, caplog):
    test_catalog = "CoreCatalog"
    test_schema = "CoreSchema"
    schema_comment = "Testing the schema creation"
    properties = {"test": "test"}

    core_client.create_catalog(name=test_catalog, comment="Catalog for schema", properties={})

    core_client.create_schema(
        name=test_schema, catalog_name=test_catalog, comment=schema_comment, properties=properties
    )

    with caplog.at_level(logging.INFO):
        schema_info = core_client.create_schema(
            name=test_schema,
            catalog_name=test_catalog,
            comment=schema_comment,
            properties=properties,
        )

    assert schema_info.name == test_schema
    assert schema_info.catalog_name == test_catalog
    assert schema_info.comment == schema_comment
    assert schema_info.properties == properties

    expected_message = f"The schema '{test_schema}' already exists in the catalog '{test_catalog}'"
    assert expected_message in caplog.text


@pytest.mark.asyncio
async def test_create_catalog(uc_client, caplog):
    test_catalog = "CatalogCreate"
    catalog_comment = "Testing the catalog creation"
    properties = {"test": "test"}
    uc_client.uc.create_catalog(name=test_catalog, comment=catalog_comment, properties=properties)

    with caplog.at_level(logging.INFO):
        catalog_info = uc_client.uc.create_catalog(
            name=test_catalog, comment=catalog_comment, properties=properties
        )

    assert catalog_info.name == test_catalog
    assert catalog_info.comment == catalog_comment
    assert catalog_info.properties == properties

    expected_message = f"The catalog '{test_catalog}' already exists."
    assert expected_message in caplog.text

    await uc_client.uc.catalogs_client.delete_catalog(name=test_catalog, force=True)


@pytest.mark.asyncio
async def test_create_schema(uc_client, caplog):
    test_catalog = "SchemaCreate"
    test_schema = "TestSchema"
    schema_comment = "Testing the schema creation"
    properties = {"test": "test"}

    uc_client.uc.create_catalog(name=test_catalog)

    with caplog.at_level(logging.INFO):
        schema_info = uc_client.uc.create_schema(
            name=test_schema,
            catalog_name=test_catalog,
            comment=schema_comment,
            properties=properties,
        )

    assert schema_info.name == test_schema
    assert schema_info.catalog_name == test_catalog
    assert schema_info.comment == schema_comment
    assert schema_info.properties == properties

    with caplog.at_level(logging.INFO):
        existing = uc_client.uc.create_schema(name=test_schema, catalog_name=test_catalog)

    expected_message = f"The schema '{test_schema}' already exists in the catalog '{test_catalog}'"
    assert expected_message in caplog.text

    assert existing.name == test_schema
    assert existing.comment == schema_comment

    non_existent_catalog = "NotARealCatalog"
    with pytest.raises(
        ValueError,
        match=f"The Catalog that you specified: '{non_existent_catalog}' does not exist on this server.",
    ):
        uc_client.uc.create_schema(name="MySchema", catalog_name=non_existent_catalog)

    await uc_client.uc.catalogs_client.delete_catalog(name=test_catalog, force=True)


@pytest.mark.asyncio
async def test_create_function(uc_client):
    function_name = f"{CATALOG}.{SCHEMA}.test_function"
    routine_definition = "return str(x)"
    data_type = "STRING"
    full_data_type = "STRING"
    comment = "test"
    parameters = [
        FunctionParameterInfo(
            name="x",
            type_text="string",
            type_json='{"name":"x","type":"string","nullable":false,"metadata":{}}',
            type_name="STRING",
            position=0,
        )
    ]

    func_info = uc_client.create_function(
        function_name=function_name,
        routine_definition=routine_definition,
        data_type=data_type,
        full_data_type=full_data_type,
        comment=comment,
        parameters=parameters,
        timeout=10,
        replace=True,
    )

    assert func_info.name == "test_function"
    assert func_info.full_name == function_name
    assert func_info.data_type == data_type
    assert func_info.full_data_type == full_data_type
    assert func_info.routine_definition == routine_definition
    assert func_info.comment == comment
    assert (
        uc_client.execute_function(function_name=function_name, parameters={"x": "test"}).value
        == "test"
    )
    assert uc_client.func_cache.get(function_name) is not None
    uc_client.clear_function_cache()
    assert uc_client.func_cache.get(function_name) is None


@pytest.mark.asyncio
async def test_parameter_metadata_correctness(uc_client: UnitycatalogFunctionClient):
    function_obj = FunctionObj(
        input_params=[
            FunctionParameterInfo(
                name="x",
                type_name="STRING",
                type_text="string",
                type_json='{"name":"x","type":"string","nullable":false,"metadata":{}}',
                position=0,
            ),
            FunctionParameterInfo(
                name="y",
                type_name="INT",
                type_text="int",
                type_json='{"name":"y","type":"int","nullable":false,"metadata":{}}',
                position=1,
            ),
        ],
        data_type="INT",
        full_data_type="INT",
        routine_definition="return x * y",
        input_data={"x": "hello", "y": 2},
        expected_result="hellohello",
        comment="Concatenates a string and an integer.",
    )

    with generate_func_name_and_cleanup(uc_client) as function_name:
        uc_client.create_function(
            function_name=function_name,
            routine_definition=function_obj.routine_definition,
            data_type=function_obj.data_type,
            full_data_type=function_obj.full_data_type,
            comment=function_obj.comment,
            parameters=function_obj.input_params,
        )

        retrieved_func = uc_client.get_function(function_name=function_name)

        for expected_param in function_obj.input_params:
            retrieved_param = next(
                (
                    p
                    for p in retrieved_func.input_params.parameters
                    if p.name == expected_param.name
                ),
                None,
            )
            assert retrieved_param is not None

            expected_type_json = json.loads(expected_param.type_json)
            retrieved_type_json = json.loads(retrieved_param.type_json)

            assert retrieved_type_json == expected_type_json, (
                f"type_json mismatch for parameter '{expected_param.name}'.\n"
                f"Expected: {expected_type_json}\n"
                f"Found: {retrieved_type_json}"
            )

        result = uc_client.execute_function(
            function_name=function_name, parameters=function_obj.input_data
        )
        assert result.value == function_obj.expected_result, "Function execution result mismatch."
        assert result.error is None, "Function execution should not have errors."


@pytest.mark.asyncio
async def test_list_functions(uc_client: UnitycatalogFunctionClient):
    function_infos = uc_client.list_functions(catalog=CATALOG, schema=SCHEMA)
    func_obj = simple_function_obj()
    with generate_func_name_and_cleanup(uc_client) as func_name:
        create_func_info = uc_client.create_function(
            function_name=func_name,
            routine_definition=func_obj.routine_definition,
            data_type=func_obj.data_type,
            full_data_type=func_obj.full_data_type,
            comment="test",
            parameters=func_obj.input_params,
            properties="null",
        )
        function_info = uc_client.get_function(func_name)
        if create_func_info.properties == "null":
            create_func_info.properties = None

        assert create_func_info == function_info

        function_infos = uc_client.list_functions(catalog=CATALOG, schema=SCHEMA)
        assert len([f for f in function_infos if f.full_name == func_name]) == 1

        with generate_func_name_and_cleanup(uc_client) as func_name_2:
            uc_client.create_function(
                function_name=func_name_2,
                routine_definition=func_obj.routine_definition,
                data_type=func_obj.data_type,
                full_data_type=func_obj.full_data_type,
                comment="test",
                parameters=func_obj.input_params,
                properties="null",
            )
            function_infos = uc_client.list_functions(catalog=CATALOG, schema=SCHEMA, max_results=1)
            assert len(function_infos) == 1
            function_info = function_infos[0]
            function_infos = uc_client.list_functions(
                catalog=CATALOG, schema=SCHEMA, max_results=1, page_token=function_infos.token
            )
            assert len(function_infos) == 1
            assert function_infos[0] != function_info


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "function_object",
    [
        simple_function_obj(),
        FunctionObj(
            input_params=[
                FunctionParameterInfo(
                    name="x",
                    type_name="INT",
                    type_text="int",
                    type_json='{"name":"x","type":"int","nullable":false,"metadata":{}}',
                    position=0,
                ),
                FunctionParameterInfo(
                    name="y",
                    type_name="INT",
                    type_text="int",
                    type_json='{"name":"y","type":"int","nullable":false,"metadata":{}}',
                    position=1,
                ),
            ],
            data_type="INT",
            full_data_type="INT",
            routine_definition="return x+y",
            input_data={"x": 1, "y": 2},
            expected_result=3,
            comment="test",
        ),
        FunctionObj(
            input_params=[
                FunctionParameterInfo(
                    name="x",
                    type_name="ARRAY",
                    type_text="array<float>",
                    type_json='{"name":"x","type":"array<float>","nullable":false,"metadata":{}}',
                    position=0,
                )
            ],
            data_type="DOUBLE",
            full_data_type="DOUBLE",
            routine_definition="return sum(x)",
            input_data={"x": (1, 2, 3)},
            expected_result=6,
            comment="test",
        ),
        FunctionObj(
            input_params=[
                FunctionParameterInfo(
                    name="x",
                    type_name="BOOLEAN",
                    type_text="boolean",
                    type_json='{"name":"x","type":"boolean","nullable":false,"metadata":{}}',
                    position=0,
                )
            ],
            data_type="BOOLEAN",
            full_data_type="BOOLEAN",
            routine_definition="return x",
            input_data={"x": True},
            expected_result=True,
            comment="test",
        ),
        FunctionObj(
            input_params=[
                FunctionParameterInfo(
                    name="x",
                    type_name="BINARY",
                    type_text="binary",
                    type_json='{"name":"x","type":"binary","nullable":false,"metadata":{}}',
                    position=0,
                )
            ],
            data_type="STRING",
            full_data_type="STRING",
            routine_definition="return x.decode('utf-8')",
            input_data={"x": b"Hello"},
            expected_result="Hello",
            comment="test",
        ),
        FunctionObj(
            input_params=[
                FunctionParameterInfo(
                    name="x",
                    type_name="BYTE",
                    type_text="byte",
                    type_json='{"name":"x","type":"byte","nullable":false,"metadata":{}}',
                    position=0,
                )
            ],
            data_type="BYTE",
            full_data_type="BYTE",
            routine_definition="return x",
            input_data={"x": 127},
            expected_result=127,
            comment="test",
        ),
        FunctionObj(
            input_params=[
                FunctionParameterInfo(
                    name="x",
                    type_name="SHORT",
                    type_text="short",
                    type_json='{"name":"x","type":"short","nullable":false,"metadata":{}}',
                    position=0,
                )
            ],
            data_type="SHORT",
            full_data_type="SHORT",
            routine_definition="return x",
            input_data={"x": 32767},
            expected_result=32767,
            comment="test",
        ),
        FunctionObj(
            input_params=[
                FunctionParameterInfo(
                    name="x",
                    type_name="LONG",
                    type_text="long",
                    type_json='{"name":"x","type":"long","nullable":false,"metadata":{}}',
                    position=0,
                )
            ],
            data_type="LONG",
            full_data_type="LONG",
            routine_definition="return x",
            input_data={"x": 2**63 - 1},
            expected_result=2**63 - 1,
            comment="test",
        ),
        FunctionObj(
            input_params=[
                FunctionParameterInfo(
                    name="x",
                    type_name="DATE",
                    type_text="date",
                    type_json='{"name":"x","type":"date","nullable":false,"metadata":{}}',
                    position=0,
                )
            ],
            data_type="STRING",
            full_data_type="STRING",
            routine_definition="return x.isoformat()",
            input_data={"x": datetime.date(2024, 10, 11)},
            expected_result="2024-10-11",
            comment="test",
        ),
        FunctionObj(
            input_params=[
                FunctionParameterInfo(
                    name="x",
                    type_name="TIMESTAMP",
                    type_text="timestamp",
                    type_json='{"name":"x","type":"timestamp","nullable":false,"metadata":{}}',
                    position=0,
                )
            ],
            data_type="STRING",
            full_data_type="STRING",
            routine_definition="return x.isoformat()",
            input_data={"x": datetime.datetime(2024, 10, 11, 11, 2, 3)},
            expected_result="2024-10-11T11:02:03",
            comment="test",
        ),
        FunctionObj(
            input_params=[
                FunctionParameterInfo(
                    name="x",
                    type_name="TIMESTAMP_NTZ",
                    type_text="timestamp_ntz",
                    type_json='{"name":"x","type":"timestamp_ntz","nullable":false,"metadata":{}}',
                    position=0,
                )
            ],
            data_type="STRING",
            full_data_type="STRING",
            routine_definition="return x.isoformat()",
            input_data={"x": datetime.datetime(2024, 10, 11, 11, 2, 3)},
            expected_result="2024-10-11T11:02:03",
            comment="test",
        ),
        FunctionObj(
            input_params=[
                FunctionParameterInfo(
                    name="x",
                    type_name="DECIMAL",
                    type_text="decimal",
                    type_json='{"name":"x","type":"decimal","nullable":false,"metadata":{}}',
                    position=0,
                )
            ],
            data_type="STRING",
            full_data_type="STRING",
            routine_definition="return str(x)",
            input_data={"x": decimal.Decimal(123)},
            expected_result="123",
            comment="test",
        ),
        FunctionObj(
            input_params=[
                FunctionParameterInfo(
                    name="x",
                    type_name="INTERVAL",
                    type_text="interval day to second",
                    type_json='{"name":"x","type":"interval day to second","nullable":false,"metadata":{}}',
                    position=0,
                )
            ],
            data_type="INTERVAL",
            full_data_type="INTERVAL",
            routine_definition="import datetime\nreturn (datetime.datetime(2024, 8, 19) - x).isoformat()",
            input_data={
                "x": datetime.timedelta(
                    days=0, hours=0, minutes=16, seconds=40, microseconds=123456
                )
            },
            expected_result="2024-08-18T23:43:19.876544",
            comment="test",
        ),
        FunctionObj(
            input_params=[
                FunctionParameterInfo(
                    name="x",
                    type_name="STRUCT",
                    type_text="struct<a:string,b:int>",
                    type_json='{"name":"x","type":"struct<a:string,b:int>","nullable":false,"metadata":{}}',
                    position=0,
                )
            ],
            data_type="STRING",
            full_data_type="STRING",
            routine_definition="return x['a']+str(x['b'])",
            input_data={"x": {"a": "value", "b": 123}},
            expected_result="value123",
            comment="test",
        ),
        FunctionObj(
            input_params=[
                FunctionParameterInfo(
                    name="x",
                    type_name="MAP",
                    type_text="map<string, int>",
                    type_json='{"name":"x","type":"map<string, int>","nullable":false,"metadata":{}}',
                    position=0,
                )
            ],
            data_type="INT",
            full_data_type="INT",
            routine_definition="return x['key1']",
            input_data={"x": {"key1": 1, "key2": 2}},
            expected_result=1,
            comment="test",
        ),
        FunctionObj(
            input_params=[
                FunctionParameterInfo(
                    name="x",
                    type_name="CHAR",
                    type_text="char",
                    type_json='{"name":"x","type":"char","nullable":false,"metadata":{}}',
                    position=0,
                )
            ],
            data_type="CHAR",
            full_data_type="CHAR",
            routine_definition="return x",
            input_data={"x": "A"},
            expected_result="A",
            comment="test",
        ),
    ],
)
async def test_function_creation_and_execution(
    uc_client: UnitycatalogFunctionClient, function_object: FunctionObj
):
    with generate_func_name_and_cleanup(uc_client) as function_name:
        uc_client.create_function(
            function_name=function_name,
            routine_definition=function_object.routine_definition,
            data_type=function_object.data_type,
            full_data_type=function_object.full_data_type,
            comment=function_object.comment,
            parameters=function_object.input_params,
        )
        result = uc_client.execute_function(
            function_name=function_name, parameters=function_object.input_data
        )
        assert result.value == function_object.expected_result


@pytest.mark.asyncio
async def test_create_function_invalid_data_type(uc_client):
    function_name = f"{CATALOG}.{SCHEMA}.invalid_function"
    routine_definition = "return x"
    data_type = "INVALID_TYPE"  # Invalid data type
    parameters = [
        FunctionParameterInfo(
            name="x",
            type_text="string",
            type_json='{"name":"x","type":"string","nullable":false,"metadata":{}}',
            type_name="STRING",
            position=0,
        )
    ]

    with pytest.raises(ValueError, match="Invalid data_type"):
        uc_client.create_function(
            function_name=function_name,
            routine_definition=routine_definition,
            data_type=data_type,
            full_data_type=data_type,
            parameters=parameters,
            timeout=10,
            replace=True,
            comment="test",
        )


@pytest.mark.asyncio
async def test_get_nonexistent_function(uc_client):
    function_name = f"{CATALOG}.{SCHEMA}.nonexistent_function"

    assert uc_client.get_function(function_name=function_name) is None


@pytest.mark.asyncio
async def test_delete_nonexistent_function(uc_client):
    function_name = f"{CATALOG}.{SCHEMA}.nonexistent_function"

    with pytest.raises(NotFoundException):
        uc_client.delete_function(function_name=function_name)


@pytest.mark.asyncio
async def test_execute_function_with_error(uc_client):
    uc_client.execution_mode = "local"
    function_name = f"{CATALOG}.{SCHEMA}.error_function"
    routine_definition = "raise ValueError('Intentional Error')"
    data_type = "STRING"
    parameters = []

    uc_client.create_function(
        function_name=function_name,
        routine_definition=routine_definition,
        data_type=data_type,
        full_data_type=data_type,
        parameters=parameters,
        timeout=10,
        replace=True,
        comment="test",
    )

    result = uc_client.execute_function(function_name=function_name, parameters={})
    assert result.error == "Intentional Error"
    assert result.value is None


@pytest.mark.asyncio
async def test_execute_function_with_error_async(uc_client):
    uc_client.execution_mode = "local"
    function_name = f"{CATALOG}.{SCHEMA}.error_function"
    routine_definition = "raise ValueError('Intentional Error')"
    data_type = "STRING"
    parameters = []

    uc_client.create_function(
        function_name=function_name,
        routine_definition=routine_definition,
        data_type=data_type,
        full_data_type=data_type,
        parameters=parameters,
        timeout=10,
        replace=True,
        comment="test",
    )

    result = await uc_client.execute_function_async(function_name=function_name, parameters={})
    assert result.error == "Intentional Error"
    assert result.value is None


@pytest.mark.asyncio
async def test_validate_input_parameter_invalid():
    invalid_parameter = {
        "type_text": "string",
        # Missing 'name' field
        "type_json": '{"name":"x","type":"string","nullable":false,"metadata":{}}',
        "type_name": "STRING",
        "position": 0,
    }

    with pytest.raises(ValidationError, match="1 validation error for FunctionParameterInfo"):
        validate_input_parameter(invalid_parameter)


@pytest.mark.asyncio
async def test_validate_param_invalid_interval():
    param = datetime.timedelta(days=1)
    column_type = "INTERVAL"
    param_type_text = "interval year to month"  # Invalid for timedelta

    with pytest.raises(ValueError, match="Invalid interval type text"):
        validate_param(param, column_type, param_type_text)


@pytest.mark.parametrize("param_type_text", ["interval day to second", "INTERVAL DAY TO SECOND"])
def test_validate_param_interval_type_text_is_case_insensitive(param_type_text):
    validate_param(datetime.timedelta(days=1), "INTERVAL", param_type_text)


def floating_point_param_infos(type_name: str, type_text: str) -> FunctionParameterInfos:
    return FunctionParameterInfos(
        parameters=[
            FunctionParameterInfo(
                name="radius",
                type_name=type_name,
                type_text=type_text,
                type_json=f'{{"name":"radius","type":"{type_text}","nullable":false,"metadata":{{}}}}',
                position=0,
            )
        ]
    )


FLOATING_POINT_TYPES = [("FLOAT", "float"), ("DOUBLE", "double"), ("DECIMAL", "decimal(10,2)")]


@pytest_asyncio.fixture
async def validation_client():
    uc_api_client = ApiClient(configuration=Configuration())
    client = UnitycatalogFunctionClient(api_client=uc_api_client)
    yield client
    await client.close_async()


@pytest.mark.asyncio
@pytest.mark.parametrize("type_name,type_text", FLOATING_POINT_TYPES)
@pytest.mark.parametrize("param_value", [2, 0, -7])
async def test_validate_input_params_accepts_int_for_floating_point_types(
    validation_client, type_name, type_text, param_value
):
    validation_client.validate_input_params(
        floating_point_param_infos(type_name, type_text), {"radius": param_value}
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("type_name,type_text", FLOATING_POINT_TYPES)
@pytest.mark.parametrize("param_value", [True, False])
async def test_validate_input_params_rejects_bool_for_floating_point_types(
    validation_client, type_name, type_text, param_value
):
    with pytest.raises(ValueError, match=f"Parameter radius should be of type .*{type_name}"):
        validation_client.validate_input_params(
            floating_point_param_infos(type_name, type_text), {"radius": param_value}
        )


@pytest_asyncio.fixture
async def local_execution_client():
    uc_api_client = ApiClient(configuration=Configuration())
    client = UnitycatalogFunctionClient(api_client=uc_api_client, execution_mode="local")
    yield client
    await client.close_async()


def repr_arg_function_info(type_name: str, type_text: str) -> FunctionInfo:
    return FunctionInfo(
        name="repr_arg",
        catalog_name=CATALOG,
        schema_name=SCHEMA,
        full_name=f"{CATALOG}.{SCHEMA}.repr_arg",
        input_params=FunctionParameterInfos(
            parameters=[
                FunctionParameterInfo(
                    name="x",
                    type_name=type_name,
                    type_text=type_text,
                    type_json=f'{{"name":"x","type":"{type_text}","nullable":false,"metadata":{{}}}}',
                    position=0,
                )
            ]
        ),
        data_type="STRING",
        full_data_type="STRING",
        routine_body="EXTERNAL",
        routine_definition="return repr(x)",
        external_language="PYTHON",
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("type_name", "type_text", "param_value", "expected"),
    [
        ("DATE", "date", "2026-01-11", datetime.date(2026, 1, 11)),
        ("TIMESTAMP", "timestamp", "2026-01-11T05:00:00", datetime.datetime(2026, 1, 11, 5)),
        (
            "TIMESTAMP",
            "timestamp",
            "2026-01-11T05:00:00Z",
            datetime.datetime(2026, 1, 11, 5, tzinfo=datetime.timezone.utc),
        ),
        (
            "TIMESTAMP_NTZ",
            "timestamp_ntz",
            "2026-01-11T05:00:00",
            datetime.datetime(2026, 1, 11, 5),
        ),
        ("BINARY", "binary", "aGVsbG8=", b"hello"),
        ("DECIMAL", "decimal(10,2)", 19.99, decimal.Decimal("19.99")),
        ("DECIMAL", "decimal(10,2)", "19.99", decimal.Decimal("19.99")),
        ("DECIMAL", "decimal(10,2)", 2, decimal.Decimal("2")),
        ("DATE", "date", datetime.date(2026, 1, 11), datetime.date(2026, 1, 11)),
        (
            "TIMESTAMP",
            "timestamp",
            datetime.datetime(2026, 1, 11, 5),
            datetime.datetime(2026, 1, 11, 5),
        ),
        ("BINARY", "binary", b"hello", b"hello"),
        ("DECIMAL", "decimal(10,2)", decimal.Decimal("19.99"), decimal.Decimal("19.99")),
    ],
)
async def test_execute_function_converts_json_arguments(
    local_execution_client, type_name, type_text, param_value, expected
):
    function_info = repr_arg_function_info(type_name, type_text)
    with mock.patch.object(local_execution_client, "get_function", return_value=function_info):
        result = local_execution_client.execute_function(
            function_info.full_name, {"x": param_value}
        )
        async_result = await local_execution_client.execute_function_async(
            function_info.full_name, {"x": param_value}
        )

    assert result.error is None
    assert result.value == repr(expected)
    assert async_result.value == repr(expected)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("type_name", "type_text", "param_value"),
    [
        ("DATE", "date", "2026-13-01"),
        ("TIMESTAMP", "timestamp", "not a timestamp"),
        ("TIMESTAMP_NTZ", "timestamp_ntz", "2026-01-11 25:00"),
        ("BINARY", "binary", "not base64!"),
        ("TIMESTAMP_NTZ", "timestamp_ntz", "2026-01-11T05:00:00Z"),
        ("TIMESTAMP_NTZ", "timestamp_ntz", "2026-01-11T05:00:00+05:00"),
        ("DECIMAL", "decimal(10,2)", "abc"),
        ("DECIMAL", "decimal(10,2)", "NaN"),
        ("DECIMAL", "decimal(10,2)", "sNaN"),
        ("DECIMAL", "decimal(10,2)", "Infinity"),
        ("DECIMAL", "decimal(10,2)", float("inf")),
        ("DECIMAL", "decimal(10,2)", float("nan")),
    ],
)
async def test_execute_function_rejects_invalid_json_arguments(
    local_execution_client, type_name, type_text, param_value
):
    function_info = repr_arg_function_info(type_name, type_text)
    with mock.patch.object(local_execution_client, "get_function", return_value=function_info):
        with pytest.raises(ValueError, match=rf"Parameter x .*{type_name}, but got invalid value"):
            local_execution_client.execute_function(function_info.full_name, {"x": param_value})
        with pytest.raises(ValueError, match=rf"Parameter x .*{type_name}, but got invalid value"):
            await local_execution_client.execute_function_async(
                function_info.full_name, {"x": param_value}
            )


@pytest.mark.asyncio
async def test_execute_function_converts_default_arguments(local_execution_client):
    def param(name: str, type_name: str, type_text: str, position: int, default=None):
        return FunctionParameterInfo(
            name=name,
            type_name=type_name,
            type_text=type_text,
            type_json=f'{{"name":"{name}","type":"{type_text}","nullable":false,"metadata":{{}}}}',
            position=position,
            parameter_default=default,
        )

    function_info = FunctionInfo(
        name="add_with_defaults",
        catalog_name=CATALOG,
        schema_name=SCHEMA,
        full_name=f"{CATALOG}.{SCHEMA}.add_with_defaults",
        input_params=FunctionParameterInfos(
            parameters=[
                param("a", "DECIMAL", "decimal(10,2)", 0),
                param("b", "DECIMAL", "decimal(10,2)", 1, "0.5"),
                param("d", "DATE", "date", 2, "2026-01-11"),
            ]
        ),
        data_type="STRING",
        full_data_type="STRING",
        routine_body="EXTERNAL",
        routine_definition="return repr((a + b, d))",
        external_language="PYTHON",
    )
    expected = repr((decimal.Decimal("2.5"), datetime.date(2026, 1, 11)))
    with mock.patch.object(local_execution_client, "get_function", return_value=function_info):
        result = local_execution_client.execute_function(function_info.full_name, {"a": 2})
        async_result = await local_execution_client.execute_function_async(
            function_info.full_name, {"a": 2}
        )

    assert result.error is None
    assert result.value == expected
    assert async_result.value == expected


@pytest.mark.asyncio
async def test_create_function_long_name(uc_client):
    long_function_name = f"{CATALOG}.{SCHEMA}." + "a" * 256  # Function name exceeds typical limits
    routine_definition = "return x"
    data_type = "STRING"
    parameters = [
        FunctionParameterInfo(
            name="x",
            type_text="string",
            type_json='{"name":"x","type":"string","nullable":false,"metadata":{}}',
            type_name="STRING",
            position=0,
        )
    ]

    with pytest.raises(ValueError, match="The maximum length of a function name"):
        uc_client.create_function(
            function_name=long_function_name,
            routine_definition=routine_definition,
            data_type=data_type,
            full_data_type=data_type,
            parameters=parameters,
            timeout=10,
            replace=True,
            comment="test",
        )


@pytest.mark.asyncio
async def test_function_caching(uc_client):
    function_name = f"{CATALOG}.{SCHEMA}.cached_function"
    routine_definition = "return x * 2"
    data_type = "INT"
    parameters = [
        FunctionParameterInfo(
            name="x",
            type_text="int",
            type_json='{"name":"x","type":"int","nullable":false,"metadata":{}}',
            type_name="INT",
            position=0,
        )
    ]

    uc_client.create_function(
        function_name=function_name,
        routine_definition=routine_definition,
        data_type=data_type,
        full_data_type=data_type,
        parameters=parameters,
        timeout=10,
        replace=True,
        comment="test",
    )

    result1 = uc_client.execute_function(function_name=function_name, parameters={"x": 2})
    result2 = uc_client.execute_function(function_name=function_name, parameters={"x": 3})

    assert result1.value == 4
    assert result2.value == 6

    assert function_name in uc_client.func_cache


@pytest.mark.asyncio
async def test_function_overwrite_cache_invalidate(uc_client):
    function_name = f"{CATALOG}.{SCHEMA}.cached_function_check"
    routine_definition = "return x * 2"
    data_type = "INT"
    parameters = [
        FunctionParameterInfo(
            name="x",
            type_text="int",
            type_json='{"name":"x","type":"int","nullable":false,"metadata":{}}',
            type_name="INT",
            position=0,
        )
    ]

    uc_client.create_function(
        function_name=function_name,
        routine_definition=routine_definition,
        data_type=data_type,
        full_data_type=data_type,
        parameters=parameters,
        timeout=10,
        replace=True,
        comment="test",
    )

    # Execute the function to cache it
    result1 = uc_client.execute_function(function_name=function_name, parameters={"x": 2})
    assert result1.value == 4

    # Overwrite the function with a new definition
    new_routine_definition = "return x * 3"
    uc_client.create_function(
        function_name=function_name,
        routine_definition=new_routine_definition,
        data_type=data_type,
        full_data_type=data_type,
        parameters=parameters,
        timeout=10,
        replace=True,
        comment="test",
    )

    # Execute the function again to check if the cache is invalidated
    result2 = uc_client.execute_function(function_name=function_name, parameters={"x": 2})
    assert result2.value == 6


@pytest_asyncio.fixture
async def mocked_store_client():
    uc_api_client = ApiClient(configuration=Configuration())
    client = UnitycatalogFunctionClient(api_client=uc_api_client, execution_mode="local")
    client.uc.functions_client = mock.AsyncMock(spec=FunctionsApi)
    yield client
    await client.close_async()


def stored_python_function(
    full_name: str,
    routine_definition: str,
    data_type: str,
    parameters: Optional[List[FunctionParameterInfo]] = None,
) -> FunctionInfo:
    catalog_name, schema_name, name = full_name.split(".")
    return FunctionInfo(
        name=name,
        catalog_name=catalog_name,
        schema_name=schema_name,
        full_name=full_name,
        data_type=data_type,
        full_data_type=data_type,
        input_params={"parameters": parameters or []},
        routine_body="EXTERNAL",
        routine_definition=routine_definition,
        external_language="PYTHON",
    )


def int_param(name: str, position: int = 0) -> FunctionParameterInfo:
    return FunctionParameterInfo(
        name=name,
        type_name="INT",
        type_text="int",
        type_json=f'{{"name":"{name}","type":"integer","nullable":false,"metadata":{{}}}}',
        position=position,
    )


def serve_functions(client: UnitycatalogFunctionClient, *function_infos: FunctionInfo) -> None:
    by_name = {info.full_name: info for info in function_infos}
    client.uc.functions_client.get_function.side_effect = lambda name, **_: by_name[name]


async def execute(client: UnitycatalogFunctionClient, use_async: bool, name: str, params: dict):
    if use_async:
        return await client.execute_function_async(name, params)
    return client.execute_function(name, params)


@pytest.mark.asyncio
@pytest.mark.parametrize("use_async", [False, True])
async def test_same_function_name_in_different_schemas_executes_own_definition(
    mocked_store_client, use_async
):
    serve_functions(
        mocked_store_client,
        stored_python_function(
            f"{CATALOG}.s1.fold", "return n * fold(n - 1) if n > 1 else 1", "INT", [int_param("n")]
        ),
        stored_python_function(
            f"{CATALOG}.s2.fold", "return n + fold(n - 1) if n > 0 else 0", "INT", [int_param("n")]
        ),
    )

    results = [
        (await execute(mocked_store_client, use_async, f"{CATALOG}.{schema}.fold", {"n": 5})).value
        for schema in ("s1", "s2", "s1", "s2")
    ]

    assert results == [120, 15, 120, 15]


@pytest.mark.asyncio
@pytest.mark.parametrize("use_async", [False, True])
async def test_function_with_list_argument_runs_on_repeated_calls(mocked_store_client, use_async):
    xs_param = FunctionParameterInfo(
        name="xs",
        type_name="ARRAY",
        type_text="array<int>",
        type_json='{"name":"xs","type":{"type":"array","elementType":"integer","containsNull":true},'
        '"nullable":false,"metadata":{}}',
        position=0,
    )
    serve_functions(
        mocked_store_client,
        stored_python_function(f"{CATALOG}.{SCHEMA}.total", "return sum(xs)", "INT", [xs_param]),
    )

    results = [
        await execute(mocked_store_client, use_async, f"{CATALOG}.{SCHEMA}.total", {"xs": xs})
        for xs in ([1, 2], [3, 4], [1, 2])
    ]

    assert [(result.error, result.value) for result in results] == [(None, 3), (None, 7), (None, 3)]


@pytest.mark.asyncio
@pytest.mark.parametrize("use_async", [False, True])
async def test_function_is_reexecuted_instead_of_memoized(
    mocked_store_client, use_async, monkeypatch
):
    serve_functions(
        mocked_store_client,
        stored_python_function(
            f"{CATALOG}.{SCHEMA}.read_setting",
            'import os\nreturn os.environ["UC_AI_TEST_SETTING"]',
            "STRING",
        ),
    )

    results = []
    for value in ("first", "second", "third"):
        monkeypatch.setenv("UC_AI_TEST_SETTING", value)
        result = await execute(
            mocked_store_client, use_async, f"{CATALOG}.{SCHEMA}.read_setting", {}
        )
        results.append(result.value)

    assert results == ["first", "second", "third"]


@pytest.mark.asyncio
@pytest.mark.parametrize("use_async", [False, True])
@pytest.mark.parametrize(
    ("new_definition", "new_param_names", "arguments", "expected"),
    [
        ("return x * 25", ["x"], {"x": 10}, 250),
        ("return x * y", ["x", "y"], {"x": 10, "y": 3}, 30),
        ("return x * 10", ["x", "y"], {"x": 10, "y": 3}, 100),
    ],
)
async def test_function_changed_in_catalog_executes_new_definition(
    mocked_store_client,
    use_async,
    new_definition,
    new_param_names,
    arguments,
    expected,
):
    function_name = f"{CATALOG}.{SCHEMA}.tax"
    serve_functions(
        mocked_store_client,
        stored_python_function(function_name, "return x * 10", "INT", [int_param("x")]),
    )
    first = await execute(mocked_store_client, use_async, function_name, {"x": 10})

    serve_functions(
        mocked_store_client,
        stored_python_function(
            function_name,
            new_definition,
            "INT",
            [int_param(name, position) for position, name in enumerate(new_param_names)],
        ),
    )
    second = await execute(mocked_store_client, use_async, function_name, arguments)

    assert (first.value, second.error, second.value) == (100, None, expected)


@pytest.mark.asyncio
@pytest.mark.parametrize("use_async", [False, True])
async def test_unchanged_function_reuses_cached_callable(mocked_store_client, use_async):
    function_name = f"{CATALOG}.{SCHEMA}.count_calls"
    serve_functions(
        mocked_store_client,
        stored_python_function(
            function_name,
            'globals()["calls"] = globals().get("calls", 0) + 1\nreturn globals()["calls"]',
            "INT",
        ),
    )

    results = [
        (await execute(mocked_store_client, use_async, function_name, {})).value for _ in range(3)
    ]

    assert results == [1, 2, 3]


@pytest.mark.asyncio
@pytest.mark.parametrize("use_async", [False, True])
async def test_changed_function_is_rebuilt_once_then_reused(mocked_store_client, use_async):
    function_name = f"{CATALOG}.{SCHEMA}.count_calls"
    counter = 'globals()["calls"] = globals().get("calls", 0) + 1\nreturn globals()["calls"]'
    serve_functions(mocked_store_client, stored_python_function(function_name, counter, "INT"))
    before = [
        (await execute(mocked_store_client, use_async, function_name, {})).value for _ in range(2)
    ]

    serve_functions(
        mocked_store_client,
        stored_python_function(function_name, counter + " * 10", "INT"),
    )
    after = [
        (await execute(mocked_store_client, use_async, function_name, {})).value for _ in range(3)
    ]

    assert (before, after) == ([1, 2], [10, 20, 30])


@pytest.mark.asyncio
@pytest.mark.parametrize("use_async", [False, True])
async def test_replacing_a_function_resets_only_its_cached_callable(mocked_store_client, use_async):
    counter = 'globals()["calls"] = globals().get("calls", 0) + 1\nreturn globals()["calls"]'
    replaced = f"{CATALOG}.{SCHEMA}.counter_replaced"
    untouched = f"{CATALOG}.{SCHEMA}.counter_untouched"
    by_name = {name: stored_python_function(name, counter, "INT") for name in (replaced, untouched)}
    mocked_store_client.uc.functions_client.get_function.side_effect = lambda name, **_: by_name[
        name.lower()
    ]
    for name in (replaced, untouched, replaced, untouched):
        await execute(mocked_store_client, use_async, name, {})

    mocked_store_client.uc.functions_client.create_function.return_value = stored_python_function(
        replaced, counter, "INT"
    )
    create_kwargs = {
        "function_name": replaced.upper(),
        "routine_definition": counter,
        "data_type": "INT",
        "full_data_type": "INT",
        "comment": "Counts its calls.",
        "parameters": [],
        "replace": True,
    }
    if use_async:
        await mocked_store_client.create_function_async(**create_kwargs)
    else:
        mocked_store_client.create_function(**create_kwargs)

    after = [
        (await execute(mocked_store_client, use_async, name, {})).value
        for name in (replaced, untouched)
    ]

    assert after == [1, 3]


@pytest.mark.asyncio
async def test_to_dict(uc_client):
    client_dict = uc_client.to_dict()
    assert isinstance(client_dict, dict)
    assert isinstance(client_dict["uc"], UnitycatalogClient)


@pytest.mark.asyncio
async def test_create_and_execute_python_function(uc_client):
    def test_simple_func(a: str, b: str) -> str:
        """
        Returns an upper case concatenation of two strings separated by a space.

        Args:
            a: the first string
            b: the second string

        Returns:
            Uppercased concatenation of the two strings.
        """
        concatenated = f"{a} {b}"

        return concatenated.upper()

    stored_func = uc_client.create_python_function(
        func=test_simple_func,
        catalog=CATALOG,
        schema=SCHEMA,
        replace=True,
    )

    function_name = f"{CATALOG}.{SCHEMA}.test_simple_func"

    assert stored_func.full_name == function_name

    retrieved_func = uc_client.get_function(function_name=function_name)

    assert retrieved_func.name == "test_simple_func"
    assert retrieved_func.full_name == function_name
    assert (
        retrieved_func.comment
        == "Returns an upper case concatenation of two strings separated by a space."
    )
    assert retrieved_func.full_data_type == "STRING"
    assert retrieved_func.external_language == "PYTHON"

    result = uc_client.execute_function(
        function_name=function_name, parameters={"a": "unity", "b": "catalog"}
    )

    assert result.value == "UNITY CATALOG"

    uc_client.delete_function(function_name=function_name)


def double_amount(amount: decimal.Decimal) -> str:
    """
    Doubles an amount.

    Args:
        amount: the amount
    """
    return str(amount * 2)


def parse_amount(text: str) -> decimal.Decimal:
    """
    Parses an amount.

    Args:
        text: the amount as text
    """
    import decimal

    return decimal.Decimal(text)


def shift_time(delta: datetime.timedelta) -> str:
    """
    Shifts a fixed time by a duration.

    Args:
        delta: the duration
    """
    import datetime

    return (datetime.datetime(2024, 1, 1) + delta).isoformat()


def to_duration(minutes: int) -> datetime.timedelta:
    """
    Converts minutes to a duration.

    Args:
        minutes: the number of minutes
    """
    import datetime

    return datetime.timedelta(minutes=minutes)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "func,parameters,expected",
    [
        (double_amount, {"amount": decimal.Decimal("1.25")}, "2.50"),
        (parse_amount, {"text": "3.75"}, decimal.Decimal("3.75")),
        (shift_time, {"delta": datetime.timedelta(minutes=90)}, "2024-01-01T01:30:00"),
        (to_duration, {"minutes": 45}, datetime.timedelta(minutes=45)),
    ],
    ids=["decimal_param", "decimal_return", "interval_param", "interval_return"],
)
async def test_create_and_execute_python_function_with_decimal_and_interval(
    uc_client, func, parameters, expected
):
    function_name = f"{CATALOG}.{SCHEMA}.{func.__name__}"
    try:
        uc_client.create_python_function(func=func, catalog=CATALOG, schema=SCHEMA, replace=True)
        result = uc_client.execute_function(function_name=function_name, parameters=parameters)
    finally:
        try_delete_function(uc_client, function_name)

    assert result.error is None
    assert result.value == expected


@pytest.mark.asyncio
async def test_create_python_function_with_invalid_arguments(uc_client):
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
        uc_client.create_python_function(func=invalid_func, catalog=CATALOG, schema=SCHEMA)

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
        uc_client.create_python_function(func=another_invalid_func, catalog=CATALOG, schema=SCHEMA)


@pytest.mark.asyncio
async def test_create_python_function_missing_return_type(uc_client):
    def missing_return_type_func(a: int, b: int):
        """A function that lacks a return type."""
        return a + b

    with pytest.raises(
        ValueError,
        match="Return type for function 'missing_return_type_func' is not defined. Please provide a return type.",
    ):
        uc_client.create_python_function(
            func=missing_return_type_func, catalog=CATALOG, schema=SCHEMA
        )


@pytest.mark.asyncio
async def test_create_python_function_not_callable(uc_client):
    scalar = 42

    with pytest.raises(ValueError, match="The provided function is not callable"):
        uc_client.create_python_function(func=scalar, catalog=CATALOG, schema=SCHEMA)


@pytest.mark.asyncio
@pytest.mark.parametrize("use_async", [False, True])
async def test_create_python_function_rejects_async_function(mocked_store_client, use_async):
    with pytest.raises(ValueError, match="is an async function"):
        if use_async:
            await mocked_store_client.create_python_function_async(
                func=async_int_func, catalog=CATALOG, schema=SCHEMA
            )
        else:
            mocked_store_client.create_python_function(
                func=async_int_func, catalog=CATALOG, schema=SCHEMA
            )

    mocked_store_client.uc.functions_client.create_function.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("use_async", [False, True])
async def test_create_wrapped_function_rejects_async_function(mocked_store_client, use_async):
    with pytest.raises(ValueError, match="is an async function"):
        if use_async:
            await mocked_store_client.create_wrapped_function_async(
                primary_func=async_int_func, functions=[], catalog=CATALOG, schema=SCHEMA
            )
        else:
            mocked_store_client.create_wrapped_function(
                primary_func=async_int_func, functions=[], catalog=CATALOG, schema=SCHEMA
            )

    mocked_store_client.uc.functions_client.create_function.assert_not_called()


@pytest.mark.asyncio
async def test_function_with_invalid_list_return_type(uc_client):
    def func_with_invalid_list_return(a: int) -> List:
        """A function returning a list without specifying the element type."""
        return list(range(a))

    with pytest.raises(
        ValueError,
        match=re.escape(
            "Error in return type for function 'func_with_invalid_list_return': typing.List. Please define the inner types, e.g., List[int], Tuple[str, int], Dict[str, int]."
        ),
    ):
        uc_client.create_python_function(
            func=func_with_invalid_list_return, catalog=CATALOG, schema=SCHEMA
        )


@pytest.mark.asyncio
async def test_function_with_invalid_dict_return_type(uc_client):
    def func_with_invalid_dict_return(a: int) -> Dict:
        """A function returning a dict without specifying key and value types."""
        return {f"key_{i}": i for i in range(a)}

    with pytest.raises(
        ValueError,
        match=re.escape(
            "Error in return type for function 'func_with_invalid_dict_return': typing.Dict. Please define the inner types, e.g., List[int], Tuple[str, int], Dict[str, int]."
        ),
    ):
        uc_client.create_python_function(
            func=func_with_invalid_dict_return, catalog=CATALOG, schema=SCHEMA
        )


@pytest.mark.asyncio
async def test_function_with_union_return_type(uc_client):
    def func_with_union_return(a: int) -> Union[str, int]:
        """A function returning a union type."""
        return a if a % 2 == 0 else str(a)

    with pytest.raises(
        ValueError,
        match=re.escape(
            "Error in return type for function 'func_with_union_return': typing.Union[str, int]. Union types are not supported in return types."
        ),
    ):
        uc_client.create_python_function(
            func=func_with_union_return, catalog=CATALOG, schema=SCHEMA
        )


# --------------------------
# Validation Tests for Wrapped Function APIs
# --------------------------


@pytest.mark.asyncio
async def test_create_wrapped_function_async_no_doc(uc_client: UnitycatalogFunctionClient):
    primary_func = wrap_func_no_doc
    wrapped_functions = [int_func_no_doc, str_func_no_doc]
    catalog = CATALOG
    schema = SCHEMA

    func_info = await uc_client.create_wrapped_function_async(
        primary_func=primary_func,
        functions=wrapped_functions,
        catalog=catalog,
        schema=schema,
        replace=True,
    )

    full_name = f"{catalog}.{schema}.wrap_func_no_doc"

    assert func_info.name == "wrap_func_no_doc"
    assert func_info.full_name == full_name
    assert func_info.data_type == "STRING"
    assert func_info.full_data_type == "STRING"
    assert func_info.comment == "A function that takes two arguments and returns a string."

    expected_definition = (
        "def int_func_no_doc(a):\n"
        "    return a + 10\n\n"
        "def str_func_no_doc(b):\n"
        '    return f"str value: {b}"\n\n'
        "func1 = int_func_no_doc(a)\n"
        "func2 = str_func_no_doc(b)\n\n"
        'return f"{func1} {func2}"'
    )
    assert func_info.routine_definition == expected_definition

    result = uc_client.execute_function(function_name=full_name, parameters={"a": 5, "b": "test"})
    assert result.value == "15 str value: test"
    assert result.error is None


@pytest.mark.asyncio
async def test_create_wrapped_function_async_with_doc(uc_client: UnitycatalogFunctionClient):
    primary_func = wrap_func_with_doc
    wrapped_functions = [int_func_with_doc, str_func_with_doc]
    catalog = CATALOG
    schema = SCHEMA

    func_info = await uc_client.create_wrapped_function_async(
        primary_func=primary_func,
        functions=wrapped_functions,
        catalog=catalog,
        schema=schema,
        replace=True,
    )

    full_name = f"{catalog}.{schema}.wrap_func_with_doc"

    assert func_info.name == "wrap_func_with_doc"
    assert func_info.full_name == full_name
    assert func_info.data_type == "STRING"
    assert func_info.full_data_type == "STRING"
    assert func_info.comment == "A function that takes two arguments and returns a string."

    expected_definition = textwrap.dedent("""\
        def int_func_with_doc(a: int) -> int:
            \"\"\"
        A function that takes an integer and returns an integer.

        Args:
            a: An integer.

        Returns:
            An integer.
        \"\"\"
            return a + 10

        def str_func_with_doc(b: str) -> str:
            \"\"\"
        A function that takes a string and returns a string.

        Args:
            b: A string.

        Returns:
            A string.
        \"\"\"
            return f"str value: {b}"

        func1 = int_func_with_doc(a)
        func2 = str_func_with_doc(b)

        return f"{func1} {func2}"
    """).strip()
    assert func_info.routine_definition == expected_definition

    result = uc_client.execute_function(function_name=full_name, parameters={"a": 52, "b": "docs"})
    assert result.value == "62 str value: docs"
    assert result.error is None


def test_create_wrapped_function_sync_no_doc(uc_client: UnitycatalogFunctionClient):
    primary_func = wrap_func_no_doc
    wrapped_functions = [int_func_no_doc, str_func_no_doc]
    catalog = CATALOG
    schema = SCHEMA
    full_name = f"{catalog}.{schema}.wrap_func_no_doc"

    func_info = uc_client.create_wrapped_function(
        primary_func=primary_func,
        functions=wrapped_functions,
        catalog=catalog,
        schema=schema,
        replace=True,
    )

    assert func_info.name == "wrap_func_no_doc"
    assert func_info.full_name == full_name
    assert func_info.data_type == "STRING"
    assert func_info.full_data_type == "STRING"
    assert func_info.comment == "A function that takes two arguments and returns a string."

    result = uc_client.execute_function(function_name=full_name, parameters={"a": 5, "b": "test"})
    assert result.value == "15 str value: test"
    assert result.error is None


def test_create_wrapped_function_sync_with_doc(uc_client: UnitycatalogFunctionClient):
    primary_func = wrap_func_with_doc
    wrapped_functions = [int_func_with_doc, str_func_with_doc]
    catalog = CATALOG
    schema = SCHEMA
    full_name = f"{catalog}.{schema}.wrap_func_with_doc"

    func_info = uc_client.create_wrapped_function(
        primary_func=primary_func,
        functions=wrapped_functions,
        catalog=catalog,
        schema=schema,
        replace=True,
    )

    assert func_info.name == "wrap_func_with_doc"
    assert func_info.full_name == full_name
    assert func_info.data_type == "STRING"
    assert func_info.full_data_type == "STRING"
    assert func_info.comment == "A function that takes two arguments and returns a string."

    result = uc_client.execute_function(function_name=full_name, parameters={"a": 15, "b": "test"})
    assert result.value == "25 str value: test"
    assert result.error is None


@pytest.mark.asyncio
async def test_create_wrapped_function_non_callable_primary(uc_client: UnitycatalogFunctionClient):
    primary_func = "not_a_function"
    wrapped_functions = [int_func_no_doc, str_func_no_doc]
    catalog = CATALOG
    schema = SCHEMA

    with pytest.raises(ValueError, match="The provided primary function is not callable."):
        await uc_client.create_wrapped_function_async(
            primary_func=primary_func,
            functions=wrapped_functions,
            catalog=catalog,
            schema=schema,
        )


@pytest.mark.asyncio
async def test_create_wrapped_function_long_name(uc_client: UnitycatalogFunctionClient):
    primary_func = wrap_func_no_doc
    wrapped_functions = [int_func_no_doc, str_func_no_doc]
    catalog = CATALOG
    schema = SCHEMA
    long_callable_name = "a" * 256

    def long_named_func(a, b):
        return a + b

    primary_func.__name__ = long_callable_name
    try:
        with pytest.raises(ValueError, match="The maximum length of a function name"):
            await uc_client.create_wrapped_function_async(
                primary_func=primary_func,
                functions=wrapped_functions,
                catalog=catalog,
                schema=schema,
            )
    finally:
        primary_func.__name__ = "wrap_func_no_doc"


@pytest.mark.asyncio
async def test_create_wrapped_function_invalid_catalog_or_schema(
    uc_client: UnitycatalogFunctionClient,
):
    primary_func = wrap_func_no_doc
    wrapped_functions = [int_func_no_doc, str_func_no_doc]
    invalid_catalog = "NonExistentCatalog"
    invalid_schema = "NonExistentSchema"

    with pytest.raises(NotFoundException, match="Catalog not found"):
        await uc_client.create_wrapped_function_async(
            primary_func=primary_func,
            functions=wrapped_functions,
            catalog=invalid_catalog,
            schema=SCHEMA,
            replace=True,
        )

    with pytest.raises(NotFoundException, match="Schema not found"):
        await uc_client.create_wrapped_function_async(
            primary_func=primary_func,
            functions=wrapped_functions,
            catalog=CATALOG,
            schema=invalid_schema,
        )


@pytest.mark.asyncio
async def test_function_with_variant_param_raises_in_oss(uc_client):
    def func_variant_param(a: Variant) -> str:
        """
        A function that accepts a VARIANT parameter.

        Args:
            a (Variant): A variant parameter.

        Returns:
            str: The JSON string representation of the variant.
        """
        import json

        return json.dumps(a)

    with pytest.raises(ValueError, match="Variant type is not supported for function parameters"):
        uc_client.create_python_function(
            func=func_variant_param, catalog=CATALOG, schema=SCHEMA, replace=True
        )


# --------------------------
# Test default parameter handling
# --------------------------


def test_create_function_with_default_params(uc_client: UnitycatalogFunctionClient):
    def func_with_defaults(a: int, b: str = "default") -> str:
        """
        A function that concatenates an integer and a string with a default string value.

        Args:
            a (int): An integer.
            b (str, optional): A string. Defaults to "default".

        Returns:
            str: The concatenated string.
        """
        return f"{a} {b}"

    full_name = f"{CATALOG}.{SCHEMA}.func_with_defaults"

    uc_client.create_python_function(
        func=func_with_defaults, catalog=CATALOG, schema=SCHEMA, replace=True
    )

    result = uc_client.execute_function(function_name=full_name, parameters={"a": 10})
    assert result.value == "10 default"

    result = uc_client.execute_function(function_name=full_name, parameters={"a": 20, "b": "test"})
    assert result.value == "20 test"


def test_create_function_with_all_defaults(uc_client: UnitycatalogFunctionClient):
    def func_with_all_defaults(
        a: int = 1, b: str = "default", c: float = 3.14, d: bool = True
    ) -> str:
        """
        A function that concatenates various types with default values.

        Args:
            a (int, optional): An integer. Defaults to 1.
            b (str, optional): A string. Defaults to "default".
            c (float, optional): A float. Defaults to 3.14.
            d (bool, optional): A boolean. Defaults to True.

        Returns:
            str: The concatenated string.
        """
        return f"{a} {b} {c} {d}"

    full_name = f"{CATALOG}.{SCHEMA}.func_with_all_defaults"

    uc_client.create_python_function(
        func=func_with_all_defaults, catalog=CATALOG, schema=SCHEMA, replace=True
    )

    result = uc_client.execute_function(function_name=full_name, parameters={})
    assert result.value == "1 default 3.14 True"

    result = uc_client.execute_function(
        function_name=full_name,
        parameters={"a": 10, "b": "test", "c": 2.71, "d": False},
    )
    assert result.value == "10 test 2.71 False"

    result = uc_client.execute_function(
        function_name=full_name,
    )
    assert result.value == "1 default 3.14 True"


def test_create_function_no_params(uc_client: UnitycatalogFunctionClient):
    def func_no_params() -> str:
        """
        A function that returns a static string.

        Returns:
            str: A static string.
        """
        return "No parameters here!"

    full_name = f"{CATALOG}.{SCHEMA}.func_no_params"

    uc_client.create_python_function(
        func=func_no_params, catalog=CATALOG, schema=SCHEMA, replace=True
    )

    result = uc_client.execute_function(function_name=full_name, parameters={})
    assert result.value == "No parameters here!"

    with pytest.raises(ValueError, match="Function does not have input parameters, but parameters"):
        uc_client.execute_function(function_name=full_name, parameters={"unexpected": "value"})


def test_create_function_with_none_param_default(uc_client: UnitycatalogFunctionClient):
    def null_func(a: Optional[str] = None) -> str:
        """
        A function that returns the string representation of its input.

        Args:
            a (Optional[str], optional): An optional string. Defaults to None.

        Returns:
            str: The string representation of the input.
        """
        return str(a)

    full_name = f"{CATALOG}.{SCHEMA}.null_func"
    uc_client.create_python_function(func=null_func, catalog=CATALOG, schema=SCHEMA, replace=True)
    result = uc_client.execute_function(function_name=full_name, parameters={})
    assert result.value == "None"


def test_get_python_callable_integration_complex(uc_client: UnitycatalogFunctionClient):
    def complex_python_func(
        a: int,
        b: float,
        c: str,
        d: bool,
        e: list[str],
        f: dict[str, int],
        h: dict[str, list[int]],
        i: dict[str, list[dict[str, list[int]]]],
    ) -> dict[str, list[str]]:
        """
        A complex function that processes various types.

        Args:
            a: an int
            b: a float
            c: a string
            d: a bool
            e: a list of strings
            f: a dict mapping strings to ints
            h: a dict mapping strings to lists of ints
            i: a dict mapping strings to lists of dicts mapping strings to lists of ints

        Returns:
            dict[str, list[str]]: A dictionary with a single key "result" and a list of string representations.
        """

        def _helper(x: float) -> int:
            return int(x) + a

        return {"result": [str(a), str(b), c, str(d), ",".join(e), str(f), str(h), str(i)]}

    function_name = f"{CATALOG}.{SCHEMA}.complex_python_func"
    uc_client.create_python_function(
        func=complex_python_func, catalog=CATALOG, schema=SCHEMA, replace=True
    )
    callable_def = uc_client.get_function_source(function_name)

    expected_header = (
        "def complex_python_func(a: int, b: float, c: str, d: bool, e: list[str], "
        "f: dict[str, int], h: dict[str, list[int]], i: dict[str, list[dict[str, "
        "list[int]]]]) -> dict[str, list[str]]:"
    )

    assert expected_header in callable_def
    assert "def _helper(x: float) -> int:" in callable_def
    assert "return {" in callable_def and '"result": [' in callable_def
    assert "Args:" in callable_def
    assert "Returns:" in callable_def


def test_tuple_handling(uc_client: UnitycatalogFunctionClient):
    def tuple_func(a: tuple[int], b: tuple[str], c: list[tuple[str]]) -> tuple[str]:
        """
        A function that processes tuples.

        Args:
            a: a tuple of integers
            b: a tuple of strings
            c: a list of tuples of strings

        Returns:
            tuple[str]: A tuple with the first string and the sum of integers.
        """
        return b[0], str(sum(a))

    function_name = f"{CATALOG}.{SCHEMA}.tuple_func"
    uc_client.create_python_function(func=tuple_func, catalog=CATALOG, schema=SCHEMA, replace=True)
    callable_def = uc_client.get_function_source(function_name)
    expected_header = "def tuple_func(a: list[int], b: list[str], c: list[list[str]]) -> list[str]:"
    assert expected_header in callable_def
    assert "return b" in callable_def
    assert "Args:" in callable_def
    assert "Returns:" in callable_def


def test_get_python_callable_integration_standard_indent(uc_client: UnitycatalogFunctionClient):
    def simple_func(a: int, b: int) -> int:
        """
        A simple test function.

        Args:
          a: an int
          b: an int

        Returns:
          int: The sum of a and b.
        """

        def _internal(x: int) -> int:
            return x + a

        return _internal(b)

    function_name = f"{CATALOG}.{SCHEMA}.simple_func"

    uc_client.create_python_function(func=simple_func, catalog=CATALOG, schema=SCHEMA, replace=True)

    callable_def = uc_client.get_function_source(function_name)

    expected_def = (
        "def simple_func(a: int, b: int) -> int:\n"
        '    """\n'
        "    A simple test function.\n"
        "    \n"
        "    Args:\n"
        "        a: an int\n"
        "        b: an int\n"
        "    \n"
        "    Returns:\n"
        "        int\n"
        '    """\n'
        "    def _internal(x: int) -> int:\n"
        "        return x + a\n\n"
        "    return _internal(b)\n"
    )

    # Assert exact match.
    assert callable_def == expected_def, f"Expected:\n{expected_def}\nGot:\n{callable_def}"


def test_get_python_callable_integration_complex(uc_client: UnitycatalogFunctionClient):
    def complex_func(a: int, b: int) -> float:
        """
        A complex test function.

        Args:
          a: an int
          b: an int

        Returns:
          int: The product of a and b plus 10.
        """
        import math

        def _inner(x: int) -> int:
            return x + 5

        def _helper(y: int) -> int:
            def _nested(z: int) -> int:
                return z * 2

            return _nested(y) + _inner(y)

        return _helper(a) + math.sqrt(b)

    function_name = f"{CATALOG}.{SCHEMA}.complex_func"

    uc_client.create_python_function(
        func=complex_func, catalog=CATALOG, schema=SCHEMA, replace=True
    )

    callable_def = uc_client.get_function_source(function_name)

    expected_def = (
        "def complex_func(a: int, b: int) -> float:\n"
        '    """\n'
        "    A complex test function.\n"
        "    \n"
        "    Args:\n"
        "        a: an int\n"
        "        b: an int\n"
        "    \n"
        "    Returns:\n"
        "        float\n"
        '    """\n'
        "    import math\n\n"
        "    def _inner(x: int) -> int:\n"
        "        return x + 5\n\n"
        "    def _helper(y: int) -> int:\n"
        "        def _nested(z: int) -> int:\n"
        "            return z * 2\n\n"
        "        return _nested(y) + _inner(y)\n\n"
        "    return _helper(a) + math.sqrt(b)\n"
    )

    assert callable_def == expected_def, f"Expected:\n{expected_def}\nGot:\n{callable_def}"


def test_long_argument_comment(uc_client: UnitycatalogFunctionClient):
    def long_comment_func(a: int) -> int:
        """
        A function with a long argument comment.

        Args:
            a: An integer that represents the first number. Like some integers, this one is also an integer.
            This comment is intentionally long.

        Returns:
            The integer value.
        """
        return a

    function_name = f"{CATALOG}.{SCHEMA}.long_comment_func"
    uc_client.create_python_function(
        func=long_comment_func, catalog=CATALOG, schema=SCHEMA, replace=True
    )
    callable_def = uc_client.get_function_source(function_name)
    expected_header = (
        "def long_comment_func(a: int) -> int:\n"
        '    """\n'
        "    A function with a long argument comment.\n"
        "    \n"
        "    Args:\n"
        "        a: An integer that represents the first number. Like some integers, this one is also an integer.\n"
        "          This comment is intentionally long.\n"
        "    \n"
        "    Returns:\n"
        "        int\n"
        '    """\n'
    )
    assert callable_def.startswith(expected_header), (
        f"Expected:\n{expected_header}\nGot:\n{callable_def}"
    )


def test_local_function_execution_sync(uc_client: UnitycatalogFunctionClient):
    uc_client.execution_mode = "local"
    function_name = f"{CATALOG}.{SCHEMA}.simple_func"
    uc_client.create_python_function(func=simple_func, catalog=CATALOG, schema=SCHEMA, replace=True)

    result = uc_client.execute_function(function_name=function_name, parameters={"a": 5, "b": 10})
    assert result.value == 15
    assert result.format == "SCALAR"


@pytest.mark.asyncio
async def test_local_function_execution_async(uc_client: UnitycatalogFunctionClient):
    uc_client.execution_mode = "local"
    function_name = f"{CATALOG}.{SCHEMA}.simple_func"
    uc_client.create_python_function(func=simple_func, catalog=CATALOG, schema=SCHEMA, replace=True)

    result = await uc_client.execute_function_async(
        function_name=function_name, parameters={"a": 5, "b": 10}
    )
    assert result.value == 15
    assert result.format == "SCALAR"


def test_manual_function_sandbox_execution_sync(uc_client: UnitycatalogFunctionClient):
    uc_client.execution_mode = "sandbox"
    function_name = f"{CATALOG}.{SCHEMA}.simple_func"
    uc_client.create_python_function(func=simple_func, catalog=CATALOG, schema=SCHEMA, replace=True)

    retrieved = uc_client.get_function_source(function_name)
    converted = load_function_from_string(retrieved)
    success, result = run_in_sandbox(converted, {"a": 5, "b": 10})
    assert success
    assert result == 15


@pytest.mark.asyncio
async def test_manual_function_sandbox_execution_async(uc_client: UnitycatalogFunctionClient):
    uc_client.execution_mode = "sandbox"
    function_name = f"{CATALOG}.{SCHEMA}.simple_func"
    uc_client.create_python_function(func=simple_func, catalog=CATALOG, schema=SCHEMA, replace=True)

    retrieved = uc_client.get_function_source(function_name)
    converted = load_function_from_string(retrieved)
    success, result = await run_in_sandbox_async(converted, {"a": 5, "b": 10})
    assert success
    assert result == 15


def test_function_exception_local_python_function(uc_client):
    uc_client.execution_mode = "local"

    def raise_error(a: int, b: int) -> None:
        """
        Raises an exception for testing purposes.
        Args:
            a: an integer
            b: another integer
        Raises:
            ValueError: Intentional error for testing
        Returns:
            Nothing
        """
        raise ValueError("Intentional error for testing")

    function_name = f"{CATALOG}.{SCHEMA}.raise_error"
    uc_client.create_python_function(func=raise_error, catalog=CATALOG, schema=SCHEMA, replace=True)
    result = uc_client.execute_function(function_name=function_name, parameters={"a": 1, "b": 2})
    assert result.error is not None
    assert "Intentional error for testing" in result.error


@pytest.mark.asyncio
async def test_function_exception_local_python_function_async(uc_client):
    uc_client.execution_mode = "local"

    def raise_error(a: int, b: int) -> None:
        """
        Raises an exception for testing purposes.
        Args:
            a: an integer
            b: another integer
        Raises:
            ValueError: Intentional error for testing
        Returns:
            Nothing
        """
        raise ValueError("Intentional error for testing")

    function_name = f"{CATALOG}.{SCHEMA}.raise_error"
    uc_client.create_python_function(func=raise_error, catalog=CATALOG, schema=SCHEMA, replace=True)
    result = await uc_client.execute_function_async(
        function_name=function_name, parameters={"a": 1, "b": 2}
    )
    assert result.error is not None
    assert "Intentional error for testing" in result.error


@pytest.mark.asyncio
async def test_function_exception_sandbox_async(uc_client):
    uc_client.execution_mode = "sandbox"

    def raise_error_async(a: int, b: int) -> None:
        """
        Raises an exception for testing purposes.
        Args:
            a: an integer
            b: another integer
        Raises:
            ValueError: Intentional error for testing
        Returns:
            Nothing
        """
        raise ValueError("Async intentional error")

    function_name = f"{CATALOG}.{SCHEMA}.raise_error_async"
    uc_client.create_python_function(
        func=raise_error_async, catalog=CATALOG, schema=SCHEMA, replace=True
    )
    result = await uc_client.execute_function_async(
        function_name=function_name, parameters={"a": 1, "b": 2}
    )
    assert result.error is not None
    assert "Async intentional error" in result.error


def test_no_output_function_local(uc_client):
    uc_client.execution_mode = "local"

    def no_output(a: int, b: int) -> None:
        """
        A function that does not return anything.
        Args:
            a: an integer
            b: another integer
        Returns:
            None
        """
        a + b
        return None

    function_name = f"{CATALOG}.{SCHEMA}.no_output"
    uc_client.create_python_function(func=no_output, catalog=CATALOG, schema=SCHEMA, replace=True)
    result = uc_client.execute_function(function_name=function_name, parameters={"a": 5, "b": 10})
    assert "The function execution has completed, but no output was produced." in result.value


@pytest.mark.asyncio
async def test_no_output_function_sandbox(uc_client):
    uc_client.execution_mode = "sandbox"

    def no_output(a: int, b: int) -> None:
        """
        A function that does not return anything.
        Args:
            a: an integer
            b: another integer
        Returns:
            None
        """
        a + b
        return None

    function_name = f"{CATALOG}.{SCHEMA}.no_output"
    uc_client.create_python_function(func=no_output, catalog=CATALOG, schema=SCHEMA, replace=True)
    result = await uc_client.execute_function_async(
        function_name=function_name, parameters={"a": 5, "b": 10}
    )
    assert "The function execution has completed, but no output was produced." in result.value


@pytest_asyncio.fixture
async def offline_uc_client():
    uc_api_client = ApiClient(configuration=Configuration())
    client = UnitycatalogFunctionClient(api_client=uc_api_client, execution_mode="local")
    client.uc.functions_client = mock.AsyncMock(spec=FunctionsApi)
    yield client
    await client.close_async()
    await uc_api_client.close()


def constant_function_info(return_literal: str, data_type: str) -> FunctionInfo:
    return FunctionInfo(
        name="constant_func",
        catalog_name=CATALOG,
        schema_name=SCHEMA,
        full_name=f"{CATALOG}.{SCHEMA}.constant_func",
        data_type=data_type,
        full_data_type=data_type,
        routine_body="EXTERNAL",
        routine_definition=f"return {return_literal}",
        external_language="PYTHON",
    )


def render_letter(name: str) -> str:
    """
    Renders a letter.

    Args:
        name: the recipient
    """
    template = """Dear {name},
Thanks.
"""
    return template.format(name=name)


def render_rows(count: int) -> str:
    """
    Renders a row for every even index.

    Args:
        count: the number of indexes
    """
    rows = []
    for index in range(count):
        if index % 2 == 0:
            rows.append(
                f"""row {index}
    indented
"""
            )
    return "".join(rows)


def render_literals(name: str) -> str:
    """
    Renders raw, bytes and backslash-continued string literals.

    Args:
        name: the prefix
    """
    raw = r"""\d+
  raw"""
    data = b"""bytes
  line"""
    joined = "back\
  slash"
    return f"{name}|{raw}|{data.decode()}|{joined}"


# fmt: off
def render_signed(name: str) -> str:
    """
    Renders a signed note whose comment and continuation lines sit left of the code.

    Args:
        name: the signer
    """
    note = """Signed,
""" + \
  name
# the signature goes last
    return note
# fmt: on


def make_nested_letter():
    def render_nested_letter(name: str) -> str:
        """
        Renders a letter from a nested function.

        Args:
            name: the recipient
        """
        template = """Dear {name},
Thanks.
"""
        return template.format(name=name)

    return render_nested_letter


def store_created_functions(client: UnitycatalogFunctionClient) -> None:
    stored = {}

    async def create_function(request, **_):
        info = request.function_info
        full_name = f"{info.catalog_name}.{info.schema_name}.{info.name}"
        stored[full_name] = FunctionInfo.from_dict({**info.to_dict(), "full_name": full_name})
        return stored[full_name]

    async def get_function(name, **_):
        if name not in stored:
            raise NotFoundException(status=404, reason="Not Found")
        return stored[name]

    client.uc.functions_client.create_function.side_effect = create_function
    client.uc.functions_client.get_function.side_effect = get_function


MULTILINE_STRING_CASES = [
    (render_letter, {"name": "Bo"}, "Dear Bo,\nThanks.\n"),
    (render_rows, {"count": 3}, "row 0\n    indented\nrow 2\n    indented\n"),
    (render_literals, {"name": "Bo"}, "Bo|\\d+\n  raw|bytes\n  line|back  slash"),
    (render_signed, {"name": "Bo"}, "Signed,\nBo"),
    (make_nested_letter(), {"name": "Bo"}, "Dear Bo,\nThanks.\n"),
]


@pytest.mark.asyncio
@pytest.mark.parametrize(("func", "params", "expected"), MULTILINE_STRING_CASES)
async def test_multiline_string_literal_survives_create_and_execute(
    offline_uc_client, func, params, expected
):
    store_created_functions(offline_uc_client)
    offline_uc_client.create_python_function(func=func, catalog=CATALOG, schema=SCHEMA)

    result = offline_uc_client.execute_function(f"{CATALOG}.{SCHEMA}.{func.__name__}", params)

    assert (result.error, result.value) == (None, expected)


@pytest.mark.asyncio
@pytest.mark.parametrize(("func", "params", "expected"), MULTILINE_STRING_CASES)
async def test_multiline_string_literal_survives_get_function_as_callable(
    offline_uc_client, func, params, expected
):
    store_created_functions(offline_uc_client)
    offline_uc_client.create_python_function(func=func, catalog=CATALOG, schema=SCHEMA)

    fetched = offline_uc_client.get_function_as_callable(f"{CATALOG}.{SCHEMA}.{func.__name__}")

    assert fetched(**params) == expected


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("padding", "expected"),
    [
        ("  ", "Dear\n  \n    xBo"),
        ("    ", "Dear\n    \n    xBo"),
        ("        ", "Dear\n        \n    xBo"),
    ],
)
async def test_whitespace_only_line_inside_string_survives_create_and_execute(
    offline_uc_client, tmp_path, padding, expected
):
    module_path = tmp_path / "padded_note.py"
    module_path.write_text(
        "def render_padded(name: str) -> str:\n"
        '    """\n'
        "    Renders a note with a whitespace-only line.\n"
        "\n"
        "    Args:\n"
        "        name: the recipient\n"
        '    """\n'
        f'    return """Dear\n{padding}\n    x""" + name\n'
    )
    spec = importlib.util.spec_from_file_location("padded_note", module_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    store_created_functions(offline_uc_client)
    offline_uc_client.create_python_function(
        func=module.render_padded, catalog=CATALOG, schema=SCHEMA
    )

    result = offline_uc_client.execute_function(f"{CATALOG}.{SCHEMA}.render_padded", {"name": "Bo"})
    fetched = offline_uc_client.get_function_as_callable(f"{CATALOG}.{SCHEMA}.render_padded")

    assert (result.error, result.value, fetched(name="Bo")) == (None, expected, expected)


FALSY_RESULT_CASES = [
    ("False", "BOOLEAN", False),
    ("0", "INT", 0),
    ("0.0", "DOUBLE", 0.0),
    ("''", "STRING", ""),
    ("[]", "ARRAY", []),
    ("{}", "MAP", {}),
]


@pytest.mark.parametrize("return_literal,data_type,expected", FALSY_RESULT_CASES)
def test_execute_function_preserves_falsy_result(
    offline_uc_client, return_literal, data_type, expected
):
    offline_uc_client.uc.functions_client.get_function.return_value = constant_function_info(
        return_literal, data_type
    )
    result = offline_uc_client.execute_function(f"{CATALOG}.{SCHEMA}.constant_func")
    assert result.error is None
    assert result.value == expected
    assert type(result.value) is type(expected)


@pytest.mark.asyncio
@pytest.mark.parametrize("return_literal,data_type,expected", FALSY_RESULT_CASES)
async def test_execute_function_async_preserves_falsy_result(
    offline_uc_client, return_literal, data_type, expected
):
    offline_uc_client.uc.functions_client.get_function.return_value = constant_function_info(
        return_literal, data_type
    )
    result = await offline_uc_client.execute_function_async(f"{CATALOG}.{SCHEMA}.constant_func")
    assert result.error is None
    assert result.value == expected
    assert type(result.value) is type(expected)


@pytest.mark.asyncio
async def test_execute_function_async_local_none_result_returns_no_output_message(
    offline_uc_client,
):
    offline_uc_client.uc.functions_client.get_function.return_value = constant_function_info(
        "None", "STRING"
    )
    result = await offline_uc_client.execute_function_async(f"{CATALOG}.{SCHEMA}.constant_func")
    assert "The function execution has completed, but no output was produced." in result.value


def test_fetch_function_callable(uc_client):
    function_name = f"{CATALOG}.{SCHEMA}.simple_func"
    uc_client.create_python_function(func=simple_func, catalog=CATALOG, schema=SCHEMA, replace=True)

    callable_def = uc_client.get_function_as_callable(function_name)
    result = callable_def(5, 10)
    assert result == 15


def test_fetch_function_callable_namespace_scoped(uc_client):
    constant = 10

    def func_with_constant(a: int) -> int:
        """
        A function that adds a constant to an integer.
        Args:
            a: an integer
        Returns:
            The sum of a and the constant.
        """
        return a + constant

    assert func_with_constant(5) == 15

    function_name = f"{CATALOG}.{SCHEMA}.func_with_constant"
    uc_client.create_python_function(
        func=func_with_constant, catalog=CATALOG, schema=SCHEMA, replace=True
    )

    scoped_namespace = {"__builtins__": __builtins__, "constant": 5}

    callable_def = uc_client.get_function_as_callable(
        function_name=function_name, namespace=scoped_namespace
    )

    scoped_ns = SimpleNamespace(**scoped_namespace)

    assert scoped_ns.func_with_constant(5) == 10
    assert callable_def(5) == 10


def test_sql_function_with_null_default(uc_client: UnitycatalogFunctionClient):
    function_name = f"{CATALOG}.{SCHEMA}.null_default_func"

    uc_client.create_function(
        function_name=function_name,
        routine_definition=("return 'a is ' + ('NULL' if a is None else a) + ', b is ' + b"),
        data_type="STRING",
        full_data_type="STRING",
        comment="Tests NULL default parameter handling",
        parameters=[
            FunctionParameterInfo(
                name="a",
                type_name="STRING",
                type_text="string",
                type_json='{"name":"a","type":"string","nullable":true,"metadata":{}}',
                position=0,
                parameter_default="NULL",
            ),
            FunctionParameterInfo(
                name="b",
                type_name="STRING",
                type_text="string",
                type_json='{"name":"b","type":"string","nullable":true,"metadata":{}}',
                position=1,
                parameter_default="'non-null-default'",
            ),
        ],
        replace=True,
    )

    result = uc_client.execute_function(function_name=function_name, parameters={})
    assert result.value == "a is NULL, b is non-null-default"

    result = uc_client.execute_function(
        function_name=function_name, parameters={"a": "custom-value"}
    )
    assert result.value == "a is custom-value, b is non-null-default"


def weather_report() -> str:
    """
    Reports the weather.
    """
    return "sunny"


LARGE_ROUTINE_DEFINITION = "return 'sunny'\n#" + "x" * 1_000_000


def create_functions(client: UnitycatalogFunctionClient, schema: str) -> list[str]:
    client.uc.create_catalog(name=CATALOG)
    client.uc.create_schema(name=schema, catalog_name=CATALOG)
    client.create_python_function(func=weather_report, catalog=CATALOG, schema=schema)
    wrapped = client.create_wrapped_function(
        primary_func=weather_report, functions=[], catalog=CATALOG, schema=schema, replace=True
    )
    large = client.create_function(
        function_name=f"{CATALOG}.{schema}.large_report",
        routine_definition=LARGE_ROUTINE_DEFINITION,
        data_type="STRING",
        full_data_type="STRING",
        comment="Reports the weather at length.",
        parameters=[],
    )
    return [wrapped.full_name, large.full_name]


async def create_functions_async(client: UnitycatalogFunctionClient, schema: str) -> list[str]:
    await client.uc.create_catalog_async(name=CATALOG)
    await client.uc.create_schema_async(name=schema, catalog_name=CATALOG)
    await client.create_python_function_async(func=weather_report, catalog=CATALOG, schema=schema)
    wrapped = await client.create_wrapped_function_async(
        primary_func=weather_report, functions=[], catalog=CATALOG, schema=schema, replace=True
    )
    large = await client.create_function_async(
        function_name=f"{CATALOG}.{schema}.large_report",
        routine_definition=LARGE_ROUTINE_DEFINITION,
        data_type="STRING",
        full_data_type="STRING",
        comment="Reports the weather at length.",
        parameters=[],
    )
    return [wrapped.full_name, large.full_name]


def sync_calls(client: UnitycatalogFunctionClient, schema: str):
    names = create_functions(client, schema)
    result = (
        sorted(function.full_name for function in client.list_functions(CATALOG, schema)),
        [client.get_function(name).full_name for name in names],
        client.get_function(names[1]).routine_definition == LARGE_ROUTINE_DEFINITION,
        [client.execute_function(name).value for name in names],
    )
    for name in names:
        client.delete_function(name)
    return result


async def async_calls(client: UnitycatalogFunctionClient, schema: str):
    names = await create_functions_async(client, schema)
    functions = await client.list_functions_async(CATALOG, schema)
    result = (
        sorted(function.full_name for function in functions),
        [(await client.get_function_async(name)).full_name for name in names],
        (await client.get_function_async(names[1])).routine_definition == LARGE_ROUTINE_DEFINITION,
        [(await client.execute_function_async(name)).value for name in names],
    )
    for name in names:
        await client.delete_function_async(name)
    return result


@pytest.mark.asyncio
@pytest.mark.parametrize("threads", [1, 4])
@pytest.mark.parametrize("use_async", [False, True])
async def test_methods_work_from_other_threads_and_event_loops(uc_client, use_async, threads):
    uc_client.execution_mode = "local"
    schemas = [f"{SCHEMA}_thread_{i}" for i in range(threads)]

    def run(schema):
        if use_async:
            return asyncio.run(async_calls(uc_client, schema))
        return sync_calls(uc_client, schema)

    try:
        with ThreadPoolExecutor(max_workers=threads) as executor:
            results = list(executor.map(run, schemas))
    finally:
        for schema in schemas:
            with suppress(NotFoundException):
                await uc_client.uc.schemas_client.delete_schema(
                    full_name=f"{CATALOG}.{schema}", force=True
                )

    assert results == [
        (
            [f"{CATALOG}.{schema}.large_report", f"{CATALOG}.{schema}.weather_report"],
            [f"{CATALOG}.{schema}.weather_report", f"{CATALOG}.{schema}.large_report"],
            True,
            ["sunny", "sunny"],
        )
        for schema in schemas
    ]


def client_threads() -> set[threading.Thread]:
    return {thread for thread in threading.enumerate() if thread.name == "unitycatalog-client"}


def client_errors(caplog) -> list[str]:
    return [
        record.getMessage()
        for record in caplog.records
        if record.name == "unitycatalog.ai.core.client" and record.levelno >= logging.ERROR
    ]


async def eventually(condition: Callable[[], bool], message: str):
    deadline = time.monotonic() + 10
    while not condition():
        assert time.monotonic() < deadline, message
        await asyncio.sleep(0.01)


async def assert_session_closed(client: UnitycatalogFunctionClient):
    with pytest.raises(RuntimeError, match="Session is closed"):
        client.delete_function("catalog.schema.missing")
    with pytest.raises(RuntimeError, match="Session is closed"):
        await client.delete_function_async("catalog.schema.missing")


class NotFoundHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        body = b'{"error_code": "NOT_FOUND", "message": "Function not found"}'
        self.send_response(404)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    do_DELETE = do_GET

    def log_message(self, format, *args):
        pass


@pytest.fixture
def not_found_server():
    server = ThreadingHTTPServer(("127.0.0.1", 0), NotFoundHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{server.server_address[1]}/api/2.1/unity-catalog"
    server.shutdown()
    server.server_close()


class KeepAliveNotFoundHandler(NotFoundHandler):
    protocol_version = "HTTP/1.1"
    client_ports = []
    closed_ports = []

    def do_GET(self):
        KeepAliveNotFoundHandler.client_ports.append(self.client_address[1])
        super().do_GET()

    do_DELETE = do_GET

    def finish(self):
        super().finish()
        KeepAliveNotFoundHandler.closed_ports.append(self.client_address[1])


@pytest.fixture
def keep_alive_server():
    KeepAliveNotFoundHandler.client_ports = []
    KeepAliveNotFoundHandler.closed_ports = []
    server = ThreadingHTTPServer(("127.0.0.1", 0), KeepAliveNotFoundHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{server.server_address[1]}/api/2.1/unity-catalog"
    server.shutdown()
    server.server_close()


@pytest.fixture
def silent_server():
    """
    A server that accepts connections and never answers. Sets `accepted` on each one and keeps
    them in `connections`.
    """
    accepted = threading.Event()
    connections = []
    with closing(socket.create_server(("127.0.0.1", 0))) as listener:

        def accept():
            with suppress(OSError):
                while True:
                    connections.append(listener.accept()[0])
                    accepted.set()

        threading.Thread(target=accept, daemon=True).start()
        yield (
            f"http://127.0.0.1:{listener.getsockname()[1]}/api/2.1/unity-catalog",
            accepted,
            connections,
        )
    for connection in connections:
        connection.close()


def function_client(host: str) -> UnitycatalogFunctionClient:
    return UnitycatalogFunctionClient(api_client=ApiClient(configuration=Configuration(host=host)))


class CloseRecordingRESTClient(RESTClientObject):
    closed = []
    slow_close = False

    async def close(self):
        if self.slow_close:
            # Slow enough that a close that isn't waited for is still running when the client returns.
            await asyncio.sleep(0.5)
        await super().close()
        CloseRecordingRESTClient.closed.append(self)


@pytest.mark.asyncio
@pytest.mark.parametrize("close", ["close", "close_async", "ApiClient.close"])
async def test_close_releases_the_client_and_later_calls_raise(not_found_server, close, caplog):
    CloseRecordingRESTClient.closed = []
    threads_before = client_threads()
    api_client = ApiClient(configuration=Configuration(host=not_found_server))
    await api_client.rest_client.close()
    replaced_rest_client = CloseRecordingRESTClient(api_client.configuration)
    replaced_rest_client.slow_close = True
    api_client.rest_client = replaced_rest_client
    client = UnitycatalogFunctionClient(api_client=api_client)
    with pytest.raises(NotFoundException):
        client.delete_function("catalog.schema.missing")

    for _ in range(2):
        if close == "close":
            client.close()
        elif close == "close_async":
            await client.close_async()
        else:
            await api_client.close()

    assert len(CloseRecordingRESTClient.closed) == 2, "the original session was not closed"
    assert CloseRecordingRESTClient.closed.count(replaced_rest_client) == 1
    assert client_threads() == threads_before
    assert client_errors(caplog) == []
    await assert_session_closed(client)


@pytest.mark.asyncio
@pytest.mark.parametrize("used", [False, True], ids=["unused", "used"])
@pytest.mark.parametrize("owner", ["running", "stopped", "closed"])
async def test_close_closes_the_original_session_on_its_own_loop(
    keep_alive_server, owner, used, caplog
):
    loop = asyncio.new_event_loop()
    thread = threading.Thread(target=loop.run_forever, daemon=True)

    async def build_api_client():
        api_client = ApiClient(configuration=Configuration(host=keep_alive_server))
        if used:
            with pytest.raises(NotFoundException):
                await FunctionsApi(api_client).delete_function("catalog.schema.missing")
        session = getattr(api_client.rest_client, "pool_manager", None)
        # The client is built on the loop that owns the session, as the docs say to do.
        return session, UnitycatalogFunctionClient(api_client=api_client)

    try:
        if owner == "running":
            thread.start()
            session, client = asyncio.run_coroutine_threadsafe(build_api_client(), loop).result(30)
        else:
            session, client = await asyncio.to_thread(loop.run_until_complete, build_api_client())
            if owner == "closed":
                loop.close()
        assert session is not None

        await asyncio.wait_for(client.close_async(), 30)

        assert session.closed, "the original session was not closed"
        if used and owner == "running":
            await eventually(
                lambda: KeepAliveNotFoundHandler.closed_ports
                == KeepAliveNotFoundHandler.client_ports,
                "the connection opened before the client was created is still open",
            )
    finally:
        if thread.is_alive():
            loop.call_soon_threadsafe(loop.stop)
            thread.join(30)
        elif not loop.is_closed():
            await asyncio.to_thread(loop.run_until_complete, asyncio.sleep(0.1))
        loop.close()
    errors = client_errors(caplog)
    if used and owner == "stopped":
        assert len(errors) == 1
        assert errors[0].startswith("Error while closing ApiClient: ")
        assert "attached to a different loop" in errors[0]
    else:
        assert errors == []
    await assert_session_closed(client)


@pytest.mark.asyncio
async def test_close_does_not_run_tasks_waiting_on_a_stopped_loop(not_found_server):
    async def build_api_client():
        return ApiClient(configuration=Configuration(host=not_found_server))

    loop = asyncio.new_event_loop()
    api_client = await asyncio.to_thread(loop.run_until_complete, build_api_client())
    client = UnitycatalogFunctionClient(api_client=api_client)
    calls_started = threading.Event()

    async def call_client():
        calls_started.set()
        await client.delete_function_async("catalog.schema.missing")

    waiting = loop.create_task(call_client())
    try:
        closing = threading.Thread(target=client.close, daemon=True)
        closing.start()
        await asyncio.to_thread(closing.join, 10)
        assert not closing.is_alive(), "close hung"
        assert not calls_started.is_set()
    finally:
        if not loop.is_running():
            waiting.cancel()
            await asyncio.to_thread(
                loop.run_until_complete, asyncio.gather(waiting, return_exceptions=True)
            )
            loop.close()
    await assert_session_closed(client)


@pytest.mark.asyncio
@pytest.mark.parametrize("use_async", [False, True])
async def test_server_errors_reach_callers_on_other_threads(not_found_server, use_async):
    client = function_client(not_found_server)

    def delete_missing_function():
        if use_async:
            return asyncio.run(client.delete_function_async("catalog.schema.missing"))
        return client.delete_function("catalog.schema.missing")

    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [executor.submit(delete_missing_function) for _ in range(2)]
            for future in futures:
                with pytest.raises(NotFoundException):
                    future.result(timeout=30)
    finally:
        client.close()


def read_until_closed(connection: socket.socket):
    """Raises `TimeoutError` if the other side keeps the connection open."""
    with suppress(ConnectionResetError):
        while connection.recv(1024):
            pass


@pytest.mark.asyncio
async def test_cancelling_a_call_cancels_its_request(silent_server):
    host, accepted, connections = silent_server
    client = function_client(host)
    try:
        call = asyncio.ensure_future(client.get_function_async("catalog.schema.weather_report"))
        assert await asyncio.to_thread(accepted.wait, 30)
        connection = connections[0]
        connection.settimeout(10)
        assert (await asyncio.to_thread(connection.recv, 1024)).startswith(b"GET ")
        call.cancel()
        with pytest.raises(asyncio.CancelledError):
            await call

        await asyncio.to_thread(read_until_closed, connection)
    finally:
        await asyncio.wait_for(client.close_async(), 30)


@pytest.mark.asyncio
@pytest.mark.parametrize("use_async", [False, True])
async def test_close_fails_pending_requests_instead_of_hanging(silent_server, use_async):
    host, accepted, _ = silent_server
    threads_before = client_threads()
    client = function_client(host)

    with ThreadPoolExecutor(max_workers=2) as executor:
        pending = executor.submit(
            asyncio.run, client.get_function_async("catalog.schema.weather_report")
        )
        assert accepted.wait(timeout=30)
        # Wait without blocking this loop, which owns the client's session, so it can run the close.
        if use_async:
            closing = executor.submit(asyncio.run, client.close_async())
        else:
            closing = executor.submit(client.close)
        await asyncio.to_thread(closing.result, 30)

        with pytest.raises(RuntimeError, match="Session is closed"):
            await asyncio.to_thread(pending.result, 30)
    assert client_threads() == threads_before


class FailingCloseRESTClient(RESTClientObject):
    closes = 0

    async def close(self):
        FailingCloseRESTClient.closes += 1
        await super().close()
        raise OSError("close failed")


@pytest.mark.asyncio
async def test_a_failed_close_is_logged_and_leaves_the_client_closed(not_found_server, caplog):
    FailingCloseRESTClient.closes = 0
    threads_before = client_threads()
    api_client = ApiClient(configuration=Configuration(host=not_found_server))
    await api_client.rest_client.close()
    api_client.rest_client = FailingCloseRESTClient(api_client.configuration)
    client = UnitycatalogFunctionClient(api_client=api_client)
    with pytest.raises(NotFoundException):
        client.delete_function("catalog.schema.missing")
    assert len(client_threads() - threads_before) == 1

    client.close()
    client.close()

    await eventually(lambda: len(client_errors(caplog)) == 2, "a failed close was not logged")
    assert sorted(client_errors(caplog)) == [
        "Error while closing ApiClient: close failed",
        "Error while closing the ApiClient's original session: close failed",
    ]
    assert FailingCloseRESTClient.closes == 2
    assert client_threads() == threads_before
    await assert_session_closed(client)


EXIT_WITHOUT_CLOSING = """
import asyncio, sys
from unitycatalog.ai.core.client import UnitycatalogFunctionClient
from unitycatalog.client import ApiClient, Configuration
from unitycatalog.client.exceptions import NotFoundException


async def build():
    api_client = ApiClient(configuration=Configuration(host=sys.argv[1]))
    return UnitycatalogFunctionClient(api_client=api_client, execution_mode="local")


client = asyncio.run(build())
try:
    client.delete_function("catalog.schema.missing")
except NotFoundException:
    print("not found")
"""


def test_a_client_that_is_not_closed_closes_at_exit(not_found_server):
    result = subprocess.run(
        [sys.executable, "-c", EXIT_WITHOUT_CLOSING, not_found_server],
        env={**os.environ, "PYTHONPATH": os.pathsep.join(sys.path)},
        capture_output=True,
        text=True,
        timeout=60,
    )

    assert (result.returncode, result.stdout.strip()) == (0, "not found")
    assert "Error while closing" not in result.stderr
    assert "Unclosed client session" not in result.stderr


@pytest.mark.asyncio
async def test_client_starts_its_thread_on_the_first_request_and_shares_it(not_found_server):
    threads_before = client_threads()
    api_client = ApiClient(configuration=Configuration(host=not_found_server))
    first = UnitycatalogFunctionClient(api_client=api_client)
    second = UnitycatalogFunctionClient(api_client=api_client)
    assert client_threads() == threads_before

    for client in [first, second]:
        with pytest.raises(NotFoundException):
            client.delete_function("catalog.schema.missing")
    assert len(client_threads() - threads_before) == 1

    second.close()
    assert client_threads() == threads_before
    await assert_session_closed(first)


@pytest.mark.asyncio
async def test_close_async_does_not_wait_for_a_busy_default_executor(silent_server):
    host, accepted, _ = silent_server
    client = function_client(host)
    loop = asyncio.get_running_loop()
    loop.set_default_executor(ThreadPoolExecutor(max_workers=1))

    pending = loop.run_in_executor(None, client.get_function, "catalog.schema.weather_report")
    assert accepted.wait(timeout=30)
    await asyncio.wait_for(client.close_async(), timeout=30)

    with pytest.raises(RuntimeError, match="Session is closed"):
        await asyncio.wait_for(pending, timeout=30)


class ServerErrorHandler(BaseHTTPRequestHandler):
    requests = 0

    def do_GET(self):
        type(self).requests += 1
        self.send_response(500)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def log_message(self, format, *args):
        pass


@pytest.mark.asyncio
async def test_close_cancels_a_request_waiting_to_retry():
    ServerErrorHandler.requests = 0
    server = ThreadingHTTPServer(("127.0.0.1", 0), ServerErrorHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    host = f"http://127.0.0.1:{server.server_address[1]}/api/2.1/unity-catalog"
    # The generated client backs off 0.1s, 0.2s, 0.4s and so on, so after the sixth attempt the
    # next one is 3.2s away.
    client = UnitycatalogFunctionClient(
        api_client=ApiClient(configuration=Configuration(host=host, retries=10))
    )
    try:
        with ThreadPoolExecutor(max_workers=1) as executor:
            pending = executor.submit(client.get_function, "catalog.schema.weather_report")
            deadline = time.monotonic() + 30
            while ServerErrorHandler.requests < 6 and time.monotonic() < deadline:
                await asyncio.sleep(0.01)
            assert ServerErrorHandler.requests == 6

            await asyncio.wait_for(client.close_async(), timeout=30)
            with pytest.raises(RuntimeError, match="Session is closed"):
                pending.result(timeout=1)
            await asyncio.sleep(3.5)
        assert ServerErrorHandler.requests == 6
    finally:
        server.shutdown()
        server.server_close()


class RequestRecordingRESTClient(RESTClientObject):
    methods = []

    async def request(self, method, *args, **kwargs):
        RequestRecordingRESTClient.methods.append(method)
        return await super().request(method, *args, **kwargs)


@pytest.mark.asyncio
async def test_requests_are_sent_with_the_rest_client_class_of_the_api_client(not_found_server):
    RequestRecordingRESTClient.methods = []
    api_client = ApiClient(configuration=Configuration(host=not_found_server))
    await api_client.rest_client.close()
    api_client.rest_client = RequestRecordingRESTClient(api_client.configuration)
    client = UnitycatalogFunctionClient(api_client=api_client)
    try:
        with pytest.raises(NotFoundException):
            await asyncio.to_thread(client.delete_function, "catalog.schema.missing")
        with pytest.raises(NotFoundException):
            await client.uc.functions_client.get_function(name="catalog.schema.missing")
    finally:
        await client.close_async()

    assert RequestRecordingRESTClient.methods == ["DELETE", "GET"]


@pytest.mark.asyncio
@pytest.mark.skipif(not hasattr(os, "fork"), reason="needs os.fork")
@pytest.mark.filterwarnings("ignore:This process .* is multi-threaded:DeprecationWarning")
async def test_a_forked_child_cant_use_the_client_and_the_parent_keeps_working(keep_alive_server):
    client = function_client(keep_alive_server)
    with pytest.raises(NotFoundException):
        await client.delete_function_async("catalog.schema.missing")

    status = (0, 0)
    pid = os.fork()
    if pid == 0:
        try:
            with ThreadPoolExecutor(max_workers=1) as executor:
                error = executor.submit(client.delete_function, "catalog.schema.missing").exception(
                    timeout=10
                )
            client.close()
            expected = (
                "The ApiClient was set up in another process. "
                "Create a new ApiClient in the child process."
            )
            os._exit(0 if isinstance(error, RuntimeError) and str(error) == expected else 1)
        finally:
            os._exit(1)
    try:
        deadline = time.monotonic() + 30
        while (status := os.waitpid(pid, os.WNOHANG))[0] == 0 and time.monotonic() < deadline:
            time.sleep(0.05)
        assert status[0] == pid, "the child hung"
        assert os.waitstatus_to_exitcode(status[1]) == 0
        with pytest.raises(NotFoundException):
            await asyncio.wait_for(
                client.delete_function_async("catalog.schema.missing"), timeout=10
            )
        port = KeepAliveNotFoundHandler.client_ports[0]
        assert KeepAliveNotFoundHandler.client_ports == [port, port]
    finally:
        if status[0] == 0:
            os.kill(pid, signal.SIGKILL)
            os.waitpid(pid, 0)
        await client.close_async()


def slow_closing_client(host: str, closing_started: threading.Event, release: threading.Event):
    """A client whose own session takes until `release` is set to close."""
    api_client = ApiClient(configuration=Configuration(host=host))

    class SlowClosingRESTClient(RESTClientObject):
        async def close(self):
            if self is not replaced_rest_client:
                closing_started.set()
                await asyncio.to_thread(release.wait, 30)
            await super().close()

    replaced_rest_client = SlowClosingRESTClient(api_client.configuration)
    api_client.rest_client, original = replaced_rest_client, api_client.rest_client
    return api_client, original


@pytest.mark.asyncio
async def test_cancelling_close_async_still_closes_the_client(not_found_server):
    closing_started = threading.Event()
    release = threading.Event()
    api_client, original = slow_closing_client(not_found_server, closing_started, release)
    await original.close()
    threads_before = client_threads()
    client = UnitycatalogFunctionClient(api_client=api_client)
    with pytest.raises(NotFoundException):
        await client.delete_function_async("catalog.schema.missing")
    shutdown_threads = client_threads() - threads_before
    thread_errors = []
    hook = threading.excepthook
    threading.excepthook = thread_errors.append
    try:
        close = asyncio.ensure_future(client.close_async())
        assert await asyncio.to_thread(closing_started.wait, 30)
        shutdown_threads |= {
            thread for thread in threading.enumerate() if thread.name == "unitycatalog-client-close"
        }
        close.cancel()
        with pytest.raises(asyncio.CancelledError):
            await close
    finally:
        release.set()
        for thread in shutdown_threads:
            await asyncio.to_thread(thread.join, 30)
        threading.excepthook = hook
    assert len(shutdown_threads) == 2
    assert not any(thread.is_alive() for thread in shutdown_threads), (
        "client shutdown did not finish"
    )
    assert thread_errors == []
    await assert_session_closed(client)


@pytest.mark.asyncio
async def test_a_second_close_waits_for_the_first_to_finish(not_found_server):
    closing_started = threading.Event()
    release = threading.Event()
    api_client, original = slow_closing_client(not_found_server, closing_started, release)
    await original.close()
    first = UnitycatalogFunctionClient(api_client=api_client)
    second = UnitycatalogFunctionClient(api_client=api_client)
    with pytest.raises(NotFoundException):
        await first.delete_function_async("catalog.schema.missing")
    try:
        first_close = asyncio.ensure_future(asyncio.to_thread(first.close))
        assert await asyncio.to_thread(closing_started.wait, 30)
        second_close = asyncio.ensure_future(asyncio.to_thread(second.close))
        await asyncio.sleep(0.5)
        assert not second_close.done()
    finally:
        release.set()
    await asyncio.wait_for(asyncio.gather(first_close, second_close), timeout=30)


@contextmanager
def client_thread_fails_to_start():
    start = threading.Thread.start

    def failing_start(thread):
        if thread.name == "unitycatalog-client":
            raise RuntimeError("can't start new thread")
        start(thread)

    with mock.patch.object(threading.Thread, "start", failing_start):
        yield


@pytest.mark.asyncio
async def test_a_request_after_a_failed_thread_start_starts_the_thread(not_found_server):
    client = function_client(not_found_server)
    try:
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            with client_thread_fails_to_start():
                with pytest.raises(RuntimeError, match="can't start new thread"):
                    await client.delete_function_async("catalog.schema.missing")
            gc.collect()
        assert [
            str(warning.message) for warning in caught if "never awaited" in str(warning.message)
        ] == []
        with pytest.raises(NotFoundException):
            await asyncio.wait_for(client.delete_function_async("catalog.schema.missing"), 10)
    finally:
        await asyncio.wait_for(client.close_async(), 10)


@pytest.mark.asyncio
@pytest.mark.parametrize("same_client", [True, False], ids=["same client", "another client"])
async def test_a_close_after_a_failed_thread_start_closes_the_client(
    not_found_server, same_client, caplog
):
    threads_before = client_threads()
    api_client = ApiClient(configuration=Configuration(host=not_found_server))
    first = UnitycatalogFunctionClient(api_client=api_client)
    second = first if same_client else UnitycatalogFunctionClient(api_client=api_client)
    with client_thread_fails_to_start():
        first.close()
    assert client_errors(caplog) == ["Error while closing ApiClient: can't start new thread"]
    with pytest.raises(NotFoundException):
        await asyncio.wait_for(first.delete_function_async("catalog.schema.missing"), 10)

    closing = threading.Thread(target=second.close, daemon=True)
    closing.start()
    await asyncio.to_thread(closing.join, 10)
    assert not closing.is_alive(), "the second close hung"
    assert client_threads() == threads_before
    await assert_session_closed(second)


@pytest.mark.asyncio
async def test_a_request_during_a_close_fails_without_blocking_the_loop(not_found_server):
    closing_started = threading.Event()
    release = threading.Event()
    replaced = None

    class SlowClosingRESTClient(RESTClientObject):
        async def close(self):
            if self is not replaced:
                closing_started.set()
                await asyncio.to_thread(release.wait, 30)
            await super().close()

    api_client = ApiClient(configuration=Configuration(host=not_found_server))
    await api_client.rest_client.close()
    replaced = SlowClosingRESTClient(api_client.configuration)
    api_client.rest_client = replaced
    client = UnitycatalogFunctionClient(api_client=api_client)
    with pytest.raises(NotFoundException):
        await client.delete_function_async("catalog.schema.missing")

    closer = threading.Thread(target=client.close, daemon=True)
    closer.start()
    try:
        assert await asyncio.to_thread(closing_started.wait, 30)
        started = time.monotonic()
        # Run the request on a thread of its own, so a blocked loop fails the test instead of hanging it.
        executor = ThreadPoolExecutor(max_workers=1)
        request = executor.submit(
            asyncio.run, client.delete_function_async("catalog.schema.missing")
        )
        try:
            with pytest.raises(RuntimeError, match="Session is closed"):
                await asyncio.to_thread(request.result, 10)
        finally:
            executor.shutdown(wait=False)
        assert time.monotonic() - started < 2
    finally:
        release.set()
        closer.join(30)
    assert not closer.is_alive()


@pytest.mark.asyncio
async def test_closing_from_a_request_on_the_clients_own_loop_fails_instead_of_hanging(
    not_found_server, caplog
):
    class CloseFromRequestRESTClient(RESTClientObject):
        async def request(self, *args, **kwargs):
            client.close()
            return await super().request(*args, **kwargs)

    api_client = ApiClient(configuration=Configuration(host=not_found_server))
    await api_client.rest_client.close()
    api_client.rest_client = CloseFromRequestRESTClient(api_client.configuration)
    client = UnitycatalogFunctionClient(api_client=api_client)
    try:
        with pytest.raises(NotFoundException):
            await asyncio.wait_for(client.delete_function_async("catalog.schema.missing"), 10)
        errors = client_errors(caplog)
        assert any("own loop thread" in error for error in errors), errors
    finally:
        closer = threading.Thread(target=client.close, daemon=True)
        closer.start()
        await asyncio.to_thread(closer.join, 10)
        assert not closer.is_alive(), "the client did not close"


@pytest.mark.asyncio
async def test_clients_built_at_the_same_time_on_one_api_client_install_one_rest_client():
    api_client = ApiClient(
        configuration=Configuration(host="http://localhost:8080/api/2.1/unity-catalog")
    )
    installs = []
    original_init = _SingleLoopRESTClient.__init__

    def slow_init(self, api_client):
        installs.append(self)
        time.sleep(0.2)
        original_init(self, api_client)

    def build():
        return UnitycatalogClient(api_client)

    with mock.patch.object(_SingleLoopRESTClient, "__init__", slow_init):
        threads = [threading.Thread(target=build) for _ in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(30)
    try:
        assert len(installs) == 1
        assert isinstance(api_client.rest_client, _SingleLoopRESTClient)
    finally:
        api_client.rest_client.shutdown()


def test_a_replaced_session_closed_from_its_own_loop_is_not_dropped(not_found_server):
    holder = {}

    async def use_and_close_on_the_same_loop():
        api_client = ApiClient(configuration=Configuration(host=not_found_server))
        response = await api_client.rest_client.request(
            "GET", f"{not_found_server}/functions/missing", headers={}
        )
        await response.read()
        holder["session"] = api_client.rest_client.pool_manager
        # Shutdown runs on the loop that owns the session, so the session is closed by the time
        # close returns.
        UnitycatalogFunctionClient(api_client=api_client).close()

    asyncio.run(use_and_close_on_the_same_loop())
    assert holder["session"].closed, "the original session was dropped"


def test_a_close_while_the_owning_loop_is_blocked_returns_and_is_not_dropped(
    keep_alive_server, caplog
):
    loop = asyncio.new_event_loop()
    owner_thread = threading.Thread(target=loop.run_forever, daemon=True)
    owner_thread.start()

    async def build_used_client():
        api_client = ApiClient(configuration=Configuration(host=keep_alive_server))
        with pytest.raises(NotFoundException):
            await FunctionsApi(api_client).delete_function("catalog.schema.missing")
        replaced = api_client.rest_client
        return replaced, UnitycatalogFunctionClient(api_client=api_client)

    replaced, client = asyncio.run_coroutine_threadsafe(build_used_client(), loop).result(30)
    closed = threading.Event()
    close_seconds = []

    def close_from_another_thread():
        started = time.monotonic()
        client.close()
        close_seconds.append(time.monotonic() - started)
        closed.set()

    worker = threading.Thread(target=close_from_another_thread, daemon=True)

    async def block_the_loop_until_closed():
        worker.start()
        closed.wait(30)  # blocks the loop that owns the session, on purpose

    try:
        asyncio.run_coroutine_threadsafe(block_the_loop_until_closed(), loop).result(60)
        assert closed.is_set(), "close did not return while the owning loop was blocked"
        assert close_seconds[0] < 10, f"close waited {close_seconds[0]:.1f}s for the blocked loop"
    finally:
        loop.call_soon_threadsafe(loop.stop)
        owner_thread.join(30)
    session = replaced.pool_manager
    assert session.closed or client_errors(caplog), "the original session was dropped"
