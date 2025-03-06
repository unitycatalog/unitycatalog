import datetime
import decimal
import json
import logging
import re
import textwrap
from contextlib import contextmanager
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any, Dict, List, Optional, Union

import pytest
import pytest_asyncio
from pydantic import ValidationError

from unitycatalog.ai.core.client import (
    UnitycatalogClient,
    UnitycatalogFunctionClient,
    validate_input_parameter,
    validate_param,
)
from unitycatalog.ai.core.executor.local import run_in_sandbox, run_in_sandbox_async
from unitycatalog.ai.core.types import Variant
from unitycatalog.ai.core.utils.execution_utils import load_function_from_string
from unitycatalog.ai.test_utils.function_utils import (
    CATALOG,
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
    FunctionParameterInfo,
    FunctionsApi,
    SchemasApi,
)
from unitycatalog.client.exceptions import NotFoundException, ServiceException

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
    assert uc_client.func_cache.get("test_function") is not None
    uc_client.clear_function_cache()
    assert uc_client.func_cache.get("test_function") is None


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
            expected_result="3",
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
            expected_result="6",
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
            expected_result="True",
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
            expected_result="127",
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
            expected_result="32767",
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
            expected_result=f"{2**63 - 1}",
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
            expected_result="1",
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

    with pytest.raises(ServiceException, match="(500)"):
        uc_client.get_function(function_name=function_name)


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

    assert result1.value == "4"
    assert result2.value == "6"

    assert function_name.split(".")[-1] in uc_client.func_cache


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
    assert result1.value == "4"

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
    assert result2.value == "6"


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
    assert result.value == "15"
    assert result.format == "SCALAR"


@pytest.mark.asyncio
async def test_local_function_execution_async(uc_client: UnitycatalogFunctionClient):
    uc_client.execution_mode = "local"
    function_name = f"{CATALOG}.{SCHEMA}.simple_func"
    uc_client.create_python_function(func=simple_func, catalog=CATALOG, schema=SCHEMA, replace=True)

    result = await uc_client.execute_function_async(
        function_name=function_name, parameters={"a": 5, "b": 10}
    )
    assert result.value == "15"
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
