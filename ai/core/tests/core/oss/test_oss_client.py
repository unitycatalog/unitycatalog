import datetime
import decimal
import json
import logging
import re
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Dict, List, Union

import pytest
import pytest_asyncio
from pydantic import ValidationError

from unitycatalog.ai.core.client import (
    UnitycatalogClient,
    UnitycatalogFunctionClient,
    validate_input_parameter,
    validate_param,
)
from unitycatalog.ai.test_utils.function_utils import CATALOG, random_func_name
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
            expected_result=f"{2**63-1}",
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
