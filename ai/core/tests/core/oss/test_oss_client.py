import asyncio
import datetime
import decimal
import logging
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Dict, List

import pytest
from pydantic import ValidationError

from unitycatalog.ai.core.oss import (
    UnitycatalogFunctionClient,
    validate_input_parameter,
    validate_param,
)
from unitycatalog.ai.test_utils.function_utils import CATALOG, random_func_name
from unitycatalog.client import (
    ApiClient,
    CatalogsApi,
    Configuration,
    CreateCatalog,
    CreateSchema,
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
        comment="test"
    )


@pytest.fixture
def uc_client() -> UnitycatalogFunctionClient:
    config = Configuration()
    config.host = "http://localhost:8080/api/2.1/unity-catalog"
    uc_api_client = ApiClient(configuration=config)
    catalog_api = CatalogsApi(api_client=uc_api_client)
    schema_api = SchemasApi(api_client=uc_api_client)

    async def setup_catalog_and_schema():
        try:
            await catalog_api.get_catalog(name=CATALOG)
        except Exception:
            create_catalog = CreateCatalog(name=CATALOG, comment="")
            await catalog_api.create_catalog(create_catalog=create_catalog)

        try:
            await schema_api.get_schema(f"{CATALOG}.{SCHEMA}")
        except Exception:
            create_schema = CreateSchema(name=SCHEMA, catalog_name=CATALOG, comment="")
            await schema_api.create_schema(create_schema=create_schema)

    asyncio.run(setup_catalog_and_schema())

    return UnitycatalogFunctionClient(uc=uc_api_client)


def test_create_function(uc_client):
    function_name = f"{CATALOG}.{SCHEMA}.test_function"
    routine_definition = "return str(x)"
    data_type = "STRING"
    full_data_type = "STRING"
    comment="test"
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


def test_list_functions(uc_client: UnitycatalogFunctionClient):
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


# command to start UC server: bin/start-uc-server
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
def test_function_creation_and_execution(
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


def test_create_function_invalid_data_type(uc_client):
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


def test_get_nonexistent_function(uc_client):
    function_name = f"{CATALOG}.{SCHEMA}.nonexistent_function"

    with pytest.raises(ServiceException, match="(500)"):
        uc_client.get_function(function_name=function_name)


def test_delete_nonexistent_function(uc_client):
    function_name = f"{CATALOG}.{SCHEMA}.nonexistent_function"

    with pytest.raises(NotFoundException):
        uc_client.delete_function(function_name=function_name)


def test_execute_function_with_error(uc_client):
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



def test_validate_input_parameter_invalid():
    invalid_parameter = {
        "type_text": "string",
        # Missing 'name' field
        "type_json": '{"name":"x","type":"string","nullable":false,"metadata":{}}',
        "type_name": "STRING",
        "position": 0,
    }

    with pytest.raises(ValidationError, match="1 validation error for FunctionParameterInfo"):
        validate_input_parameter(invalid_parameter)


def test_validate_param_invalid_interval():
    param = datetime.timedelta(days=1)
    column_type = "INTERVAL"
    param_type_text = "interval year to month"  # Invalid for timedelta

    with pytest.raises(ValueError, match="Invalid interval type text"):
        validate_param(param, column_type, param_type_text)


def test_create_function_long_name(uc_client):
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


def test_function_caching(uc_client):
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

    # Execute the function multiple times
    result1 = uc_client.execute_function(function_name=function_name, parameters={"x": 2})
    result2 = uc_client.execute_function(function_name=function_name, parameters={"x": 3})

    assert result1.value == "4"
    assert result2.value == "6"

    # Check if the function is cached
    assert function_name.split(".")[-1] in uc_client.func_cache


def test_to_dict(uc_client):
    client_dict = uc_client.to_dict()
    assert isinstance(client_dict, dict)
    if "uc" in client_dict:
        assert isinstance(client_dict["uc"], FunctionsApi)
