import datetime
import decimal
import logging
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Dict, List

import pytest

from unitycatalog import Unitycatalog
from unitycatalog.ai.core.oss import UnitycatalogFunctionClient
from unitycatalog.ai.test_utils.function_utils import CATALOG, random_func_name
from unitycatalog.types.function_create_params import (
    FunctionInfoInputParamsParameter,
)

SCHEMA = "uc_test"
_logger = logging.getLogger(__name__)


@pytest.fixture
def uc_client() -> UnitycatalogFunctionClient:
    uc = Unitycatalog()
    try:
        uc.catalogs.retrieve(CATALOG)
    except Exception:
        uc.catalogs.create(name=CATALOG)
    try:
        uc.schemas.retrieve(full_name=f"{CATALOG}.{SCHEMA}")
    except Exception as e:
        uc.schemas.create(catalog_name=CATALOG, name=SCHEMA)
    return UnitycatalogFunctionClient(uc=uc)


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
    input_params: List[FunctionInfoInputParamsParameter]
    data_type: str
    routine_definition: str
    input_data: Dict[str, Any]
    expected_result: str


def simple_function_obj():
    return FunctionObj(
        input_params=[
            FunctionInfoInputParamsParameter(
                name="x",
                type_name="STRING",
                type_text="string",
            )
        ],
        data_type="STRING",
        routine_definition="return x",
        input_data={"x": "test"},
        expected_result="test",
    )


# command to start UC server: bin/start-uc-server
@pytest.mark.parametrize(
    "function_object",
    [
        simple_function_obj(),
        FunctionObj(
            input_params=[
                FunctionInfoInputParamsParameter(
                    name="x",
                    type_name="INT",
                    type_text="int",
                ),
                FunctionInfoInputParamsParameter(
                    name="y",
                    type_name="INT",
                    type_text="int",
                ),
            ],
            data_type="INT",
            routine_definition="return x+y",
            input_data={"x": 1, "y": 2},
            expected_result="3",
        ),
        FunctionObj(
            input_params=[
                FunctionInfoInputParamsParameter(
                    name="x",
                    type_name="ARRAY",
                    type_text="array<float>",
                )
            ],
            data_type="DOUBLE",
            routine_definition="return sum(x)",
            input_data={"x": (1, 2, 3)},
            expected_result="6",
        ),
        FunctionObj(
            input_params=[
                FunctionInfoInputParamsParameter(
                    name="x",
                    type_name="BOOLEAN",
                    type_text="boolean",
                )
            ],
            data_type="BOOLEAN",
            routine_definition="return x",
            input_data={"x": True},
            expected_result="True",
        ),
        FunctionObj(
            input_params=[
                FunctionInfoInputParamsParameter(
                    name="x",
                    type_name="BINARY",
                    type_text="binary",
                )
            ],
            data_type="STRING",
            routine_definition="return x.decode('utf-8')",
            input_data={"x": b"Hello"},
            expected_result="Hello",
        ),
        FunctionObj(
            input_params=[
                FunctionInfoInputParamsParameter(
                    name="x",
                    type_name="BYTE",
                    type_text="byte",
                )
            ],
            data_type="BYTE",
            routine_definition="return x",
            input_data={"x": 127},
            expected_result="127",
        ),
        FunctionObj(
            input_params=[
                FunctionInfoInputParamsParameter(
                    name="x",
                    type_name="SHORT",
                    type_text="short",
                )
            ],
            data_type="SHORT",
            routine_definition="return x",
            input_data={"x": 32767},
            expected_result="32767",
        ),
        FunctionObj(
            input_params=[
                FunctionInfoInputParamsParameter(
                    name="x",
                    type_name="LONG",
                    type_text="long",
                )
            ],
            data_type="LONG",
            routine_definition="return x",
            input_data={"x": 2**63 - 1},
            expected_result=f"{2**63-1}",
        ),
        FunctionObj(
            input_params=[
                FunctionInfoInputParamsParameter(
                    name="x",
                    type_name="DATE",
                    type_text="date",
                )
            ],
            data_type="STRING",
            routine_definition="return x.isoformat()",
            input_data={"x": datetime.date(2024, 10, 11)},
            expected_result="2024-10-11",
        ),
        FunctionObj(
            input_params=[
                FunctionInfoInputParamsParameter(
                    name="x",
                    type_name="TIMESTAMP",
                    type_text="timestamp",
                )
            ],
            data_type="STRING",
            routine_definition="return x.isoformat()",
            input_data={"x": datetime.datetime(2024, 10, 11, 11, 2, 3)},
            expected_result="2024-10-11T11:02:03",
        ),
        FunctionObj(
            input_params=[
                FunctionInfoInputParamsParameter(
                    name="x",
                    type_name="TIMESTAMP_NTZ",
                    type_text="timestamp_ntz",
                )
            ],
            data_type="STRING",
            routine_definition="return x.isoformat()",
            input_data={"x": datetime.datetime(2024, 10, 11, 11, 2, 3)},
            expected_result="2024-10-11T11:02:03",
        ),
        FunctionObj(
            input_params=[
                FunctionInfoInputParamsParameter(
                    name="x",
                    type_name="DECIMAL",
                    type_text="decimal",
                )
            ],
            data_type="STRING",
            routine_definition="return str(x)",
            input_data={"x": decimal.Decimal(123)},
            expected_result="123",
        ),
        FunctionObj(
            input_params=[
                FunctionInfoInputParamsParameter(
                    name="x",
                    type_name="INTERVAL",
                    type_text="interval day to second",
                )
            ],
            data_type="INTERVAL",
            routine_definition="import datetime\nreturn (datetime.datetime(2024, 8, 19) - x).isoformat()",
            input_data={
                "x": datetime.timedelta(
                    days=0, hours=0, minutes=16, seconds=40, microseconds=123456
                )
            },
            expected_result="2024-08-18T23:43:19.876544",
        ),
        FunctionObj(
            input_params=[
                FunctionInfoInputParamsParameter(
                    name="x",
                    type_name="STRUCT",
                    type_text="struct<a:string,b:int>",
                )
            ],
            data_type="STRING",
            routine_definition="return x['a']+str(x['b'])",
            input_data={"x": {"a": "value", "b": 123}},
            expected_result="value123",
        ),
        FunctionObj(
            input_params=[
                FunctionInfoInputParamsParameter(
                    name="x",
                    type_name="MAP",
                    type_text="map<string, int>",
                )
            ],
            data_type="INT",
            routine_definition="return x['key1']",
            input_data={"x": {"key1": 1, "key2": 2}},
            expected_result="1",
        ),
        FunctionObj(
            input_params=[
                FunctionInfoInputParamsParameter(
                    name="x",
                    type_name="CHAR",
                    type_text="char",
                )
            ],
            data_type="CHAR",
            routine_definition="return x",
            input_data={"x": "A"},
            expected_result="A",
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
            parameters=function_object.input_params,
        )
        result = uc_client.execute_function(
            function_name=function_name, parameters=function_object.input_data
        )
        assert result.value == function_object.expected_result


def test_list_functions(uc_client: UnitycatalogFunctionClient):
    function_infos = uc_client.list_functions(catalog=CATALOG, schema=SCHEMA)
    func_obj = simple_function_obj()
    with generate_func_name_and_cleanup(uc_client) as func_name:
        create_func_info = uc_client.create_function(
            function_name=func_name,
            routine_definition=func_obj.routine_definition,
            data_type=func_obj.data_type,
            parameters=func_obj.input_params,
        )
        function_info = uc_client.get_function(func_name)
        assert create_func_info == function_info

        function_infos = uc_client.list_functions(catalog=CATALOG, schema=SCHEMA)
        assert len([f for f in function_infos if f.full_name == func_name]) == 1

        with generate_func_name_and_cleanup(uc_client) as func_name_2:
            uc_client.create_function(
                function_name=func_name_2,
                routine_definition=func_obj.routine_definition,
                data_type=func_obj.data_type,
                parameters=func_obj.input_params,
            )
            function_infos = uc_client.list_functions(catalog=CATALOG, schema=SCHEMA, max_results=1)
            assert len(function_infos) == 1
            function_info = function_infos[0]
            function_infos = uc_client.list_functions(
                catalog=CATALOG, schema=SCHEMA, max_results=1, page_token=function_infos.token
            )
            assert len(function_infos) == 1
            assert function_infos[0] != function_info
