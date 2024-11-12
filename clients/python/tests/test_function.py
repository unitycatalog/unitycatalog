import pytest

import subprocess

from unitycatalog.client import (
    CreateFunction,
    CreateFunctionRequest,
    ColumnTypeName,
    FunctionParameterInfos,
    FunctionParameterInfo,
)


@pytest.mark.asyncio
async def test_function_list(functions_api):
    api_response = await functions_api.list_functions("unity", "default")
    function_names = {f.name for f in api_response.functions}

    assert function_names == {"lowercase", "sum"}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "function_name,function_def",
    [
        ("sum", "t = x + y + z\\nreturn t"),
        ("lowercase", "g = s.lower()\\nreturn g"),
    ],
)
async def test_function_get(functions_api, function_name, function_def):
    function_info = await functions_api.get_function(f"unity.default.{function_name}")

    assert function_info.name == function_name
    assert function_info.catalog_name == "unity"
    assert function_info.schema_name == "default"
    assert function_info.external_language == "python"
    assert function_info.routine_definition == function_def


@pytest.mark.asyncio
async def test_function_create(functions_api):
    function_info = await functions_api.create_function(
        create_function_request=CreateFunctionRequest(
            function_info=CreateFunction(
                name="myFunction",
                catalog_name="unity",
                schema_name="default",
                input_params=FunctionParameterInfos(
                    parameters=[
                        FunctionParameterInfo(
                            name="a",
                            type_text="int",
                            type_name=ColumnTypeName.INT,
                            type_json='{"name":"a","type":"integer"}',
                            position=0,
                        ),
                        FunctionParameterInfo(
                            name="b",
                            type_text="int",
                            type_name=ColumnTypeName.INT,
                            type_json='{"name":"b","type":"integer"}',
                            position=1,
                        ),
                    ]
                ),
                data_type=ColumnTypeName.INT,
                full_data_type="int",
                routine_body="EXTERNAL",
                routine_definition="c=a*b\\nreturn c",
                parameter_style="S",
                is_deterministic=True,
                sql_data_access="NO_SQL",
                is_null_call=False,
                security_type="DEFINER",
                specific_name="myFunction",
                properties="{}",
                external_language="python",
            )
        )
    )

    try:
        assert function_info.name == "myFunction"
        assert function_info.catalog_name == "unity"
        assert function_info.schema_name == "default"
        assert function_info.external_language == "python"
        assert function_info.routine_definition == "c=a*b\\nreturn c"

        result = subprocess.run(
            'bin/uc function call --full_name unity.default.myFunction --input_params "2,3"',
            shell=True,
            check=True,
            text=True,
            capture_output=True,
        )
        assert "6" in result.stdout, result
    finally:
        await functions_api.delete_function("unity.default.myFunction")
