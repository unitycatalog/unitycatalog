import pytest

import subprocess

from unitycatalog import (
    CreateTable,
    CreateFunction,
    CreateVolumeRequestContent,
    CreateFunctionRequest,
    ColumnTypeName,
    TableType,
    DataSourceFormat,
    ColumnInfo,
    VolumeType,
    FunctionParameterInfos,
    FunctionParameterInfo,
)


def test_catalog_list(catalogs_api):
    api_response = catalogs_api.list_catalogs()
    catalog_names = [c.name for c in api_response.catalogs]

    assert catalog_names == ["unity"]


def test_schema_list(schemas_api):
    api_response = schemas_api.list_schemas("unity")
    schema_names = [s.name for s in api_response.schemas]

    assert schema_names == ["default"]


def test_table_list(tables_api):
    api_response = tables_api.list_tables("unity", "default")
    table_names_and_types = {(t.name, t.table_type) for t in api_response.tables}

    assert table_names_and_types == {
        ("marksheet", TableType.MANAGED),
        ("marksheet_uniform", TableType.EXTERNAL),
        ("numbers", TableType.EXTERNAL),
        ("user_countries", TableType.EXTERNAL),
    }


def test_table_get(tables_api):
    table_info = tables_api.get_table("unity.default.numbers")

    assert table_info.name == "numbers"
    assert table_info.catalog_name == "unity"
    assert table_info.schema_name == "default"
    assert table_info.table_type == TableType.EXTERNAL
    assert table_info.data_source_format == DataSourceFormat.DELTA

    columns = {
        (c.name, c.type_text, c.type_name, c.nullable) for c in table_info.columns
    }
    assert columns == {
        ("as_int", "int", ColumnTypeName.INT, False),
        ("as_double", "double", ColumnTypeName.DOUBLE, False),
    }


def test_table_create(tables_api):
    table_info = tables_api.create_table(
        CreateTable(
            name="mytable",
            catalog_name="unity",
            schema_name="default",
            table_type=TableType.EXTERNAL,
            data_source_format=DataSourceFormat.DELTA,
            columns=[
                ColumnInfo(
                    name="col1",
                    type_text="int",
                    type_name=ColumnTypeName.INT,
                    type_json='{"name":"col1","type":"integer"}',
                    position=0,
                ),
                ColumnInfo(
                    name="col2",
                    type_text="double",
                    type_name=ColumnTypeName.DOUBLE,
                    type_json='{"name":"col2","type":"double"}',
                    position=1,
                ),
            ],
            storage_location="/tmp/uc/mytable",
        )
    )

    try:
        assert table_info.name == "mytable"
        assert table_info.catalog_name == "unity"
        assert table_info.schema_name == "default"
        assert table_info.table_type == TableType.EXTERNAL
        assert table_info.data_source_format == DataSourceFormat.DELTA
        columns = {(c.name, c.type_text, c.type_name) for c in table_info.columns}
        assert columns == {
            ("col1", "int", ColumnTypeName.INT),
            ("col2", "double", ColumnTypeName.DOUBLE),
        }
        assert table_info.storage_location.rstrip("/") == "file:///tmp/uc/mytable"

        # append some randomly generated data to the table
        subprocess.run(
            "bin/uc table write --full_name unity.default.mytable",
            shell=True,
            check=True,
        )

        table_info = tables_api.get_table("unity.default.mytable")

        columns = {(c.name, c.type_text, c.type_name) for c in table_info.columns}
        assert columns == {
            ("col1", "int", ColumnTypeName.INT),
            ("col2", "double", ColumnTypeName.DOUBLE),
        }

    finally:
        tables_api.delete_table("unity.default.mytable")


def test_volume_list(volumes_api):
    api_response = volumes_api.list_volumes("unity", "default")
    volume_names = {v.name for v in api_response.volumes}

    assert volume_names == {"txt_files", "json_files"}


@pytest.mark.parametrize(
    "volume_name,volume_type",
    [
        ("txt_files", VolumeType.MANAGED),
        ("json_files", VolumeType.EXTERNAL),
    ],
)
def test_volume_get(volumes_api, volume_name, volume_type):
    volume_info = volumes_api.get_volume(f"unity.default.{volume_name}")

    assert volume_info.name == volume_name
    assert volume_info.catalog_name == "unity"
    assert volume_info.schema_name == "default"
    assert volume_info.volume_type == volume_type


def test_volume_create(volumes_api):
    subprocess.run("mkdir -p /tmp/uc/myVolume", shell=True, check=True)
    subprocess.run(
        "cp etc/data/external/unity/default/volumes/json_files/c.json /tmp/uc/myVolume/",
        shell=True,
        check=True,
    )

    volume_info = volumes_api.create_volume(
        CreateVolumeRequestContent(
            name="myVolume",
            catalog_name="unity",
            schema_name="default",
            volume_type=VolumeType.EXTERNAL,
            storage_location="/tmp/uc/myVolume",
        )
    )

    try:
        assert volume_info.name == "myVolume"
        assert volume_info.catalog_name == "unity"
        assert volume_info.schema_name == "default"
        assert volume_info.volume_type == VolumeType.EXTERNAL
        assert volume_info.storage_location == "file:///tmp/uc/myVolume/"

        result = subprocess.run(
            "bin/uc volume read --full_name unity.default.myVolume",
            shell=True,
            check=True,
            text=True,
            capture_output=True,
        )
        assert "c.json" in result.stdout, result

        result = subprocess.run(
            "bin/uc volume read --full_name unity.default.myVolume --path c.json",
            shell=True,
            check=True,
            text=True,
            capture_output=True,
        )
        assert "marks" in result.stdout, result

        subprocess.run("rm -rf /tmp/uc/myVolume", shell=True, check=True)

    finally:
        volumes_api.delete_volume("unity.default.myVolume")


def test_function_list(functions_api):
    api_response = functions_api.list_functions("unity", "default")
    function_names = {f.name for f in api_response.functions}

    assert function_names == {"lowercase", "sum"}


@pytest.mark.parametrize(
    "function_name,function_def",
    [
        ("sum", "t = x + y + z\\nreturn t"),
        ("lowercase", "g = s.lower()\\nreturn g"),
    ],
)
def test_function_get(functions_api, function_name, function_def):
    function_info = functions_api.get_function(f"unity.default.{function_name}")

    assert function_info.name == function_name
    assert function_info.catalog_name == "unity"
    assert function_info.schema_name == "default"
    assert function_info.external_language == "python"
    assert function_info.routine_definition == function_def


def test_function_create(functions_api):
    function_info = functions_api.create_function(
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
        functions_api.delete_function("unity.default.myFunction")
