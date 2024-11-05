import pytest
import subprocess

from unitycatalog.client import (
    CreateTable,
    ColumnTypeName,
    TableType,
    DataSourceFormat,
    ColumnInfo,
)


@pytest.mark.asyncio
async def test_table_list(tables_api):
    api_response = await tables_api.list_tables("unity", "default")
    table_names_and_types = {(t.name, t.table_type) for t in api_response.tables}

    assert table_names_and_types == {
        ("marksheet", TableType.MANAGED),
        ("marksheet_uniform", TableType.EXTERNAL),
        ("numbers", TableType.EXTERNAL),
        ("user_countries", TableType.EXTERNAL),
    }


@pytest.mark.asyncio
async def test_table_get(tables_api):
    table_info = await tables_api.get_table("unity.default.numbers")

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


@pytest.mark.asyncio
async def test_table_create(tables_api):
    table_info = await tables_api.create_table(
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

        table_info = await tables_api.get_table("unity.default.mytable")

        columns = {(c.name, c.type_text, c.type_name) for c in table_info.columns}
        assert columns == {
            ("col1", "int", ColumnTypeName.INT),
            ("col2", "double", ColumnTypeName.DOUBLE),
        }

    finally:
        await tables_api.delete_table("unity.default.mytable")
