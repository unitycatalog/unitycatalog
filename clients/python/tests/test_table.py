import pytest
import subprocess

from unitycatalog.client import (
    CreateTable,
    ColumnTypeName,
    TableType,
    DataSourceFormat,
    ColumnInfo,
    Dependency,
    DependencyList,
    TableDependency,
)
from unitycatalog.client.exceptions import ApiException


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
                    type_json='{"name":"col1","type":"integer","nullable":true,"metadata":{}}',
                    position=0,
                ),
                ColumnInfo(
                    name="col2",
                    type_text="double",
                    type_name=ColumnTypeName.DOUBLE,
                    type_json='{"name":"col2","type":"double","nullable":true,"metadata":{}}',
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


_METRIC_VIEW_DEFINITION = """\
version: "0.1"
source: unity.default.numbers
dimensions:
  - name: as_int
    expr: as_int
measures:
  - name: row_count
    expr: count(*)
"""

_METRIC_VIEW_FULL_NAME = "unity.default.uc_test_metric_view"


def _metric_view_dependencies():
    return DependencyList(
        dependencies=[
            Dependency(
                table=TableDependency(table_full_name="unity.default.numbers")
            )
        ]
    )


@pytest.mark.asyncio
async def test_metric_view_create_omits_columns(tables_api):
    """METRIC_VIEW create must succeed when columns is omitted (schema from view_definition)."""
    created = await tables_api.create_table(
        CreateTable(
            name="uc_test_metric_view",
            catalog_name="unity",
            schema_name="default",
            table_type=TableType.METRIC_VIEW,
            view_definition=_METRIC_VIEW_DEFINITION,
            view_dependencies=_metric_view_dependencies(),
            comment="Python SDK metric view without columns",
        )
    )
    try:
        assert created.name == "uc_test_metric_view"
        assert created.table_type == TableType.METRIC_VIEW
        assert created.view_definition == _METRIC_VIEW_DEFINITION
        assert created.storage_location is None
        assert not created.columns

        fetched = await tables_api.get_table(_METRIC_VIEW_FULL_NAME)
        assert fetched.table_type == TableType.METRIC_VIEW
        assert fetched.view_definition == _METRIC_VIEW_DEFINITION
        assert not fetched.columns
        assert fetched.view_dependencies is not None
        assert len(fetched.view_dependencies.dependencies) == 1
        assert (
            fetched.view_dependencies.dependencies[0].table.table_full_name
            == "unity.default.numbers"
        )

        listed = await tables_api.list_tables("unity", "default")
        assert any(
            t.name == "uc_test_metric_view" and t.table_type == TableType.METRIC_VIEW
            for t in listed.tables
        )
    finally:
        await tables_api.delete_table(_METRIC_VIEW_FULL_NAME)


@pytest.mark.asyncio
async def test_metric_view_create_with_empty_columns(tables_api):
    """Empty columns=[] remains a valid workaround / explicit no-schema payload."""
    created = await tables_api.create_table(
        CreateTable(
            name="uc_test_metric_view",
            catalog_name="unity",
            schema_name="default",
            table_type=TableType.METRIC_VIEW,
            columns=[],
            view_definition=_METRIC_VIEW_DEFINITION,
            view_dependencies=_metric_view_dependencies(),
        )
    )
    try:
        assert created.table_type == TableType.METRIC_VIEW
        assert not created.columns
    finally:
        await tables_api.delete_table(_METRIC_VIEW_FULL_NAME)


@pytest.mark.asyncio
async def test_metric_view_create_requires_view_definition(tables_api):
    with pytest.raises(ApiException) as exc_info:
        await tables_api.create_table(
            CreateTable(
                name="uc_test_metric_view_bad",
                catalog_name="unity",
                schema_name="default",
                table_type=TableType.METRIC_VIEW,
                view_dependencies=_metric_view_dependencies(),
            )
        )
    assert exc_info.value.status == 400
    assert "view_definition is required for metric view" in str(exc_info.value)


@pytest.mark.asyncio
async def test_metric_view_create_requires_view_dependencies(tables_api):
    with pytest.raises(ApiException) as exc_info:
        await tables_api.create_table(
            CreateTable(
                name="uc_test_metric_view_bad",
                catalog_name="unity",
                schema_name="default",
                table_type=TableType.METRIC_VIEW,
                view_definition=_METRIC_VIEW_DEFINITION,
            )
        )
    assert exc_info.value.status == 400
    assert "view_dependencies must contain at least one entry" in str(exc_info.value)
