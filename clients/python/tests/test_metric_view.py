import pytest

from unitycatalog.client import (
    CreateTable,
    Dependency,
    DependencyList,
    TableDependency,
    TableType,
)
from unitycatalog.client.exceptions import ApiException

METRIC_VIEW_NAME = "uc_test_metric_view"
METRIC_VIEW_FULL_NAME = f"unity.default.{METRIC_VIEW_NAME}"

VIEW_DEFINITION = """\
version: "0.1"
source: unity.default.numbers
dimensions:
  - name: as_int
    expr: as_int
measures:
  - name: row_count
    expr: count(*)
"""


def _dependencies():
    return DependencyList(
        dependencies=[
            Dependency(table=TableDependency(table_full_name="unity.default.numbers"))
        ]
    )


@pytest.mark.asyncio
async def test_metric_view_create_omits_columns(tables_api):
    """METRIC_VIEW create must succeed when columns is omitted (schema from view_definition)."""
    created = await tables_api.create_table(
        CreateTable(
            name=METRIC_VIEW_NAME,
            catalog_name="unity",
            schema_name="default",
            table_type=TableType.METRIC_VIEW,
            view_definition=VIEW_DEFINITION,
            view_dependencies=_dependencies(),
            comment="Python SDK metric view without columns",
        )
    )
    try:
        assert created.name == METRIC_VIEW_NAME
        assert created.table_type == TableType.METRIC_VIEW
        assert created.view_definition == VIEW_DEFINITION
        assert created.storage_location is None
        assert not created.columns

        fetched = await tables_api.get_table(METRIC_VIEW_FULL_NAME)
        assert fetched.table_type == TableType.METRIC_VIEW
        assert fetched.view_definition == VIEW_DEFINITION
        assert not fetched.columns
        assert fetched.view_dependencies is not None
        assert len(fetched.view_dependencies.dependencies) == 1
        assert (
            fetched.view_dependencies.dependencies[0].table.table_full_name
            == "unity.default.numbers"
        )

        listed = await tables_api.list_tables("unity", "default")
        assert any(
            t.name == METRIC_VIEW_NAME and t.table_type == TableType.METRIC_VIEW
            for t in listed.tables
        )
    finally:
        await tables_api.delete_table(METRIC_VIEW_FULL_NAME)


@pytest.mark.asyncio
async def test_metric_view_create_with_empty_columns(tables_api):
    """Empty columns=[] remains a valid workaround / explicit no-schema payload."""
    created = await tables_api.create_table(
        CreateTable(
            name=METRIC_VIEW_NAME,
            catalog_name="unity",
            schema_name="default",
            table_type=TableType.METRIC_VIEW,
            columns=[],
            view_definition=VIEW_DEFINITION,
            view_dependencies=_dependencies(),
        )
    )
    try:
        assert created.table_type == TableType.METRIC_VIEW
        assert not created.columns
    finally:
        await tables_api.delete_table(METRIC_VIEW_FULL_NAME)


@pytest.mark.asyncio
async def test_metric_view_create_requires_view_definition(tables_api):
    with pytest.raises(ApiException) as exc_info:
        await tables_api.create_table(
            CreateTable(
                name=METRIC_VIEW_NAME,
                catalog_name="unity",
                schema_name="default",
                table_type=TableType.METRIC_VIEW,
                view_dependencies=_dependencies(),
            )
        )
    assert exc_info.value.status == 400
    assert "view_definition is required for metric view" in str(exc_info.value)


@pytest.mark.asyncio
async def test_metric_view_create_requires_view_dependencies(tables_api):
    with pytest.raises(ApiException) as exc_info:
        await tables_api.create_table(
            CreateTable(
                name=METRIC_VIEW_NAME,
                catalog_name="unity",
                schema_name="default",
                table_type=TableType.METRIC_VIEW,
                view_definition=VIEW_DEFINITION,
            )
        )
    assert exc_info.value.status == 400
    assert "view_dependencies must contain at least one entry" in str(exc_info.value)
