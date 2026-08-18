import pytest

from unitycatalog.client import (
    CreateTable,
    Dependency,
    DependencyList,
    TableDependency,
    TableType,
)
from unitycatalog.client.exceptions import ApiException

VIEW_NAME = "uc_test_view"
VIEW_FULL_NAME = f"unity.default.{VIEW_NAME}"

VIEW_DEFINITION = "SELECT as_int FROM unity.default.numbers"


def _dependencies():
    return DependencyList(
        dependencies=[
            Dependency(table=TableDependency(table_full_name="unity.default.numbers"))
        ]
    )


@pytest.mark.asyncio
async def test_view_create_omits_columns(tables_api):
    """VIEW create must succeed when columns is omitted (schema from view_definition)."""
    created = await tables_api.create_table(
        CreateTable(
            name=VIEW_NAME,
            catalog_name="unity",
            schema_name="default",
            table_type=TableType.VIEW,
            view_definition=VIEW_DEFINITION,
            view_dependencies=_dependencies(),
            comment="Python SDK view without columns",
        )
    )
    try:
        assert created.name == VIEW_NAME
        assert created.table_type == TableType.VIEW
        assert created.view_definition == VIEW_DEFINITION
        assert created.storage_location is None
        assert not created.columns

        fetched = await tables_api.get_table(VIEW_FULL_NAME)
        assert fetched.table_type == TableType.VIEW
        assert fetched.view_definition == VIEW_DEFINITION
        assert not fetched.columns

        listed = await tables_api.list_tables("unity", "default")
        assert any(
            t.name == VIEW_NAME and t.table_type == TableType.VIEW
            for t in listed.tables
        )
    finally:
        await tables_api.delete_table(VIEW_FULL_NAME)


@pytest.mark.asyncio
async def test_view_create_omits_columns_and_dependencies(tables_api):
    """view_dependencies is optional for a plain view; create still succeeds without columns."""
    created = await tables_api.create_table(
        CreateTable(
            name=VIEW_NAME,
            catalog_name="unity",
            schema_name="default",
            table_type=TableType.VIEW,
            view_definition=VIEW_DEFINITION,
        )
    )
    try:
        assert created.table_type == TableType.VIEW
        assert not created.columns
    finally:
        await tables_api.delete_table(VIEW_FULL_NAME)


@pytest.mark.asyncio
async def test_view_create_requires_view_definition(tables_api):
    with pytest.raises(ApiException) as exc_info:
        await tables_api.create_table(
            CreateTable(
                name=VIEW_NAME,
                catalog_name="unity",
                schema_name="default",
                table_type=TableType.VIEW,
                view_dependencies=_dependencies(),
            )
        )
    assert exc_info.value.status == 400
    assert "view_definition is required for view" in str(exc_info.value)
