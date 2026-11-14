import pytest

from unitycatalog.client.delta.models.delta_primitive_type import DeltaPrimitiveType


@pytest.mark.asyncio
async def test_delta_load_table(delta_tables_api):
    response = await delta_tables_api.load_table("unity", "default", "numbers")

    fields = response.metadata.columns.fields
    assert all(isinstance(f.type, DeltaPrimitiveType) for f in fields)
    assert {(f.name, f.type.type, f.nullable) for f in fields} == {
        ("as_int", "integer", False),
        ("as_double", "double", False),
    }
