import pytest


@pytest.mark.asyncio
async def test_schema_list(schemas_api):
    api_response = await schemas_api.list_schemas("unity")
    schema_names = [s.name for s in api_response.schemas]
    assert "default" in schema_names
