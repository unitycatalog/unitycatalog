import pytest


@pytest.mark.asyncio
async def test_catalog_list(catalogs_api):
    response = await catalogs_api.list_catalogs()
    assert response is not None
    assert isinstance(response.catalogs, list)
