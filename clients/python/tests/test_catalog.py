def test_catalog_list(catalogs_api):
    api_response = catalogs_api.list_catalogs()
    catalog_names = [c.name for c in api_response.catalogs]

    assert catalog_names == ["unity"]
