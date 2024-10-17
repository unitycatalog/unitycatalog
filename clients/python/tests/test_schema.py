def test_schema_list(schemas_api):
    api_response = schemas_api.list_schemas("unity")
    schema_names = [s.name for s in api_response.schemas]

    assert schema_names == ["default"]
