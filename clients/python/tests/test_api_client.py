import asyncio
import json

from unitycatalog.client import ApiClient, CatalogsApi, Configuration
from unitycatalog.client.delta.api import DeltaTablesApi
from unitycatalog.client.delta.models.delta_commit import DeltaCommit
from unitycatalog.client.delta.models.delta_load_table_response import DeltaLoadTableResponse
from unitycatalog.client.models.delta_commit import DeltaCommit as UcDeltaCommit


def test_api_client_created_outside_event_loop_can_send_requests():
    api_client = ApiClient(
        Configuration(host="http://localhost:8080/api/2.1/unity-catalog")
    )

    async def list_catalog_names():
        try:
            response = await CatalogsApi(api_client).list_catalogs()
            return [catalog.name for catalog in response.catalogs]
        finally:
            await api_client.close()

    assert "unity" in asyncio.run(list_catalog_names())


def test_api_client_that_never_sent_a_request_can_be_closed():
    api_client = ApiClient(
        Configuration(host="http://localhost:8080/api/2.1/unity-catalog")
    )

    asyncio.run(api_client.close())


def test_api_client_with_retries_creates_and_closes_retry_client_lazily():
    api_client = ApiClient(
        Configuration(host="http://localhost:8080/api/2.1/unity-catalog", retries=3)
    )
    rest_client = api_client.rest_client
    assert rest_client.pool_manager is None
    assert rest_client.retry_client is None

    async def list_catalogs_then_close():
        try:
            await CatalogsApi(api_client).list_catalogs()
            assert rest_client.pool_manager is not None
            assert rest_client.retry_client is not None
        finally:
            await api_client.close()

    asyncio.run(list_catalogs_then_close())
    assert rest_client.pool_manager.closed


def test_api_client_created_outside_event_loop_can_send_delta_requests():
    api_client = ApiClient(
        Configuration(host="http://localhost:8080/api/2.1/unity-catalog")
    )

    async def load_numbers_column_names():
        try:
            response = await DeltaTablesApi(api_client).load_table("unity", "default", "numbers")
            return {field.name for field in response.metadata.columns.fields}
        finally:
            await api_client.close()

    assert asyncio.run(load_numbers_column_names()) == {"as_int", "as_double"}


def test_api_client_deserializes_delta_responses_with_delta_models():
    assert DeltaCommit is not UcDeltaCommit
    commit = {
        "version": 3,
        "timestamp": 1700000000000,
        "file-name": "00000000000000000003.uuid.json",
        "file-size": 512,
        "file-modification-timestamp": 1700000000001,
    }
    response = {
        "metadata": {
            "etag": "1",
            "table-type": "MANAGED",
            "table-uuid": "6a1e7ed8-0d43-4ae3-8a27-3e5e0a3fb6c1",
            "location": "file:///tmp/numbers",
            "created-time": 1700000000000,
            "updated-time": 1700000000000,
            "columns": {
                "type": "struct",
                "fields": [{"name": "id", "type": "long", "nullable": False, "metadata": {}}],
            },
            "properties": {},
        },
        "commits": [commit],
    }
    api_client = ApiClient(
        Configuration(host="http://localhost:8080/api/2.1/unity-catalog")
    )

    table = api_client.deserialize(json.dumps(response), "DeltaLoadTableResponse", "application/json")

    assert isinstance(table, DeltaLoadTableResponse)
    assert [type(c) for c in table.commits] == [DeltaCommit]
    assert table.commits[0].to_dict() == commit
    assert table.metadata.columns.fields[0].type.to_dict() == "long"
