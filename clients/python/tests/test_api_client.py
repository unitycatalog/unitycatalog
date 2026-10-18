import asyncio

from unitycatalog.client import ApiClient, CatalogsApi, Configuration


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
        await CatalogsApi(api_client).list_catalogs()
        assert rest_client.pool_manager is not None
        assert rest_client.retry_client is not None
        await api_client.close()

    asyncio.run(list_catalogs_then_close())
    assert rest_client.pool_manager.closed
