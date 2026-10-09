import asyncio

from unitycatalog.client import ApiClient, CatalogsApi, Configuration


def test_api_client_created_outside_event_loop_can_send_requests():
    api_client = ApiClient(Configuration(host="http://localhost:8080/api/2.1/unity-catalog"))

    async def list_catalog_names():
        try:
            response = await CatalogsApi(api_client).list_catalogs()
            return [catalog.name for catalog in response.catalogs]
        finally:
            await api_client.close()

    assert "unity" in asyncio.run(list_catalog_names())


def test_api_client_that_never_sent_a_request_can_be_closed():
    api_client = ApiClient(Configuration(host="http://localhost:8080/api/2.1/unity-catalog"))

    asyncio.run(api_client.close())
