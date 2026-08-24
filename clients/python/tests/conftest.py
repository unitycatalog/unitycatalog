import pytest
import pytest_asyncio
import subprocess
import os
import time
import requests
import signal

from unitycatalog.client import (
    ApiClient,
    CatalogsApi,
    Configuration,
    FunctionsApi,
    GrantsApi,
    ModelVersionsApi,
    RegisteredModelsApi,
    SchemasApi,
    TablesApi,
    TemporaryCredentialsApi,
    VolumesApi,
)


@pytest.fixture(scope="session", autouse=True)
def uc_server():
    # Start server
    try:
        log_file = "/tmp/server_log.txt"
        with open(log_file, 'w') as fp:
            process = subprocess.Popen("bin/start-uc-server", shell=True, stdout=fp, stderr=fp, preexec_fn=os.setsid)
            print(f">> Started server with PID {os.getpgid(process.pid)}")
            return_code = process.poll()
            if return_code is not None:
                with open(log_file, 'r') as lf:
                    print(f"Error starting process:\n{lf.read()}")
                raise Exception(f"Failed to start server. Return code: {return_code}")
            print(">> Waiting for server to accept connections ...")
            for _ in range(90):
                try:
                    response = requests.head("http://localhost:8081", timeout=60)
                    if response.status_code == 200:
                        print("Server is running.")
                        break
                except requests.RequestException as e:
                    pass

                time.sleep(2)
            else:
                with open(log_file, 'r') as lf:
                    print(f">> Server is taking too long to get ready, failing tests. Log:\n{lf.read()}")
                raise Exception(f"Server took too long to start. Log:\n{lf.read()}")
        yield

    # Stop server
    finally:
        try:
            os.killpg(os.getpgid(process.pid), signal.SIGTERM)
            print(">> Stopped the server")
        except ProcessLookupError:
            # Process already terminated
            pass


@pytest_asyncio.fixture()
async def api_client():
    """
    Asynchronous fixture to initialize and yield the ApiClient.
    """
    config = Configuration(host="http://localhost:8080/api/2.1/unity-catalog")
    client = ApiClient(config)
    yield client
    await client.close()


@pytest_asyncio.fixture()
async def catalogs_api(api_client):
    return CatalogsApi(api_client)


@pytest_asyncio.fixture()
async def functions_api(api_client):
    return FunctionsApi(api_client)


@pytest_asyncio.fixture()
async def grants_api(api_client):
    return GrantsApi(api_client)


@pytest_asyncio.fixture()
async def model_versions_api(api_client):
    return ModelVersionsApi(api_client)


@pytest_asyncio.fixture()
async def registered_models_api(api_client):
    return RegisteredModelsApi(api_client)


@pytest_asyncio.fixture()
async def schemas_api(api_client):
    return SchemasApi(api_client)


@pytest_asyncio.fixture()
async def tables_api(api_client):
    return TablesApi(api_client)


@pytest_asyncio.fixture()
async def temporary_credentials_api(api_client):
    return TemporaryCredentialsApi(api_client)


@pytest_asyncio.fixture()
async def volumes_api(api_client):
    return VolumesApi(api_client)
