import pytest
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
    VolumesApi
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


@pytest.fixture(scope="session")
def api_client():
    config = Configuration(
        host = "http://localhost:8081/api/2.1/unity-catalog"
    )

    with ApiClient(config) as api_client:
        yield api_client

@pytest.fixture(scope="session")
def catalogs_api(api_client):
    yield CatalogsApi(api_client)

@pytest.fixture(scope="session")
def functions_api(api_client):
    yield FunctionsApi(api_client)

@pytest.fixture(scope="session")
def grants_api(api_client):
    yield GrantsApi(api_client)

@pytest.fixture(scope="session")
def model_versions_api(api_client):
    yield ModelVersionsApi(api_client)

@pytest.fixture(scope="session")
def registered_models_api(api_client):
    yield RegisteredModelsApi(api_client)

@pytest.fixture(scope="session")
def schemas_api(api_client):
    yield SchemasApi(api_client)

@pytest.fixture(scope="session")
def tables_api(api_client):
    yield TablesApi(api_client)

@pytest.fixture(scope="session")
def temporary_credentials_api(api_client):
    yield TemporaryCredentialsApi(api_client)

@pytest.fixture(scope="session")
def volumes_api(api_client):
    yield VolumesApi(api_client)
