import uuid

import pytest

from unitycatalog.client import CreateCatalog, CreateSchema
from unitycatalog.client.delta.models.delta_create_staging_table_request import (
    DeltaCreateStagingTableRequest,
)
from unitycatalog.client.delta.models.delta_credential_operation import DeltaCredentialOperation
from unitycatalog.client.delta.models.delta_credentials_response import DeltaCredentialsResponse
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


@pytest.mark.parametrize("expiration", [None, 1700000000000])
def test_delta_credential_expiration_accepts_null(expiration):
    credential = {
        "prefix": "file:///tmp/uc/tbl",
        "operation": "READ",
        "config": {},
        "expiration-time-ms": expiration,
    }

    response = DeltaCredentialsResponse.from_dict({"storage-credentials": [credential]})

    assert response.storage_credentials[0].expiration_time_ms == expiration


@pytest.mark.asyncio
async def test_delta_staging_credentials_on_local_storage(
    catalogs_api, schemas_api, delta_tables_api, delta_temporary_credentials_api, tmp_path
):
    catalog = f"delta_credentials_{uuid.uuid4().hex}"
    await catalogs_api.create_catalog(CreateCatalog(name=catalog, storage_root=tmp_path.as_uri()))
    try:
        await schemas_api.create_schema(CreateSchema(name="default", catalog_name=catalog))
        staging = await delta_tables_api.create_staging_table(
            catalog, "default", DeltaCreateStagingTableRequest(name="staged")
        )
        staging_credentials = await delta_temporary_credentials_api.get_staging_table_credentials(
            str(staging.table_id)
        )

        for credentials in (staging.storage_credentials, staging_credentials.storage_credentials):
            assert credentials[0].prefix.startswith("file:")
            assert credentials[0].expiration_time_ms is None
    finally:
        await catalogs_api.delete_catalog(catalog, force=True)


@pytest.mark.asyncio
async def test_delta_table_credentials_on_local_storage(delta_temporary_credentials_api):
    credentials = await delta_temporary_credentials_api.get_table_credentials(
        operation=DeltaCredentialOperation.READ,
        catalog="unity",
        var_schema="default",
        table="numbers",
    )

    assert credentials.storage_credentials[0].prefix.startswith("file:")
    assert credentials.storage_credentials[0].expiration_time_ms is None
