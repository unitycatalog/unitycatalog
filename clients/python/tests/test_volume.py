import pytest

import subprocess

from unitycatalog.client import (
    CreateVolumeRequestContent,
    VolumeType,
)


@pytest.mark.asyncio
async def test_volume_list(volumes_api):
    api_response = await volumes_api.list_volumes("unity", "default")
    volume_names = {v.name for v in api_response.volumes}

    assert volume_names == {"txt_files", "json_files"}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "volume_name,volume_type",
    [
        ("txt_files", VolumeType.MANAGED),
        ("json_files", VolumeType.EXTERNAL),
    ],
)
async def test_volume_get(volumes_api, volume_name, volume_type):
    volume_info = await volumes_api.get_volume(f"unity.default.{volume_name}")

    assert volume_info.name == volume_name
    assert volume_info.catalog_name == "unity"
    assert volume_info.schema_name == "default"
    assert volume_info.volume_type == volume_type


@pytest.mark.asyncio
async def test_volume_create(volumes_api):
    subprocess.run("mkdir -p /tmp/uc/myVolume", shell=True, check=True)
    subprocess.run(
        "cp etc/data/external/unity/default/volumes/json_files/c.json /tmp/uc/myVolume/",
        shell=True,
        check=True,
    )

    volume_info = await volumes_api.create_volume(
        CreateVolumeRequestContent(
            name="myVolume",
            catalog_name="unity",
            schema_name="default",
            volume_type=VolumeType.EXTERNAL,
            storage_location="/tmp/uc/myVolume",
        )
    )

    try:
        assert volume_info.name == "myVolume"
        assert volume_info.catalog_name == "unity"
        assert volume_info.schema_name == "default"
        assert volume_info.volume_type == VolumeType.EXTERNAL
        assert volume_info.storage_location == "file:///tmp/uc/myVolume/"

        result = subprocess.run(
            "bin/uc volume read --full_name unity.default.myVolume",
            shell=True,
            check=True,
            text=True,
            capture_output=True,
        )
        assert "c.json" in result.stdout, result

        result = subprocess.run(
            "bin/uc volume read --full_name unity.default.myVolume --path c.json",
            shell=True,
            check=True,
            text=True,
            capture_output=True,
        )
        assert "marks" in result.stdout, result

        subprocess.run("rm -rf /tmp/uc/myVolume", shell=True, check=True)

    finally:
        await volumes_api.delete_volume("unity.default.myVolume")
