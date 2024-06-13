package io.unitycatalog.server.sdk.volume;

import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.VolumesApi;
import io.unitycatalog.client.model.CreateVolumeRequestContent;
import io.unitycatalog.client.model.UpdateVolumeRequestContent;
import io.unitycatalog.client.model.VolumeInfo;
import io.unitycatalog.server.base.volume.VolumeOperations;

import java.util.List;

public class SdkVolumeOperations implements VolumeOperations {
    private final VolumesApi volumesApi;

    public SdkVolumeOperations(ApiClient apiClient) {
        this.volumesApi = new VolumesApi(apiClient);
    }

    @Override
    public VolumeInfo createVolume(CreateVolumeRequestContent createVolumeRequest) throws ApiException {
        return volumesApi.createVolume(createVolumeRequest);
    }

    @Override
    public List<VolumeInfo> listVolumes(String catalogName, String schemaName) throws ApiException {
        return volumesApi.listVolumes(catalogName, schemaName, 100 , null).getVolumes();
    }

    @Override
    public VolumeInfo getVolume(String volumeFullName) throws ApiException {
        return volumesApi.getVolume(volumeFullName);
    }

    @Override
    public VolumeInfo updateVolume(String fullName, UpdateVolumeRequestContent updateVolumeRequest) throws ApiException {
        return volumesApi.updateVolume(fullName, updateVolumeRequest);
    }

    @Override
    public void deleteVolume(String volumeFullName) throws ApiException {
        volumesApi.deleteVolume(volumeFullName);
    }

}
