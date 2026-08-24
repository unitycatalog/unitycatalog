package io.unitycatalog.server.base.volume;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.CreateVolumeRequestContent;
import io.unitycatalog.client.model.UpdateVolumeRequestContent;
import io.unitycatalog.client.model.VolumeInfo;
import java.util.List;
import java.util.Optional;

public interface VolumeOperations {
  VolumeInfo createVolume(CreateVolumeRequestContent createVolumeRequest) throws ApiException;

  List<VolumeInfo> listVolumes(String catalogName, String schemaName, Optional<String> pageToken)
      throws ApiException;

  VolumeInfo getVolume(String volumeFullName) throws ApiException;

  VolumeInfo updateVolume(String fullName, UpdateVolumeRequestContent updateVolumeRequest)
      throws ApiException;

  void deleteVolume(String volumeFullName) throws ApiException;
}
