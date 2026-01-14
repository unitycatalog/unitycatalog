package io.unitycatalog.cli.volume;

import com.fasterxml.jackson.core.type.TypeReference;
import io.unitycatalog.cli.BaseCliOperations;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.CreateVolumeRequestContent;
import io.unitycatalog.client.model.UpdateVolumeRequestContent;
import io.unitycatalog.client.model.VolumeInfo;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.volume.VolumeOperations;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class CliVolumeOperations extends BaseCliOperations implements VolumeOperations {

  public CliVolumeOperations(ServerConfig config) {
    super("volume", config);
  }

  @Override
  public VolumeInfo createVolume(CreateVolumeRequestContent createVolumeRequest)
      throws ApiException {
    List<String> argsList =
        new ArrayList<>(
            List.of(
                "--full_name",
                createVolumeRequest.getCatalogName()
                    + "."
                    + createVolumeRequest.getSchemaName()
                    + "."
                    + createVolumeRequest.getName(),
                "--storage_location",
                createVolumeRequest.getStorageLocation()));
    if (createVolumeRequest.getComment() != null) {
      argsList.add("--comment");
      argsList.add(createVolumeRequest.getComment());
    }
    return execute(VolumeInfo.class, "create", argsList);
  }

  @Override
  public List<VolumeInfo> listVolumes(
      String catalogName, String schemaName, Optional<String> pageToken) throws ApiException {
    List<String> argsList =
        new ArrayList<>(List.of("--catalog", catalogName, "--schema", schemaName));
    if (pageToken.isPresent()) {
      argsList.add("--page_token");
      argsList.add(pageToken.get());
    }
    return execute(new TypeReference<>() {}, "list", argsList);
  }

  @Override
  public VolumeInfo getVolume(String volumeFullName) throws ApiException {
    return execute(VolumeInfo.class, "get", List.of("--full_name", volumeFullName));
  }

  @Override
  public VolumeInfo updateVolume(
      String volumeFullName, UpdateVolumeRequestContent updateVolumeRequest) throws ApiException {
    List<String> argsList = new ArrayList<>();
    if (updateVolumeRequest.getNewName() != null) {
      argsList.add("--new_name");
      argsList.add(updateVolumeRequest.getNewName());
    }
    if (updateVolumeRequest.getComment() != null) {
      argsList.add("--comment");
      argsList.add(updateVolumeRequest.getComment());
    }
    return executeUpdate(
        VolumeInfo.class,
        /* catchEmptyUpdateCliException= */ true,
        List.of("--full_name", volumeFullName),
        argsList);
  }

  @Override
  public void deleteVolume(String volumeFullName) throws ApiException {
    execute(Void.class, "delete", List.of("--full_name", volumeFullName));
  }
}
