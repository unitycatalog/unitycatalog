package io.unitycatalog.server.utils;

import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.server.persist.utils.ExternalLocationUtils;
import io.unitycatalog.server.persist.utils.UriUtils;
import java.util.UUID;
import org.junit.jupiter.api.Test;

public class FileOperationsTest {
  @Test
  public void testModelDirectory() {
    // Test model directory creation with a given storage root
    NormalizedURL parentStorageLocation = NormalizedURL.from("file:///tmp/storage");
    UUID modelId = UUID.randomUUID();
    NormalizedURL modelPathUri =
        ExternalLocationUtils.getManagedLocationForModel(parentStorageLocation, modelId);
    assertThat(modelPathUri.toString()).isEqualTo(parentStorageLocation + "/models/" + modelId);

    UriUtils.createStorageLocationDir(modelPathUri);
    UriUtils.deleteStorageLocationDir(modelPathUri);

    // cleanup the created storage directory
    UriUtils.deleteStorageLocationDir(parentStorageLocation);
  }
}
