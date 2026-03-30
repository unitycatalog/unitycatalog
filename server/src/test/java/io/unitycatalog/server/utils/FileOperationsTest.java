package io.unitycatalog.server.utils;

import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.server.persist.utils.ExternalLocationUtils;
import io.unitycatalog.server.persist.utils.FileOperations;
import java.nio.file.Path;
import java.util.UUID;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class FileOperationsTest {

  @TempDir Path rootBase;

  @SneakyThrows
  @Test
  public void testModelDirectory() {
    // Test model directory creation with a given storage root
    NormalizedURL parentStorageLocation = NormalizedURL.from(rootBase.toString());
    UUID modelId = UUID.randomUUID();
    NormalizedURL modelPathUri =
        ExternalLocationUtils.getManagedLocationForModel(parentStorageLocation, modelId);
    assertThat(modelPathUri.toString()).isEqualTo(parentStorageLocation + "/models/" + modelId);

    FileOperations.createStorageLocationDir(modelPathUri);
    FileOperations.deleteDirectory(modelPathUri);
  }
}
