package io.unitycatalog.server.utils;

import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.server.persist.utils.FileOperations;
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
        FileOperations.getManagedLocationForModel(parentStorageLocation, modelId);
    assertThat(modelPathUri.toString()).isEqualTo(parentStorageLocation + "/models/" + modelId);

    UriUtils.createStorageLocationDir(modelPathUri);
    UriUtils.deleteStorageLocationDir(modelPathUri);

    // cleanup the created storage directory
    UriUtils.deleteStorageLocationDir(parentStorageLocation);
  }

  @Test
  public void testManagedLocation() {
    NormalizedURL parentStorageLocation = NormalizedURL.from("file:///tmp/storage");

    // Test table directory creation
    UUID tableId = UUID.randomUUID();
    NormalizedURL tablePathUri =
        FileOperations.getManagedLocationForTable(parentStorageLocation, tableId);
    assertThat(tablePathUri.toString()).isEqualTo("file:///tmp/storage/tables/" + tableId);

    // Test volume directory creation
    UUID volumeId = UUID.randomUUID();
    NormalizedURL volumePathUri =
        FileOperations.getManagedLocationForVolume(parentStorageLocation, volumeId);
    assertThat(volumePathUri.toString()).isEqualTo("file:///tmp/storage/volumes/" + volumeId);

    // Test catalog directory creation
    UUID catalogId = UUID.randomUUID();
    NormalizedURL catalogPathUri =
        FileOperations.getManagedLocationForCatalog(parentStorageLocation, catalogId);
    assertThat(catalogPathUri.toString())
        .isEqualTo("file:///tmp/storage/__unitystorage/catalogs/" + catalogId);

    // Test schema directory creation
    UUID schemaId = UUID.randomUUID();
    NormalizedURL schemaPathUri =
        FileOperations.getManagedLocationForSchema(parentStorageLocation, schemaId);
    assertThat(schemaPathUri.toString())
        .isEqualTo("file:///tmp/storage/__unitystorage/schemas/" + schemaId);
  }
}
