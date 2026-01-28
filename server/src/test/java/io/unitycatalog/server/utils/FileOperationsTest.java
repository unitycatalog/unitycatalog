package io.unitycatalog.server.utils;

import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.server.persist.utils.FileOperations;
import io.unitycatalog.server.persist.utils.UriUtils;
import java.util.UUID;
import org.junit.jupiter.api.Test;

public class FileOperationsTest {
  @Test
  public void testUriUtils() {
    ServerProperties serverProperties = new ServerProperties();
    FileOperations fileOperations = new FileOperations(serverProperties);

    // Test model directory creation with a given storage root
    NormalizedURL parentStorageLocation = NormalizedURL.from("file:///tmp/storage");
    UUID modelId = UUID.randomUUID();
    NormalizedURL modelPathUri =
        fileOperations.createManagedLocationForModel(parentStorageLocation, modelId);
    assertThat(modelPathUri.toString()).isEqualTo("file:///tmp/storage/models/" + modelId);

    UriUtils.createStorageLocationDir(modelPathUri);
    UriUtils.deleteStorageLocationDir(modelPathUri);

    // cleanup the created storage directory
    UriUtils.deleteStorageLocationDir(NormalizedURL.from("file:/tmp/storage"));

    // Test table directory creation
    UUID tableId = UUID.randomUUID();
    NormalizedURL tablePathUri =
        fileOperations.createManagedLocationForTable(parentStorageLocation, tableId);
    assertThat(tablePathUri.toString()).isEqualTo("file:///tmp/storage/tables/" + tableId);

    // Test volume directory creation
    UUID volumeId = UUID.randomUUID();
    NormalizedURL volumePathUri =
        fileOperations.createManagedLocationForVolume(parentStorageLocation, volumeId);
    assertThat(volumePathUri.toString()).isEqualTo("file:///tmp/storage/volumes/" + volumeId);

    // Test catalog directory creation
    UUID catalogId = UUID.randomUUID();
    NormalizedURL catalogPathUri =
        fileOperations.createManagedLocationForCatalog(parentStorageLocation, catalogId);
    assertThat(catalogPathUri.toString())
        .isEqualTo("file:///tmp/storage/__unitystorage/catalogs/" + catalogId);

    // Test schema directory creation
    UUID schemaId = UUID.randomUUID();
    NormalizedURL schemaPathUri =
        fileOperations.createManagedLocationForSchema(parentStorageLocation, schemaId);
    assertThat(schemaPathUri.toString())
        .isEqualTo("file:///tmp/storage/__unitystorage/schemas/" + schemaId);
  }
}
