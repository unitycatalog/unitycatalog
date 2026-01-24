package io.unitycatalog.server.utils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.server.exception.BaseException;
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
    NormalizedURL storageRoot = NormalizedURL.from("file:///tmp/storage");
    UUID modelId = UUID.randomUUID();
    NormalizedURL modelPathUri = fileOperations.createManagedModelDirectory(storageRoot, modelId);
    assertThat(modelPathUri.toString()).isEqualTo("file:///tmp/storage/models/" + modelId);

    UriUtils.createStorageLocationPath(modelPathUri);
    UriUtils.deleteStorageLocationPath(modelPathUri);

    // cleanup the created storage directory
    UriUtils.deleteStorageLocationPath("file:/tmp/storage");

    // Test table directory creation
    UUID tableId = UUID.randomUUID();
    NormalizedURL tablePathUri = fileOperations.createManagedTableDirectory(storageRoot, tableId);
    assertThat(tablePathUri.toString()).isEqualTo("file:///tmp/storage/tables/" + tableId);

    // Test volume directory creation
    UUID volumeId = UUID.randomUUID();
    NormalizedURL volumePathUri =
        fileOperations.createManagedVolumeDirectory(storageRoot, volumeId);
    assertThat(volumePathUri.toString()).isEqualTo("file:///tmp/storage/volumes/" + volumeId);

    // Test catalog directory creation
    UUID catalogId = UUID.randomUUID();
    NormalizedURL catalogPathUri =
        fileOperations.createManagedCatalogDirectory(storageRoot, catalogId);
    assertThat(catalogPathUri.toString())
        .isEqualTo("file:///tmp/storage/__unitystorage/catalogs/" + catalogId);

    // Test schema directory creation
    UUID schemaId = UUID.randomUUID();
    NormalizedURL schemaPathUri =
        fileOperations.createManagedSchemaDirectory(storageRoot, schemaId);
    assertThat(schemaPathUri.toString())
        .isEqualTo("file:///tmp/storage/__unitystorage/schemas/" + schemaId);

    assertThatThrownBy(() -> UriUtils.createStorageLocationPath(".."))
        .isInstanceOf(BaseException.class);

    assertThatThrownBy(() -> UriUtils.deleteStorageLocationPath(""))
        .isInstanceOf(BaseException.class);
  }
}
