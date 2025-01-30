package io.unitycatalog.server.utils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.persist.utils.FileOperations;
import io.unitycatalog.server.persist.utils.UriUtils;
import java.util.Properties;
import org.junit.jupiter.api.Test;

public class FileOperationsTest {
  @Test
  public void testUriUtils() {
    Properties properties = new Properties();
    properties.setProperty("storage-root.models", "/tmp");
    ServerProperties serverProperties = new ServerProperties(properties);
    FileOperations fileOperations = new FileOperations(serverProperties);

    String modelPathUri = fileOperations.getModelStorageLocation("catalog", "schema", "my-model");
    String modelVersionPathUri =
        fileOperations.getModelVersionStorageLocation("catalog", "schema", "my-model", "1");
    String modelPath = UriUtils.createStorageLocationPath(modelPathUri);
    String modelVersionPath = UriUtils.createStorageLocationPath(modelVersionPathUri);

    assertThat(modelPath).isEqualTo("file:///tmp/catalog/schema/models/my-model");
    assertThat(modelVersionPath).isEqualTo("file:///tmp/catalog/schema/models/my-model/versions/1");

    UriUtils.deleteStorageLocationPath(modelVersionPathUri);
    UriUtils.deleteStorageLocationPath(modelPathUri);

    // cleanup the created catalog
    UriUtils.deleteStorageLocationPath("file:/tmp/catalog");

    properties.setProperty("storage-root.models", "file:///tmp/random");
    serverProperties = new ServerProperties(properties);
    fileOperations = new FileOperations(serverProperties);

    modelPathUri = fileOperations.getModelStorageLocation("catalog", "schema", "my-model");
    modelVersionPathUri =
        fileOperations.getModelVersionStorageLocation("catalog", "schema", "my-model", "1");
    modelPath = UriUtils.createStorageLocationPath(modelPathUri);
    modelVersionPath = UriUtils.createStorageLocationPath(modelVersionPathUri);

    assertThat(modelPath).isEqualTo("file:///tmp/random/catalog/schema/models/my-model");
    assertThat(modelVersionPath)
        .isEqualTo("file:///tmp/random/catalog/schema/models/my-model/versions/1");

    UriUtils.deleteStorageLocationPath(modelVersionPathUri);
    UriUtils.deleteStorageLocationPath(modelPathUri);

    // cleanup the created catalog
    UriUtils.deleteStorageLocationPath("file:/tmp/random");

    assertThatThrownBy(() -> UriUtils.createStorageLocationPath(".."))
        .isInstanceOf(BaseException.class);

    assertThatThrownBy(() -> UriUtils.deleteStorageLocationPath(""))
        .isInstanceOf(BaseException.class);
  }
}
