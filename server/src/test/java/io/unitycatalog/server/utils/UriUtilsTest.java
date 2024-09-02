package io.unitycatalog.server.utils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.persist.utils.UriUtils;
import org.junit.jupiter.api.Test;

public class UriUtilsTest {

  @Test
  public void testUriUtils() {

    System.setProperty("storage-root.models", "/tmp");

    String modelPathUri = UriUtils.getModelStorageLocation("catalog", "schema", "my-model");
    String modelVersionPathUri =
        UriUtils.getModelVersionStorageLocation("catalog", "schema", "my-model", "1");
    String modelPath = UriUtils.createStorageLocationPath(modelPathUri);
    String modelVersionPath = UriUtils.createStorageLocationPath(modelVersionPathUri);

    assertThat(modelPath).isEqualTo("file:/tmp/catalog/schema/models/my-model");
    assertThat(modelVersionPath).isEqualTo("file:/tmp/catalog/schema/models/my-model/versions/1");

    UriUtils.deleteStorageLocationPath(modelVersionPathUri);
    UriUtils.deleteStorageLocationPath(modelPathUri);

    // cleanup the created catalog
    UriUtils.deleteStorageLocationPath("file:/tmp/catalog");

    System.setProperty("storage-root.models", "file:///tmp/random");

    modelPathUri = UriUtils.getModelStorageLocation("catalog", "schema", "my-model");
    modelVersionPathUri =
        UriUtils.getModelVersionStorageLocation("catalog", "schema", "my-model", "1");
    modelPath = UriUtils.createStorageLocationPath(modelPathUri);
    modelVersionPath = UriUtils.createStorageLocationPath(modelVersionPathUri);

    assertThat(modelPath).isEqualTo("file:///tmp/random/catalog/schema/models/my-model");
    assertThat(modelVersionPath)
        .isEqualTo("file:///tmp/random/catalog/schema/models/my-model/versions/1");

    UriUtils.deleteStorageLocationPath(modelVersionPathUri);
    UriUtils.deleteStorageLocationPath(modelPathUri);

    // cleanup the created catalog
    UriUtils.deleteStorageLocationPath("file:/tmp/random");

    assertThatThrownBy(
            () -> {
              UriUtils.createStorageLocationPath("..");
            })
        .isInstanceOf(BaseException.class);

    assertThatThrownBy(
            () -> {
              UriUtils.deleteStorageLocationPath("");
            })
        .isInstanceOf(BaseException.class);
  }
}
