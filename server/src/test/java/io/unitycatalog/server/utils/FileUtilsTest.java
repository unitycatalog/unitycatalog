package io.unitycatalog.server.utils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.persist.utils.FileUtils;
import org.junit.jupiter.api.Test;

public class FileUtilsTest {

  @Test
  public void testFileUtils() {

    System.setProperty("storageRoot", "/tmp");
    System.setProperty("registeredModelStorageRoot", "/tmp");

    String tablePath = FileUtils.createTableDirectory("catalog", "schema", "table");
    String volumePath = FileUtils.createVolumeDirectory("volume");
    String modelPath = FileUtils.createRegisteredModelDirectory("catalog", "schema", "my-model");
    String modelVersionPath =
        FileUtils.createModelVersionDirectory("catalog", "schema", "my-model");

    assertThat(tablePath).isEqualTo("file:///tmp/catalog/schema/tables/table/");
    assertThat(volumePath).isEqualTo("file:///tmp/volume/");
    assertThat(modelPath).isEqualTo("file:///tmp/catalog/schema/models/my-model/");
    assertThat(modelVersionPath).isEqualTo("file:///tmp/catalog/schema/models/my-model/versions/");

    FileUtils.deleteDirectory(tablePath);
    FileUtils.deleteDirectory(volumePath);
    FileUtils.deleteDirectory(modelVersionPath);
    FileUtils.deleteDirectory(modelPath);

    System.setProperty("storageRoot", "file:///tmp/random");
    System.setProperty("registeredModelStorageRoot", "file:///tmp/random");

    tablePath = FileUtils.createTableDirectory("catalog", "schema", "table");
    volumePath = FileUtils.createVolumeDirectory("volume");
    modelPath = FileUtils.createRegisteredModelDirectory("catalog", "schema", "my-model");
    modelVersionPath = FileUtils.createModelVersionDirectory("catalog", "schema", "my-model");

    assertThat(tablePath).isEqualTo("file:///tmp/random/catalog/schema/tables/table/");
    assertThat(volumePath).isEqualTo("file:///tmp/random/volume/");
    assertThat(modelPath).isEqualTo("file:///tmp/random/catalog/schema/models/my-model/");
    assertThat(modelVersionPath)
        .isEqualTo("file:///tmp/random/catalog/schema/models/my-model/versions/");

    FileUtils.deleteDirectory(tablePath);
    FileUtils.deleteDirectory(volumePath);
    FileUtils.deleteDirectory(modelVersionPath);
    FileUtils.deleteDirectory(modelPath);

    assertThatThrownBy(() -> FileUtils.createTableDirectory("..", "schema", "table"))
        .isInstanceOf(BaseException.class);

    assertThatThrownBy(
            () -> {
              FileUtils.createVolumeDirectory("..");
            })
        .isInstanceOf(BaseException.class);

    assertThatThrownBy(
            () -> {
              FileUtils.createRegisteredModelDirectory("..", "schema", "model");
            })
        .isInstanceOf(BaseException.class);

    assertThatThrownBy(
            () -> {
              FileUtils.createModelVersionDirectory("..", "schema", "model");
            })
        .isInstanceOf(BaseException.class);
  }
}
