package io.unitycatalog.server.utils;

import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.persist.utils.FileUtils;
import org.junit.Assert;
import org.junit.Test;

public class FileUtilsTest {

  @Test
  public void testFileUtils() {

    System.setProperty("storageRoot", "/tmp");

    String tablePath = FileUtils.createTableDirectory("catalog", "schema", "table");
    String volumePath = FileUtils.createVolumeDirectory("volume");

    assertThat(tablePath).isEqualTo("file:///tmp/catalog/schema/tables/table/");
    assertThat(volumePath).isEqualTo("file:///tmp/volume/");

    FileUtils.deleteDirectory(tablePath);
    FileUtils.deleteDirectory(volumePath);

    System.setProperty("storageRoot", "file:///tmp/random");

    tablePath = FileUtils.createTableDirectory("catalog", "schema", "table");
    volumePath = FileUtils.createVolumeDirectory("volume");

    assertThat(tablePath).isEqualTo("file:///tmp/random/catalog/schema/tables/table/");
    assertThat(volumePath).isEqualTo("file:///tmp/random/volume/");

    FileUtils.deleteDirectory(tablePath);
    FileUtils.deleteDirectory(volumePath);

    Assert.assertThrows(
        BaseException.class,
        () -> {
          FileUtils.createTableDirectory("..", "schema", "table");
        });

    Assert.assertThrows(
        BaseException.class,
        () -> {
          FileUtils.createVolumeDirectory("..");
        });
  }
}
