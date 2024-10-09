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
    String volumePath = FileUtils.createEntityDirectory("volume");
    assertThat(volumePath).isEqualTo("file:///tmp/volume/");
    FileUtils.deleteDirectory(volumePath);

    System.setProperty("storageRoot", "file:///tmp/random");
    volumePath = FileUtils.createEntityDirectory("volume");
    assertThat(volumePath).isEqualTo("file:///tmp/random/volume/");
    FileUtils.deleteDirectory(volumePath);

    assertThatThrownBy(
            () -> {
              FileUtils.createEntityDirectory("..");
            })
        .isInstanceOf(BaseException.class);
  }
}
