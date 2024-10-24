package io.unitycatalog.server.utils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.persist.utils.FileUtils;
import java.util.UUID;
import org.junit.jupiter.api.Test;

public class FileUtilsTest {

  @Test
  public void testFileUtils() {

    System.setProperty("storageRoot", "/tmp");
    String tableId = UUID.randomUUID().toString();
    String tablePath = FileUtils.createTableDirectory(tableId);
    assertThat(tablePath).isEqualTo("file:///tmp/tables/" + tableId);

    System.setProperty("storageRoot", "file:///tmp/random");
    tablePath = FileUtils.createTableDirectory(tableId);
    assertThat(tablePath).isEqualTo("file:///tmp/random/tables/" + tableId);
    assertThatThrownBy(() -> FileUtils.createTableDirectory(".."))
        .isInstanceOf(BaseException.class);
  }
}
