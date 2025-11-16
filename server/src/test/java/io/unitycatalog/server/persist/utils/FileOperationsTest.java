package io.unitycatalog.server.persist.utils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.server.exception.BaseException;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class FileOperationsTest {

  @Test
  public void testToStandardizedURIString() {
    assertThat(FileOperations.toStandardizedURIString("s3://my-bucket/my-file"))
        .isEqualTo("s3://my-bucket/my-file");
    assertThat(
            FileOperations.toStandardizedURIString(
                "abfs://my-container@my-storage.dfs.core.windows.net/my-file"))
        .isEqualTo("abfs://my-container@my-storage.dfs.core.windows.net/my-file");
    assertThat(FileOperations.toStandardizedURIString("gs://my-bucket/my-file"))
        .isEqualTo("gs://my-bucket/my-file");
    assertThatThrownBy(() -> FileOperations.toStandardizedURIString("ftp://example.com/file"))
        .isInstanceOf(BaseException.class);

    assertThat(FileOperations.toStandardizedURIString("file:/tmp/mydir/"))
        .isEqualTo("file:///tmp/mydir/");
    assertThat(FileOperations.toStandardizedURIString("file:/tmp/mydir"))
        .isEqualTo("file:///tmp/mydir");
    assertThat(FileOperations.toStandardizedURIString("file:/tmp/")).isEqualTo("file:///tmp/");
    assertThat(FileOperations.toStandardizedURIString("file:/tmp")).isEqualTo("file:///tmp");
    assertThat(FileOperations.toStandardizedURIString("file:/")).isEqualTo("file:///");

    String uuid = UUID.randomUUID().toString();
    assertThat(FileOperations.toStandardizedURIString("/tmp/tables/" + uuid))
        .isEqualTo("file:///tmp/tables/" + uuid);
  }
}
