package io.unitycatalog.server.persist.utils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.server.exception.BaseException;
import java.util.UUID;
import org.junit.jupiter.api.Test;

public class FileOperationsTest {

  @Test
  public void testToStandardizedURIString() {
    assertThat(FileOperations.toStandardizedURIString("s3://my-bucket///"))
        .isEqualTo("s3://my-bucket");
    assertThat(FileOperations.toStandardizedURIString("s3://my-bucket/"))
        .isEqualTo("s3://my-bucket");
    assertThat(FileOperations.toStandardizedURIString("s3://my-bucket"))
        .isEqualTo("s3://my-bucket");
    assertThat(FileOperations.toStandardizedURIString("s3://my-bucket/my-file"))
        .isEqualTo("s3://my-bucket/my-file");
    assertThat(FileOperations.toStandardizedURIString("s3://my-bucket/my-file/"))
        .isEqualTo("s3://my-bucket/my-file");
    assertThat(FileOperations.toStandardizedURIString("s3://my-bucket/my-file///"))
        .isEqualTo("s3://my-bucket/my-file");
    assertThat(FileOperations.toStandardizedURIString("s3://my-bucket///my-file"))
        .isEqualTo("s3://my-bucket/my-file");

    assertThat(
            FileOperations.toStandardizedURIString(
                "abfs://my-container@my-storage.dfs.core.windows.net///"))
        .isEqualTo("abfs://my-container@my-storage.dfs.core.windows.net");
    assertThat(
            FileOperations.toStandardizedURIString(
                "abfs://my-container@my-storage.dfs.core.windows.net/"))
        .isEqualTo("abfs://my-container@my-storage.dfs.core.windows.net");
    assertThat(
            FileOperations.toStandardizedURIString(
                "abfs://my-container@my-storage.dfs.core.windows.net"))
        .isEqualTo("abfs://my-container@my-storage.dfs.core.windows.net");
    assertThat(
            FileOperations.toStandardizedURIString(
                "abfs://my-container@my-storage.dfs.core.windows.net/my-file"))
        .isEqualTo("abfs://my-container@my-storage.dfs.core.windows.net/my-file");
    assertThat(
            FileOperations.toStandardizedURIString(
                "abfs://my-container@my-storage.dfs.core.windows.net/my-file/"))
        .isEqualTo("abfs://my-container@my-storage.dfs.core.windows.net/my-file");
    assertThat(
            FileOperations.toStandardizedURIString(
                "abfs://my-container@my-storage.dfs.core.windows.net/my-file///"))
        .isEqualTo("abfs://my-container@my-storage.dfs.core.windows.net/my-file");
    assertThat(
            FileOperations.toStandardizedURIString(
                "abfs://my-container@my-storage.dfs.core.windows.net///my-file"))
        .isEqualTo("abfs://my-container@my-storage.dfs.core.windows.net/my-file");

    assertThat(FileOperations.toStandardizedURIString("gs://my-bucket///"))
        .isEqualTo("gs://my-bucket");
    assertThat(FileOperations.toStandardizedURIString("gs://my-bucket/"))
        .isEqualTo("gs://my-bucket");
    assertThat(FileOperations.toStandardizedURIString("gs://my-bucket"))
        .isEqualTo("gs://my-bucket");
    assertThat(FileOperations.toStandardizedURIString("gs://my-bucket/my-file"))
        .isEqualTo("gs://my-bucket/my-file");
    assertThat(FileOperations.toStandardizedURIString("gs://my-bucket/my-file/"))
        .isEqualTo("gs://my-bucket/my-file");
    assertThat(FileOperations.toStandardizedURIString("gs://my-bucket/my-file///"))
        .isEqualTo("gs://my-bucket/my-file");
    assertThat(FileOperations.toStandardizedURIString("gs://my-bucket///my-file"))
        .isEqualTo("gs://my-bucket/my-file");

    assertThatThrownBy(() -> FileOperations.toStandardizedURIString("ftp://example.com/file"))
        .isInstanceOf(BaseException.class);

    assertThat(FileOperations.toStandardizedURIString("file:/tmp/mydir/"))
        .isEqualTo("file:///tmp/mydir");
    assertThat(FileOperations.toStandardizedURIString("file:/tmp/mydir//////"))
        .isEqualTo("file:///tmp/mydir");
    assertThat(FileOperations.toStandardizedURIString("file:/tmp/mydir"))
        .isEqualTo("file:///tmp/mydir");
    assertThat(FileOperations.toStandardizedURIString("file:/tmp//")).isEqualTo("file:///tmp");
    assertThat(FileOperations.toStandardizedURIString("file:/tmp/")).isEqualTo("file:///tmp");
    assertThat(FileOperations.toStandardizedURIString("file:/tmp")).isEqualTo("file:///tmp");
    assertThat(FileOperations.toStandardizedURIString("file://tmp")).isEqualTo("file:///tmp");
    assertThat(FileOperations.toStandardizedURIString("file:///tmp")).isEqualTo("file:///tmp");
    assertThat(FileOperations.toStandardizedURIString("file:////tmp")).isEqualTo("file:///tmp");
    assertThat(FileOperations.toStandardizedURIString("file:/")).isEqualTo("file:///");
    assertThat(FileOperations.toStandardizedURIString("file://///")).isEqualTo("file:///");

    assertThat(FileOperations.toStandardizedURIString("/tmp/mydir/"))
        .isEqualTo("file:///tmp/mydir");
    assertThat(FileOperations.toStandardizedURIString("/tmp/mydir//////"))
        .isEqualTo("file:///tmp/mydir");
    assertThat(FileOperations.toStandardizedURIString("/tmp/mydir")).isEqualTo("file:///tmp/mydir");
    assertThat(FileOperations.toStandardizedURIString("/tmp//")).isEqualTo("file:///tmp");
    assertThat(FileOperations.toStandardizedURIString("/tmp/")).isEqualTo("file:///tmp");
    assertThat(FileOperations.toStandardizedURIString("/tmp")).isEqualTo("file:///tmp");
    assertThat(FileOperations.toStandardizedURIString("//tmp")).isEqualTo("file:///tmp");
    assertThat(FileOperations.toStandardizedURIString("///tmp")).isEqualTo("file:///tmp");
    assertThat(FileOperations.toStandardizedURIString("////tmp")).isEqualTo("file:///tmp");
    assertThat(FileOperations.toStandardizedURIString("/")).isEqualTo("file:///");
    assertThat(FileOperations.toStandardizedURIString("/////")).isEqualTo("file:///");

    String uuid = UUID.randomUUID().toString();
    assertThat(FileOperations.toStandardizedURIString("/tmp/tables/" + uuid))
        .isEqualTo("file:///tmp/tables/" + uuid);
  }

  @Test
  public void testRemoveExtraSlashes() {
    assertThat(FileOperations.removeExtraSlashes(null)).isEqualTo(null);
    assertThat(FileOperations.removeExtraSlashes("")).isEqualTo("");
    assertThat(FileOperations.removeExtraSlashes("///////")).isEqualTo("/");
    assertThat(FileOperations.removeExtraSlashes("//a/////////")).isEqualTo("/a");
    assertThat(FileOperations.removeExtraSlashes("///a/b///")).isEqualTo("/a/b");
    assertThat(FileOperations.removeExtraSlashes("a/b")).isEqualTo("a/b");
    assertThat(FileOperations.removeExtraSlashes("///a/b")).isEqualTo("/a/b");
    assertThat(FileOperations.removeExtraSlashes("a/b////")).isEqualTo("a/b");
  }
}
