package io.unitycatalog.server.utils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.unitycatalog.server.exception.BaseException;
import java.util.UUID;
import org.junit.jupiter.api.Test;

public class NormalizedURLTest {

  void assertNormalizedURL(String url, String expected) {
    assertThat(NormalizedURL.from(url).toString()).isEqualTo(expected);
  }

  @Test
  public void testToStandardizedURIString() {
    assertNormalizedURL("s3://my-bucket///", "s3://my-bucket");
    assertNormalizedURL("s3://my-bucket/", "s3://my-bucket");
    assertNormalizedURL("s3://my-bucket", "s3://my-bucket");
    assertNormalizedURL("s3://my-bucket/my-file", "s3://my-bucket/my-file");
    assertNormalizedURL("s3://my-bucket/my-file/", "s3://my-bucket/my-file");
    assertNormalizedURL("s3://my-bucket/my-file///", "s3://my-bucket/my-file");
    assertNormalizedURL("s3://my-bucket///my-file", "s3://my-bucket/my-file");

    assertNormalizedURL(
        "abfs://my-container@my-storage.dfs.core.windows.net///",
        "abfs://my-container@my-storage.dfs.core.windows.net");
    assertNormalizedURL(
        "abfs://my-container@my-storage.dfs.core.windows.net/",
        "abfs://my-container@my-storage.dfs.core.windows.net");
    assertNormalizedURL(
        "abfs://my-container@my-storage.dfs.core.windows.net",
        "abfs://my-container@my-storage.dfs.core.windows.net");
    assertNormalizedURL(
        "abfs://my-container@my-storage.dfs.core.windows.net/my-file",
        "abfs://my-container@my-storage.dfs.core.windows.net/my-file");
    assertNormalizedURL(
        "abfs://my-container@my-storage.dfs.core.windows.net/my-file/",
        "abfs://my-container@my-storage.dfs.core.windows.net/my-file");
    assertNormalizedURL(
        "abfs://my-container@my-storage.dfs.core.windows.net/my-file///",
        "abfs://my-container@my-storage.dfs.core.windows.net/my-file");
    assertNormalizedURL(
        "abfs://my-container@my-storage.dfs.core.windows.net///my-file",
        "abfs://my-container@my-storage.dfs.core.windows.net/my-file");

    assertNormalizedURL("gs://my-bucket///", "gs://my-bucket");
    assertNormalizedURL("gs://my-bucket/", "gs://my-bucket");
    assertNormalizedURL("gs://my-bucket", "gs://my-bucket");
    assertNormalizedURL("gs://my-bucket/my-file", "gs://my-bucket/my-file");
    assertNormalizedURL("gs://my-bucket/my-file/", "gs://my-bucket/my-file");
    assertNormalizedURL("gs://my-bucket/my-file///", "gs://my-bucket/my-file");
    assertNormalizedURL("gs://my-bucket///my-file", "gs://my-bucket/my-file");

    assertThatThrownBy(() -> NormalizedURL.from("ftp://example.com/file"))
        .isInstanceOf(BaseException.class);

    assertNormalizedURL("file:/tmp/mydir/", "file:///tmp/mydir");
    assertNormalizedURL("file:/tmp/mydir//////", "file:///tmp/mydir");
    assertNormalizedURL("file:/tmp/mydir", "file:///tmp/mydir");
    assertNormalizedURL("file:/tmp//", "file:///tmp");
    assertNormalizedURL("file:/tmp/", "file:///tmp");
    assertNormalizedURL("file:/tmp", "file:///tmp");
    assertNormalizedURL("file://tmp", "file:///tmp");
    assertNormalizedURL("file:///tmp", "file:///tmp");
    assertNormalizedURL("file:////tmp", "file:///tmp");
    assertNormalizedURL("file:/", "file:///");
    assertNormalizedURL("file://///", "file:///");

    assertNormalizedURL("/tmp/mydir/", "file:///tmp/mydir");
    assertNormalizedURL("/tmp/mydir//////", "file:///tmp/mydir");
    assertNormalizedURL("/tmp/mydir", "file:///tmp/mydir");
    assertNormalizedURL("/tmp//", "file:///tmp");
    assertNormalizedURL("/tmp/", "file:///tmp");
    assertNormalizedURL("/tmp", "file:///tmp");
    assertNormalizedURL("//tmp", "file:///tmp");
    assertNormalizedURL("///tmp", "file:///tmp");
    assertNormalizedURL("////tmp", "file:///tmp");
    assertNormalizedURL("/", "file:///");
    assertNormalizedURL("/////", "file:///");

    String uuid = UUID.randomUUID().toString();
    assertNormalizedURL("/tmp/tables/" + uuid, "file:///tmp/tables/" + uuid);

    assertThrows(BaseException.class, () -> NormalizedURL.from(""));
    assertThrows(BaseException.class, () -> NormalizedURL.from("  "));
    assertThat(NormalizedURL.from(null)).isNull();
  }

  @Test
  public void testGetStorageBase() {
    assertThat(NormalizedURL.from("s3://bucket/path").getStorageBase())
        .isEqualTo(NormalizedURL.from("s3://bucket"));
    assertThat(NormalizedURL.from("s3://bucket/path/to/file").getStorageBase())
        .isEqualTo(NormalizedURL.from("s3://bucket"));
    assertThat(NormalizedURL.from("gs://bucket/path").getStorageBase())
        .isEqualTo(NormalizedURL.from("gs://bucket"));
    assertThat(
            NormalizedURL.from("abfs://container@account.dfs.core.windows.net/path")
                .getStorageBase())
        .isEqualTo(NormalizedURL.from("abfs://container@account.dfs.core.windows.net"));
    assertThat(
            NormalizedURL.from("abfss://container@account.dfs.core.windows.net/path")
                .getStorageBase())
        .isEqualTo(NormalizedURL.from("abfss://container@account.dfs.core.windows.net"));
  }
}
