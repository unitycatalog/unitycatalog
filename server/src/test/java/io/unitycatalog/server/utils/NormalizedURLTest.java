package io.unitycatalog.server.utils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.unitycatalog.server.exception.BaseException;
import java.net.URI;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

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
    assertThat(NormalizedURL.from((String) null)).isNull();
    assertThat(NormalizedURL.from((URI) null)).isNull();
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

  @ParameterizedTest
  @ValueSource(
      strings = {
        "s3://bucket",
        "s3://bucket/",
        "s3://bucket///",
        "s3://bucket/.",
        "s3://bucket/a/..",
        "s3://bucket/%2F",
        "s3://bucket?query",
        "s3://bucket#fragment",
        "gs://bucket/",
        "abfs://container@account.dfs.core.windows.net/",
        "abfss://container@account.dfs.core.windows.net/"
      })
  public void testIdentifiesCloudStorageRoots(String location) {
    assertThat(NormalizedURL.from(location).isCloudStorageRoot()).isTrue();
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "s3://bucket/path",
        "gs://bucket/path",
        "abfs://container@account.dfs.core.windows.net/path",
        "abfss://container@account.dfs.core.windows.net/path",
        "file:///"
      })
  public void testDoesNotIdentifyScopedOrLocalLocationsAsCloudStorageRoots(String location) {
    assertThat(NormalizedURL.from(location).isCloudStorageRoot()).isFalse();
  }
}
