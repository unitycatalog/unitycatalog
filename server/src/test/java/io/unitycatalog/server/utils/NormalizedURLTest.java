package io.unitycatalog.server.utils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.server.exception.BaseException;
import java.util.UUID;
import org.junit.jupiter.api.Test;

public class NormalizedURLTest {

  void assertNormalizedURL(String url, String expected) {
    assertThat(new NormalizedURL(url).toString()).isEqualTo(expected);
  }

  @Test
  public void testNullURLThrowsException() {
    assertThatThrownBy(() -> new NormalizedURL(null))
        .isInstanceOf(BaseException.class)
        .hasMessageContaining("URL cannot be null");
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

    assertThatThrownBy(() -> new NormalizedURL("ftp://example.com/file"))
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
  }

  @Test
  public void testRemoveExtraSlashes() {
    assertThat(NormalizedURL.removeExtraSlashes(null)).isEqualTo(null);
    assertThat(NormalizedURL.removeExtraSlashes("")).isEqualTo("");
    assertThat(NormalizedURL.removeExtraSlashes("///////")).isEqualTo("/");
    assertThat(NormalizedURL.removeExtraSlashes("//a/////////")).isEqualTo("/a");
    assertThat(NormalizedURL.removeExtraSlashes("///a/b///")).isEqualTo("/a/b");
    assertThat(NormalizedURL.removeExtraSlashes("a/b")).isEqualTo("a/b");
    assertThat(NormalizedURL.removeExtraSlashes("///a/b")).isEqualTo("/a/b");
    assertThat(NormalizedURL.removeExtraSlashes("a/b////")).isEqualTo("a/b");
  }
}
