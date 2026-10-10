package io.unitycatalog.server.utils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
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
  public void testLocalFileURIEscapes() {
    // Escapes that keep one plain path are kept as sent.
    assertNormalizedURL("file:///data/%74", "file:///data/%74");
    assertNormalizedURL("file:///data/my%20table", "file:///data/my%20table");
    assertNormalizedURL("file:///data/a%25b", "file:///data/a%25b");
    assertNormalizedURL("file:///data/a%2e%2eb", "file:///data/a%2e%2eb");
    assertNormalizedURL("file:///data/a%2E%2Eb", "file:///data/a%2E%2Eb");
    // Decoded once, "%252e%252e" is the name "%2e%2e", not "..".
    assertNormalizedURL("file:///data/%252e%252e/x", "file:///data/%252e%252e/x");
    // Without a scheme the path is taken literally: "%2F" is three characters of a name.
    assertNormalizedURL("/data/a%2Fb", "file:///data/a%252Fb");

    // An encoded '/' or NUL, or an encoded dot segment, in either case.
    assertRejected("file:///data/a%2Fb");
    assertRejected("file:///data/a%2fb");
    assertRejected("file:///data/a%2F");
    assertRejected("file:///data/root/%2e%2e/etc");
    assertRejected("file:///data/root/%2E%2E/etc");
    assertRejected("file:///data/root/%2e%2E/etc");
    assertRejected("file:///data/root/.%2e/etc");
    assertRejected("file:///data/root/%2E./etc");
    assertRejected("file:///data/root/%2e/etc");
    assertRejected("file:///data/root/%2e%2e");
    // Plain dot segments collapse, but one above the root is not a plain path, with or without a
    // scheme.
    assertNormalizedURL("file:///data/a/../b", "file:///data/b");
    assertNormalizedURL("/data/a/../b", "file:///data/b");
    assertRejected("file:///../etc");
    assertRejected("file:///data/../../etc");
    assertRejected("/../etc");
    assertRejected("file:///data/a%00b");
    // A host is the first directory of the path, so its escapes and dots count too.
    assertNormalizedURL("file://data/../etc/x", "file:///etc/x");
    assertRejected("file://data%2Froot%2F..%2F..%2Fetc/x");
    assertRejected("file://%2e%2e/etc");
    assertRejected("file://[::1]/x");
    // Not a local path.
    assertRejected("file:///data/t?x=1");
    assertRejected("file:///data/t#frag");
    assertRejected("file:a/b");

    // Cloud locations keep their escapes, since an object key may contain a literal '%'.
    assertNormalizedURL("s3://bucket/a%2Fb", "s3://bucket/a%2Fb");
    assertNormalizedURL("gs://bucket/%2e%2e/x", "gs://bucket/%2e%2e/x");
  }

  private void assertRejected(String url) {
    assertThatThrownBy(() -> NormalizedURL.from(url))
        .isInstanceOf(BaseException.class)
        .hasMessageContaining(url)
        .extracting(e -> ((BaseException) e).getErrorCode())
        .isEqualTo(ErrorCode.INVALID_ARGUMENT);
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
