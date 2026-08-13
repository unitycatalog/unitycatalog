package io.unitycatalog.server.utils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.server.exception.BaseException;
import java.net.URI;
import org.junit.jupiter.api.Test;

public class UriSchemeTest {

  @Test
  public void testFromURI() {
    assertThat(UriScheme.fromURI(URI.create("s3://bucket/path"))).isEqualTo(UriScheme.S3);
    assertThat(UriScheme.fromURI(URI.create("gs://bucket/path"))).isEqualTo(UriScheme.GS);
    assertThat(UriScheme.fromURI(URI.create("abfs://container@account/path")))
        .isEqualTo(UriScheme.ABFS);
    assertThat(UriScheme.fromURI(URI.create("abfss://container@account/path")))
        .isEqualTo(UriScheme.ABFSS);
    assertThat(UriScheme.fromURI(URI.create("file:///tmp/path"))).isEqualTo(UriScheme.FILE);
    assertThat(UriScheme.fromURI(URI.create("/tmp/path"))).isEqualTo(UriScheme.NULL);
  }

  @Test
  public void testUnsupportedScheme() {
    assertThatThrownBy(() -> UriScheme.fromURI(URI.create("ftp://example.com/file")))
        .isInstanceOf(BaseException.class)
        .hasMessageContaining("Unsupported URI scheme");
  }

  @Test
  public void testToString() {
    assertThat(UriScheme.S3.toString()).isEqualTo("s3");
    assertThat(UriScheme.GS.toString()).isEqualTo("gs");
    assertThat(UriScheme.ABFS.toString()).isEqualTo("abfs");
    assertThat(UriScheme.ABFSS.toString()).isEqualTo("abfss");
    assertThat(UriScheme.FILE.toString()).isEqualTo("file");
    assertThat(UriScheme.NULL.toString()).isNull();
  }
}
