package io.unitycatalog.spark.fs;

import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.spark.UCHadoopConf;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

class CredScopedKeyTest {

  // ── PathCredScopedKey ────────────────────────────────────────────────────

  @Test
  void pathKey_equalWhenSamePathAndOp() {
    assertThat(new CredScopedKey.PathCredScopedKey("s3://b/p", "READ"))
        .isEqualTo(new CredScopedKey.PathCredScopedKey("s3://b/p", "READ"))
        .hasSameHashCodeAs(new CredScopedKey.PathCredScopedKey("s3://b/p", "READ"));
  }

  @Test
  void pathKey_notEqualWhenDifferentOp() {
    assertThat(new CredScopedKey.PathCredScopedKey("s3://b/p", "READ"))
        .isNotEqualTo(new CredScopedKey.PathCredScopedKey("s3://b/p", "WRITE"));
  }

  @Test
  void pathKey_notEqualWhenDifferentPath() {
    assertThat(new CredScopedKey.PathCredScopedKey("s3://b/a", "READ"))
        .isNotEqualTo(new CredScopedKey.PathCredScopedKey("s3://b/b", "READ"));
  }

  // ── TableCredScopedKey ───────────────────────────────────────────────────

  @Test
  void tableKey_equalWhenSameIdAndOp() {
    assertThat(new CredScopedKey.TableCredScopedKey("tid-1", "READ_WRITE"))
        .isEqualTo(new CredScopedKey.TableCredScopedKey("tid-1", "READ_WRITE"))
        .hasSameHashCodeAs(new CredScopedKey.TableCredScopedKey("tid-1", "READ_WRITE"));
  }

  @Test
  void tableKey_notEqualWhenDifferentId() {
    assertThat(new CredScopedKey.TableCredScopedKey("tid-1", "READ_WRITE"))
        .isNotEqualTo(new CredScopedKey.TableCredScopedKey("tid-2", "READ_WRITE"));
  }

  // ── NoopCredScopedKey ────────────────────────────────────────────────────

  @Test
  void noopKey_equalWhenSameSchemeAndAuthority() {
    URI uri = URI.create("s3://my-bucket/path");
    assertThat(new CredScopedKey.NoopCredScopedKey(uri, new Configuration()))
        .isEqualTo(new CredScopedKey.NoopCredScopedKey(uri, new Configuration()))
        .hasSameHashCodeAs(new CredScopedKey.NoopCredScopedKey(uri, new Configuration()));
  }

  @Test
  void noopKey_notEqualWhenDifferentAuthority() {
    assertThat(
            new CredScopedKey.NoopCredScopedKey(URI.create("s3://bucket-a"), new Configuration()))
        .isNotEqualTo(
            new CredScopedKey.NoopCredScopedKey(URI.create("s3://bucket-b"), new Configuration()));
  }

  // ── CredScopedKey.create() factory ──────────────────────────────────────

  @Test
  void create_returnsTableKey() {
    Configuration conf = new Configuration();
    conf.set(UCHadoopConf.UC_CREDENTIALS_TYPE_KEY, UCHadoopConf.UC_CREDENTIALS_TYPE_TABLE_VALUE);
    conf.set(UCHadoopConf.UC_TABLE_ID_KEY, "tid");
    conf.set(UCHadoopConf.UC_TABLE_OPERATION_KEY, "READ");

    assertThat(CredScopedKey.create(URI.create("s3://b"), conf))
        .isInstanceOf(CredScopedKey.TableCredScopedKey.class)
        .isEqualTo(new CredScopedKey.TableCredScopedKey("tid", "READ"));
  }

  @Test
  void create_returnsPathKey() {
    Configuration conf = new Configuration();
    conf.set(UCHadoopConf.UC_CREDENTIALS_TYPE_KEY, UCHadoopConf.UC_CREDENTIALS_TYPE_PATH_VALUE);
    conf.set(UCHadoopConf.UC_PATH_KEY, "s3://b/p");
    conf.set(UCHadoopConf.UC_PATH_OPERATION_KEY, "WRITE");

    assertThat(CredScopedKey.create(URI.create("s3://b"), conf))
        .isInstanceOf(CredScopedKey.PathCredScopedKey.class)
        .isEqualTo(new CredScopedKey.PathCredScopedKey("s3://b/p", "WRITE"));
  }

  @Test
  void create_returnsNoopKeyWhenNoType() {
    assertThat(CredScopedKey.create(URI.create("s3://b"), new Configuration()))
        .isInstanceOf(CredScopedKey.NoopCredScopedKey.class);
  }
}
