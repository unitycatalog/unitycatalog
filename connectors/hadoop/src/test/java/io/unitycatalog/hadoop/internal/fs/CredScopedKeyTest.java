package io.unitycatalog.hadoop.internal.fs;

import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.hadoop.internal.UCHadoopConf;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

class CredScopedKeyTest {

  @Test
  void pathKeyEqualWhenSamePathAndOp() {
    assertThat(new CredScopedKey.PathCredScopedKey("s3://b/p", "READ"))
        .isEqualTo(new CredScopedKey.PathCredScopedKey("s3://b/p", "READ"))
        .hasSameHashCodeAs(new CredScopedKey.PathCredScopedKey("s3://b/p", "READ"));
  }

  @Test
  void pathKeyNotEqualWhenDifferentOp() {
    assertThat(new CredScopedKey.PathCredScopedKey("s3://b/p", "READ"))
        .isNotEqualTo(new CredScopedKey.PathCredScopedKey("s3://b/p", "WRITE"));
  }

  @Test
  void pathKeyNotEqualWhenDifferentPath() {
    assertThat(new CredScopedKey.PathCredScopedKey("s3://b/a", "READ"))
        .isNotEqualTo(new CredScopedKey.PathCredScopedKey("s3://b/b", "READ"));
  }

  @Test
  void tableKeyEqualWhenSameIdAndOp() {
    assertThat(new CredScopedKey.TableCredScopedKey("tid-1", "READ_WRITE"))
        .isEqualTo(new CredScopedKey.TableCredScopedKey("tid-1", "READ_WRITE"))
        .hasSameHashCodeAs(new CredScopedKey.TableCredScopedKey("tid-1", "READ_WRITE"));
  }

  @Test
  void tableKeyNotEqualWhenDifferentId() {
    assertThat(new CredScopedKey.TableCredScopedKey("tid-1", "READ_WRITE"))
        .isNotEqualTo(new CredScopedKey.TableCredScopedKey("tid-2", "READ_WRITE"));
  }

  @Test
  void defaultKeyEqualWhenSameSchemeAndAuthority() {
    URI uri = URI.create("s3://my-bucket/path");
    assertThat(new CredScopedKey.DefaultCredScopedKey(uri, new Configuration()))
        .isEqualTo(new CredScopedKey.DefaultCredScopedKey(uri, new Configuration()))
        .hasSameHashCodeAs(new CredScopedKey.DefaultCredScopedKey(uri, new Configuration()));
  }

  @Test
  void defaultKeyNotEqualWhenDifferentAuthority() {
    assertThat(
            new CredScopedKey.DefaultCredScopedKey(
                URI.create("s3://bucket-a"), new Configuration()))
        .isNotEqualTo(
            new CredScopedKey.DefaultCredScopedKey(
                URI.create("s3://bucket-b"), new Configuration()));
  }

  @Test
  void createReturnsTableKey() {
    Configuration conf = new Configuration();
    conf.set(UCHadoopConf.UC_CREDENTIALS_TYPE_KEY, UCHadoopConf.UC_CREDENTIALS_TYPE_TABLE_VALUE);
    conf.set(UCHadoopConf.UC_TABLE_ID_KEY, "tid");
    conf.set(UCHadoopConf.UC_TABLE_OPERATION_KEY, "READ");

    assertThat(CredScopedKey.create(URI.create("s3://b"), conf))
        .isInstanceOf(CredScopedKey.TableCredScopedKey.class)
        .isEqualTo(new CredScopedKey.TableCredScopedKey("tid", "READ"));
  }

  @Test
  void createReturnsPathKey() {
    Configuration conf = new Configuration();
    conf.set(UCHadoopConf.UC_CREDENTIALS_TYPE_KEY, UCHadoopConf.UC_CREDENTIALS_TYPE_PATH_VALUE);
    conf.set(UCHadoopConf.UC_PATH_KEY, "s3://b/p");
    conf.set(UCHadoopConf.UC_PATH_OPERATION_KEY, "WRITE");

    assertThat(CredScopedKey.create(URI.create("s3://b"), conf))
        .isInstanceOf(CredScopedKey.PathCredScopedKey.class)
        .isEqualTo(new CredScopedKey.PathCredScopedKey("s3://b/p", "WRITE"));
  }

  @Test
  void createReturnsDefaultKeyWhenNoType() {
    assertThat(CredScopedKey.create(URI.create("s3://b"), new Configuration()))
        .isInstanceOf(CredScopedKey.DefaultCredScopedKey.class);
  }
}
