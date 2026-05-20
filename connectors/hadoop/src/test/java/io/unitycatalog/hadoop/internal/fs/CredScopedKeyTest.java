package io.unitycatalog.hadoop.internal.fs;

import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.hadoop.internal.UCDeltaTableIdentifier;
import io.unitycatalog.hadoop.internal.UCHadoopConfConstants;
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
    conf.set(
        UCHadoopConfConstants.UC_CREDENTIALS_TYPE_KEY,
        UCHadoopConfConstants.UC_CREDENTIALS_TYPE_TABLE_VALUE);
    conf.set(UCHadoopConfConstants.UC_TABLE_ID_KEY, "tid");
    conf.set(UCHadoopConfConstants.UC_TABLE_OPERATION_KEY, "READ");

    assertThat(CredScopedKey.create(URI.create("s3://b"), conf))
        .isInstanceOf(CredScopedKey.TableCredScopedKey.class)
        .isEqualTo(new CredScopedKey.TableCredScopedKey("tid", "READ"));
  }

  @Test
  void createReturnsPathKey() {
    Configuration conf = new Configuration();
    conf.set(
        UCHadoopConfConstants.UC_CREDENTIALS_TYPE_KEY,
        UCHadoopConfConstants.UC_CREDENTIALS_TYPE_PATH_VALUE);
    conf.set(UCHadoopConfConstants.UC_PATH_KEY, "s3://b/p");
    conf.set(UCHadoopConfConstants.UC_PATH_OPERATION_KEY, "WRITE");

    assertThat(CredScopedKey.create(URI.create("s3://b"), conf))
        .isInstanceOf(CredScopedKey.PathCredScopedKey.class)
        .isEqualTo(new CredScopedKey.PathCredScopedKey("s3://b/p", "WRITE"));
  }

  @Test
  void createReturnsDefaultKeyWhenNoType() {
    assertThat(CredScopedKey.create(URI.create("s3://b"), new Configuration()))
        .isInstanceOf(CredScopedKey.DefaultCredScopedKey.class);
  }

  @Test
  void deltaTableKeyEqualWhenSameFields() {
    UCDeltaTableIdentifier id = UCDeltaTableIdentifier.of("cat", "sch", "tbl");
    assertThat(new CredScopedKey.DeltaTableCredScopedKey(id, "READ_WRITE", "s3://b/t"))
        .isEqualTo(new CredScopedKey.DeltaTableCredScopedKey(id, "READ_WRITE", "s3://b/t"))
        .hasSameHashCodeAs(new CredScopedKey.DeltaTableCredScopedKey(id, "READ_WRITE", "s3://b/t"));
  }

  @Test
  void deltaTableKeyNotEqualWhenDifferentCatalog() {
    assertThat(
            new CredScopedKey.DeltaTableCredScopedKey(
                UCDeltaTableIdentifier.of("cat1", "sch", "tbl"), "READ", "s3://b/t"))
        .isNotEqualTo(
            new CredScopedKey.DeltaTableCredScopedKey(
                UCDeltaTableIdentifier.of("cat2", "sch", "tbl"), "READ", "s3://b/t"));
  }

  @Test
  void deltaTableKeyNotEqualWhenDifferentOperation() {
    UCDeltaTableIdentifier id = UCDeltaTableIdentifier.of("cat", "sch", "tbl");
    assertThat(new CredScopedKey.DeltaTableCredScopedKey(id, "READ", "s3://b/t"))
        .isNotEqualTo(new CredScopedKey.DeltaTableCredScopedKey(id, "READ_WRITE", "s3://b/t"));
  }

  @Test
  void deltaTableKeyNotEqualWhenDifferentLocation() {
    UCDeltaTableIdentifier id = UCDeltaTableIdentifier.of("cat", "sch", "tbl");
    assertThat(new CredScopedKey.DeltaTableCredScopedKey(id, "READ", "s3://b/t1"))
        .isNotEqualTo(new CredScopedKey.DeltaTableCredScopedKey(id, "READ", "s3://b/t2"));
  }

  @Test
  void deltaTableKeyIgnoresCredentialUid() {
    Configuration left = deltaTableConf("s3://b/t", "uid-1");
    Configuration right = deltaTableConf("s3://b/t", "uid-2");

    assertThat(CredScopedKey.create(URI.create("s3://b"), left))
        .isEqualTo(CredScopedKey.create(URI.create("s3://b"), right));
  }

  @Test
  void createReturnsDeltaTableKeyWhenDeltaApiEnabled() {
    Configuration conf = new Configuration();
    conf.set(
        UCHadoopConfConstants.UC_CREDENTIALS_TYPE_KEY,
        UCHadoopConfConstants.UC_CREDENTIALS_TYPE_TABLE_VALUE);
    conf.set(UCHadoopConfConstants.UC_DELTA_CREDENTIALS_API_ENABLED_KEY, "true");
    conf.set(UCHadoopConfConstants.UC_DELTA_CATALOG_KEY, "cat");
    conf.set(UCHadoopConfConstants.UC_DELTA_SCHEMA_KEY, "sch");
    conf.set(UCHadoopConfConstants.UC_DELTA_TABLE_NAME_KEY, "tbl");
    conf.set(UCHadoopConfConstants.UC_DELTA_LOCATION_KEY, "s3://b/tbl");
    conf.set(UCHadoopConfConstants.UC_TABLE_OPERATION_KEY, "READ_WRITE");
    conf.set(UCHadoopConfConstants.UC_CREDENTIALS_UID_KEY, "uid-1");

    UCDeltaTableIdentifier id = UCDeltaTableIdentifier.of("cat", "sch", "tbl");
    assertThat(CredScopedKey.create(URI.create("s3://b"), conf))
        .isInstanceOf(CredScopedKey.DeltaTableCredScopedKey.class)
        .isEqualTo(new CredScopedKey.DeltaTableCredScopedKey(id, "READ_WRITE", "s3://b/tbl"));
  }

  @Test
  void createReturnsTableKeyWhenDeltaApiNotEnabled() {
    Configuration conf = new Configuration();
    conf.set(
        UCHadoopConfConstants.UC_CREDENTIALS_TYPE_KEY,
        UCHadoopConfConstants.UC_CREDENTIALS_TYPE_TABLE_VALUE);
    conf.set(UCHadoopConfConstants.UC_TABLE_ID_KEY, "tid");
    conf.set(UCHadoopConfConstants.UC_TABLE_OPERATION_KEY, "READ");

    assertThat(CredScopedKey.create(URI.create("s3://b"), conf))
        .isInstanceOf(CredScopedKey.TableCredScopedKey.class)
        .isEqualTo(new CredScopedKey.TableCredScopedKey("tid", "READ"));
  }

  private static Configuration deltaTableConf(String location, String uid) {
    Configuration conf = new Configuration();
    conf.set(
        UCHadoopConfConstants.UC_CREDENTIALS_TYPE_KEY,
        UCHadoopConfConstants.UC_CREDENTIALS_TYPE_TABLE_VALUE);
    conf.set(UCHadoopConfConstants.UC_DELTA_CREDENTIALS_API_ENABLED_KEY, "true");
    conf.set(UCHadoopConfConstants.UC_DELTA_CATALOG_KEY, "cat");
    conf.set(UCHadoopConfConstants.UC_DELTA_SCHEMA_KEY, "sch");
    conf.set(UCHadoopConfConstants.UC_DELTA_TABLE_NAME_KEY, "tbl");
    conf.set(UCHadoopConfConstants.UC_DELTA_LOCATION_KEY, location);
    conf.set(UCHadoopConfConstants.UC_TABLE_OPERATION_KEY, "READ");
    conf.set(UCHadoopConfConstants.UC_CREDENTIALS_UID_KEY, uid);
    return conf;
  }
}
