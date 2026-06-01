package io.unitycatalog.hadoop.internal.id;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;

import io.unitycatalog.hadoop.internal.UCDeltaTableIdentifier;
import io.unitycatalog.hadoop.internal.UCHadoopConfConstants;
import java.net.URI;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

class CredIdTest {

  @Test
  void pathKeyEqualWhenSamePathAndOp() {
    assertThat(new PathCredId("s3://b/p", "READ"))
        .isEqualTo(new PathCredId("s3://b/p", "READ"))
        .hasSameHashCodeAs(new PathCredId("s3://b/p", "READ"));
  }

  @Test
  void pathKeyNotEqualWhenDifferentOp() {
    assertThat(new PathCredId("s3://b/p", "READ"))
        .isNotEqualTo(new PathCredId("s3://b/p", "WRITE"));
  }

  @Test
  void pathKeyNotEqualWhenDifferentPath() {
    assertThat(new PathCredId("s3://b/a", "READ")).isNotEqualTo(new PathCredId("s3://b/b", "READ"));
  }

  @Test
  void tableKeyEqualWhenSameIdAndOp() {
    assertThat(new TableCredId("tid-1", "READ_WRITE"))
        .isEqualTo(new TableCredId("tid-1", "READ_WRITE"))
        .hasSameHashCodeAs(new TableCredId("tid-1", "READ_WRITE"));
  }

  @Test
  void tableKeyNotEqualWhenDifferentId() {
    assertThat(new TableCredId("tid-1", "READ_WRITE"))
        .isNotEqualTo(new TableCredId("tid-2", "READ_WRITE"));
  }

  @Test
  void defaultKeyEqualWhenSameSchemeAndAuthority() {
    URI uri = URI.create("s3://my-bucket/path");
    assertThat(new DefaultCredId(uri, new Configuration()))
        .isEqualTo(new DefaultCredId(uri, new Configuration()))
        .hasSameHashCodeAs(new DefaultCredId(uri, new Configuration()));
  }

  @Test
  void defaultKeyNotEqualWhenDifferentAuthority() {
    assertThat(new DefaultCredId(URI.create("s3://bucket-a"), new Configuration()))
        .isNotEqualTo(new DefaultCredId(URI.create("s3://bucket-b"), new Configuration()));
  }

  @Test
  void createReturnsTableKey() {
    Configuration conf = new Configuration();
    conf.set(
        UCHadoopConfConstants.UC_CREDENTIALS_TYPE_KEY,
        UCHadoopConfConstants.UC_CREDENTIALS_TYPE_TABLE_VALUE);
    conf.set(UCHadoopConfConstants.UC_TABLE_ID_KEY, "tid");
    conf.set(UCHadoopConfConstants.UC_TABLE_OPERATION_KEY, "READ");

    assertThat(CredId.create(conf))
        .isInstanceOf(TableCredId.class)
        .isEqualTo(new TableCredId("tid", "READ"));
  }

  @Test
  void createReturnsPathKey() {
    Configuration conf = new Configuration();
    conf.set(
        UCHadoopConfConstants.UC_CREDENTIALS_TYPE_KEY,
        UCHadoopConfConstants.UC_CREDENTIALS_TYPE_PATH_VALUE);
    conf.set(UCHadoopConfConstants.UC_PATH_KEY, "s3://b/p");
    conf.set(UCHadoopConfConstants.UC_PATH_OPERATION_KEY, "WRITE");

    assertThat(CredId.create(conf))
        .isInstanceOf(PathCredId.class)
        .isEqualTo(new PathCredId("s3://b/p", "WRITE"));
  }

  @Test
  void createReturnsDefaultKeyWhenNoType() {
    Configuration conf = new Configuration();
    assertThat(CredId.create(conf, () -> new DefaultCredId(URI.create("s3://b"), conf)))
        .isInstanceOf(DefaultCredId.class);
  }

  @Test
  void createThrowsWhenNoTypeAndNoFallback() {
    assertThatThrownBy(() -> CredId.create(new Configuration()))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void deltaTableKeyEqualWhenSameFields() {
    UCDeltaTableIdentifier id = UCDeltaTableIdentifier.of("cat", "sch", "tbl");
    assertThat(new DeltaTableCredId(id, "READ_WRITE", "s3://b/t"))
        .isEqualTo(new DeltaTableCredId(id, "READ_WRITE", "s3://b/t"))
        .hasSameHashCodeAs(new DeltaTableCredId(id, "READ_WRITE", "s3://b/t"));
  }

  @Test
  void deltaTableKeyNotEqualWhenDifferentCatalog() {
    assertThat(
            new DeltaTableCredId(
                UCDeltaTableIdentifier.of("cat1", "sch", "tbl"), "READ", "s3://b/t"))
        .isNotEqualTo(
            new DeltaTableCredId(
                UCDeltaTableIdentifier.of("cat2", "sch", "tbl"), "READ", "s3://b/t"));
  }

  @Test
  void deltaTableKeyNotEqualWhenDifferentOperation() {
    UCDeltaTableIdentifier id = UCDeltaTableIdentifier.of("cat", "sch", "tbl");
    assertThat(new DeltaTableCredId(id, "READ", "s3://b/t"))
        .isNotEqualTo(new DeltaTableCredId(id, "READ_WRITE", "s3://b/t"));
  }

  @Test
  void deltaTableKeyNotEqualWhenDifferentLocation() {
    UCDeltaTableIdentifier id = UCDeltaTableIdentifier.of("cat", "sch", "tbl");
    assertThat(new DeltaTableCredId(id, "READ", "s3://b/t1"))
        .isNotEqualTo(new DeltaTableCredId(id, "READ", "s3://b/t2"));
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

    UCDeltaTableIdentifier id = UCDeltaTableIdentifier.of("cat", "sch", "tbl");
    assertThat(CredId.create(conf))
        .isInstanceOf(DeltaTableCredId.class)
        .isEqualTo(new DeltaTableCredId(id, "READ_WRITE", "s3://b/tbl"));
  }

  @Test
  void createReturnsTableKeyWhenDeltaApiNotEnabled() {
    Configuration conf = new Configuration();
    conf.set(
        UCHadoopConfConstants.UC_CREDENTIALS_TYPE_KEY,
        UCHadoopConfConstants.UC_CREDENTIALS_TYPE_TABLE_VALUE);
    conf.set(UCHadoopConfConstants.UC_TABLE_ID_KEY, "tid");
    conf.set(UCHadoopConfConstants.UC_TABLE_OPERATION_KEY, "READ");

    assertThat(CredId.create(conf))
        .isInstanceOf(TableCredId.class)
        .isEqualTo(new TableCredId("tid", "READ"));
  }

  @Test
  void deltaStagingTableKeyEqualWhenSameFields() {
    assertThat(new DeltaStagingTableCredId("stid-1", "s3://b/staging"))
        .isEqualTo(new DeltaStagingTableCredId("stid-1", "s3://b/staging"))
        .hasSameHashCodeAs(new DeltaStagingTableCredId("stid-1", "s3://b/staging"));
  }

  @Test
  void deltaStagingTableKeyNotEqualWhenDifferentId() {
    assertThat(new DeltaStagingTableCredId("stid-1", "s3://b/staging"))
        .isNotEqualTo(new DeltaStagingTableCredId("stid-2", "s3://b/staging"));
  }

  @Test
  void deltaStagingTableKeyNotEqualWhenDifferentLocation() {
    assertThat(new DeltaStagingTableCredId("stid-1", "s3://b/loc1"))
        .isNotEqualTo(new DeltaStagingTableCredId("stid-1", "s3://b/loc2"));
  }

  @Test
  void createReturnsDeltaStagingTableKey() {
    Configuration conf = new Configuration();
    conf.set(UCHadoopConfConstants.UC_DELTA_STAGING_TABLE_ID_KEY, "stid-1");
    conf.set(UCHadoopConfConstants.UC_DELTA_STAGING_TABLE_LOCATION_KEY, "s3://b/staging");

    assertThat(CredId.create(conf))
        .isInstanceOf(DeltaStagingTableCredId.class)
        .isEqualTo(new DeltaStagingTableCredId("stid-1", "s3://b/staging"));
  }

  @Test
  void stagingTableKeyTakesPriorityOverTableType() {
    Configuration conf = new Configuration();
    conf.set(
        UCHadoopConfConstants.UC_CREDENTIALS_TYPE_KEY,
        UCHadoopConfConstants.UC_CREDENTIALS_TYPE_TABLE_VALUE);
    conf.set(UCHadoopConfConstants.UC_TABLE_ID_KEY, "tid");
    conf.set(UCHadoopConfConstants.UC_TABLE_OPERATION_KEY, "READ");
    conf.set(UCHadoopConfConstants.UC_DELTA_STAGING_TABLE_ID_KEY, "stid-1");
    conf.set(UCHadoopConfConstants.UC_DELTA_STAGING_TABLE_LOCATION_KEY, "s3://b/staging");

    assertThat(CredId.create(conf)).isInstanceOf(DeltaStagingTableCredId.class);
  }

  @Test
  void pathKeyProps() {
    CredId key = new PathCredId("s3://b/p", "WRITE");
    assertThat(key.props())
        .containsOnly(
            entry(
                UCHadoopConfConstants.UC_CREDENTIALS_TYPE_KEY,
                UCHadoopConfConstants.UC_CREDENTIALS_TYPE_PATH_VALUE),
            entry(UCHadoopConfConstants.UC_PATH_KEY, "s3://b/p"),
            entry(UCHadoopConfConstants.UC_PATH_OPERATION_KEY, "WRITE"));
    assertPropsRoundTrip(key);
  }

  @Test
  void tableKeyProps() {
    CredId key = new TableCredId("tid", "READ");
    assertThat(key.props())
        .containsOnly(
            entry(
                UCHadoopConfConstants.UC_CREDENTIALS_TYPE_KEY,
                UCHadoopConfConstants.UC_CREDENTIALS_TYPE_TABLE_VALUE),
            entry(UCHadoopConfConstants.UC_TABLE_ID_KEY, "tid"),
            entry(UCHadoopConfConstants.UC_TABLE_OPERATION_KEY, "READ"));
    assertPropsRoundTrip(key);
  }

  @Test
  void deltaTableKeyProps() {
    CredId key =
        new DeltaTableCredId(
            UCDeltaTableIdentifier.of("cat", "sch", "tbl"), "READ_WRITE", "s3://b/tbl");
    assertThat(key.props())
        .containsOnly(
            entry(UCHadoopConfConstants.UC_DELTA_CREDENTIALS_API_ENABLED_KEY, "true"),
            entry(
                UCHadoopConfConstants.UC_CREDENTIALS_TYPE_KEY,
                UCHadoopConfConstants.UC_CREDENTIALS_TYPE_TABLE_VALUE),
            entry(UCHadoopConfConstants.UC_DELTA_CATALOG_KEY, "cat"),
            entry(UCHadoopConfConstants.UC_DELTA_SCHEMA_KEY, "sch"),
            entry(UCHadoopConfConstants.UC_DELTA_TABLE_NAME_KEY, "tbl"),
            entry(UCHadoopConfConstants.UC_DELTA_LOCATION_KEY, "s3://b/tbl"),
            entry(UCHadoopConfConstants.UC_TABLE_OPERATION_KEY, "READ_WRITE"));
    assertPropsRoundTrip(key);
  }

  @Test
  void deltaStagingTableKeyProps() {
    CredId key = new DeltaStagingTableCredId("stid-1", "s3://b/staging");
    assertThat(key.props())
        .containsOnly(
            entry(UCHadoopConfConstants.UC_DELTA_CREDENTIALS_API_ENABLED_KEY, "true"),
            entry(UCHadoopConfConstants.UC_DELTA_STAGING_TABLE_ID_KEY, "stid-1"),
            entry(UCHadoopConfConstants.UC_DELTA_STAGING_TABLE_LOCATION_KEY, "s3://b/staging"));
    assertPropsRoundTrip(key);
  }

  @Test
  void defaultKeyPropsAreEmpty() {
    // The fallback scope carries no credential-request props, so it cannot round-trip via create.
    assertThat(new DefaultCredId(URI.create("s3://b"), new Configuration()).props()).isEmpty();
  }

  @Test
  void propsAreUnmodifiable() {
    Map<String, String> props = new TableCredId("tid", "READ").props();
    assertThatThrownBy(() -> props.put("k", "v")).isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void tableKeyRejectsNullFields() {
    assertThatThrownBy(() -> new TableCredId(null, "READ"))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("tableId");
    assertThatThrownBy(() -> new TableCredId("tid", null))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("tableOperation");
  }

  @Test
  void pathKeyRejectsNullFields() {
    assertThatThrownBy(() -> new PathCredId(null, "READ"))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("path");
    assertThatThrownBy(() -> new PathCredId("s3://b/p", null))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("pathOperation");
  }

  @Test
  void deltaTableKeyRejectsNullFields() {
    UCDeltaTableIdentifier id = UCDeltaTableIdentifier.of("cat", "sch", "tbl");
    assertThatThrownBy(() -> new DeltaTableCredId(null, "READ", "s3://b/t"))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("identifier");
    assertThatThrownBy(() -> new DeltaTableCredId(id, null, "s3://b/t"))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("tableOperation");
    assertThatThrownBy(() -> new DeltaTableCredId(id, "READ", null))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("location");
  }

  @Test
  void deltaStagingTableKeyRejectsNullFields() {
    assertThatThrownBy(() -> new DeltaStagingTableCredId(null, "s3://b/staging"))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("stagingTableId");
    assertThatThrownBy(() -> new DeltaStagingTableCredId("stid-1", null))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("location");
  }

  /** Asserts that feeding a key's {@code props()} back through {@code create} reconstructs it. */
  private static void assertPropsRoundTrip(CredId key) {
    Configuration conf = new Configuration(false);
    key.props().forEach(conf::set);
    assertThat(CredId.create(conf)).isEqualTo(key);
  }
}
