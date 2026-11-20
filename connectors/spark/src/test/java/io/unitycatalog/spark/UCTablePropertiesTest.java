package io.unitycatalog.spark;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class UCTablePropertiesTest {

  @Test
  public void isSparkDatasourceSchemaProperty_matchesHiveExternalCatalogKeys() {
    assertThat(UCTableProperties.isSparkDatasourceSchemaProperty("spark.sql.sources.schema"))
        .isTrue();
    assertThat(
            UCTableProperties.isSparkDatasourceSchemaProperty("spark.sql.sources.schema.numParts"))
        .isTrue();
    assertThat(UCTableProperties.isSparkDatasourceSchemaProperty("spark.sql.sources.schema.part.0"))
        .isTrue();
    assertThat(
            UCTableProperties.isSparkDatasourceSchemaProperty("spark.sql.sources.schema.partCol.0"))
        .isTrue();
    assertThat(UCTableProperties.isSparkDatasourceSchemaProperty("spark.sql.partitionSchema"))
        .isTrue();
    assertThat(
            UCTableProperties.isSparkDatasourceSchemaProperty("spark.sql.partitionSchema.part.0"))
        .isTrue();
    assertThat(
            UCTableProperties.isSparkDatasourceSchemaProperty(
                TableCatalog.OPTION_PREFIX + "spark.sql.sources.schema.part.0"))
        .isTrue();
  }

  @Test
  public void isSparkDatasourceSchemaProperty_doesNotMatchUserOrProviderKeys() {
    assertThat(UCTableProperties.isSparkDatasourceSchemaProperty("spark.sql.sources.provider"))
        .isFalse();
    assertThat(UCTableProperties.isSparkDatasourceSchemaProperty("spark.sql.create.version"))
        .isFalse();
    assertThat(UCTableProperties.isSparkDatasourceSchemaProperty("user.custom")).isFalse();
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "comment",
        "collation",
        "external",
        "is_managed_location",
        "location",
        "owner",
        "provider",
        "table_type"
      })
  public void shouldPersistProperty_dropsReservedKeyButKeepsOptionOfTheSameName(String key) {
    assertThat(UCTableProperties.shouldPersistProperty(key)).isFalse();
    assertThat(UCTableProperties.shouldPersistProperty("option." + key)).isTrue();
  }

  @Test
  public void shouldPersistProperty_dropsCredentialsAndSchemaKeys() {
    assertThat(UCTableProperties.shouldPersistProperty("fs.s3a.session.token")).isFalse();
    assertThat(
            UCTableProperties.shouldPersistProperty(
                TableCatalog.OPTION_PREFIX + "fs.s3a.session.token"))
        .isFalse();
    assertThat(UCTableProperties.shouldPersistProperty("spark.sql.sources.schema.part.0"))
        .isFalse();
    assertThat(
            UCTableProperties.shouldPersistProperty(
                TableCatalog.OPTION_PREFIX + "spark.sql.sources.schema.part.0"))
        .isFalse();
  }

  @Test
  public void shouldPersistProperty_keepsUserAndViewMetadataKeys() {
    assertThat(UCTableProperties.shouldPersistProperty("user.custom")).isTrue();
    assertThat(UCTableProperties.shouldPersistProperty("spark.sql.create.version")).isTrue();
    assertThat(UCTableProperties.shouldPersistProperty("view.sqlConfig.spark.sql.ansi.enabled"))
        .isTrue();
  }
}
