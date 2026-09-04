package io.unitycatalog.spark;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

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
  }

  @Test
  public void isSparkDatasourceSchemaProperty_doesNotMatchUserOrProviderKeys() {
    assertThat(UCTableProperties.isSparkDatasourceSchemaProperty("spark.sql.sources.provider"))
        .isFalse();
    assertThat(UCTableProperties.isSparkDatasourceSchemaProperty("spark.sql.create.version"))
        .isFalse();
    assertThat(UCTableProperties.isSparkDatasourceSchemaProperty("user.custom")).isFalse();
  }
}
