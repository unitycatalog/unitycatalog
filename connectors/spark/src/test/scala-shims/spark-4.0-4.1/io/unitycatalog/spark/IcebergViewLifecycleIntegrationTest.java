package io.unitycatalog.spark;

import static io.unitycatalog.server.utils.IcebergViewTestUtils.request;
import static io.unitycatalog.server.utils.TestUtils.CATALOG_NAME;
import static io.unitycatalog.server.utils.TestUtils.SCHEMA_NAME;
import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.server.utils.IcebergRestClient;
import io.unitycatalog.server.utils.ServerProperties.Property;
import io.unitycatalog.spark.utils.OptionsUtil;
import java.util.Map;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.rest.requests.ImmutableCreateViewRequest;
import org.apache.iceberg.view.ImmutableViewVersion;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;

public class IcebergViewLifecycleIntegrationTest extends BaseSparkIntegrationTest {
  @Override
  protected boolean isIcebergCatalog(String catalog) {
    return true;
  }

  @Override
  protected void setUpProperties() {
    super.setUpProperties();
    serverProperties.setProperty(Property.ICEBERG_VIEW_ENABLED.getKey(), "true");
    serverProperties.setProperty(
        Property.ICEBERG_VIEW_METADATA_ROOT.getKey(),
        testDirectoryRoot.resolve("view_metadata").toString());
  }

  @Test
  public void ucReaderResolvesAliasedOutputOfAnIcebergCreatedView() {
    session = createSparkSessionWithCatalogs(CATALOG_NAME);
    String view = CATALOG_NAME + "." + SCHEMA_NAME + ".aliased_view";
    sql("CREATE VIEW %s (public_id) AS SELECT 41 AS source_id", view);
    try {
      assertThat(nativeSession().sql("SELECT public_id FROM " + view).collectAsList())
          .extracting(row -> row.getInt(0))
          .containsExactly(41);
    } finally {
      sql("DROP VIEW IF EXISTS %s", view);
    }
  }

  private SparkSession nativeSession() {
    var reader = session.newSession();
    String catalog = "spark.sql.catalog." + CATALOG_NAME;
    reader.conf().set(catalog, UCSingleCatalog.class.getName());
    reader.conf().set(catalog + "." + OptionsUtil.URI, serverConfig.getServerUrl());
    reader.conf().set(catalog + "." + OptionsUtil.TOKEN, serverConfig.getAuthToken());
    reader.conf().set(catalog + "." + OptionsUtil.WAREHOUSE, CATALOG_NAME);
    return reader;
  }

  @Test
  public void absentDefaultCatalogUsesTheReaderCatalogAndSavedNamespace() throws Exception {
    String alias = "iceberg_alias";
    session = createSparkSessionWithCatalogs(alias);
    session.conf().set("spark.sql.catalog." + alias + ".warehouse", CATALOG_NAME);
    assertThat(session.conf().getOption("spark.sql.catalog." + CATALOG_NAME).isEmpty()).isTrue();
    String source = alias + ".source_ns.source_view";
    String view = alias + "." + SCHEMA_NAME + ".relative_view";
    sql("CREATE NAMESPACE %s.source_ns", alias);
    sql("CREATE VIEW %s AS SELECT CAST(41 AS BIGINT) AS source_id", source);
    try {
      var template = request("relative_view", "spark", "SELECT source_id FROM source_view");
      var version =
          ImmutableViewVersion.builder()
              .from(template.viewVersion())
              .defaultCatalog(null)
              .defaultNamespace(Namespace.of("source_ns"))
              .build();
      var created =
          new IcebergRestClient(serverConfig)
              .createView(
                  CATALOG_NAME,
                  SCHEMA_NAME,
                  ImmutableCreateViewRequest.builder()
                      .from(template)
                      .viewVersion(version)
                      .properties(Map.of("spark.query-column-names", "source_id"))
                      .build());
      assertThat(created.metadata().currentVersion().defaultCatalog()).isNull();
      assertThat(sql("SELECT id FROM %s", view))
          .extracting(row -> row.getLong(0))
          .containsExactly(41L);
      assertThat(
              nativeSession()
                  .sql("SELECT id FROM " + CATALOG_NAME + "." + SCHEMA_NAME + ".relative_view")
                  .collectAsList())
          .extracting(row -> row.getLong(0))
          .containsExactly(41L);
    } finally {
      sql("DROP VIEW IF EXISTS %s", view);
      sql("DROP VIEW IF EXISTS %s", source);
      sql("DROP NAMESPACE %s.source_ns", alias);
    }
  }

  @Test
  public void sparkCreatesQueriesAndDropsATableBackedView() throws Exception {
    session = createSparkSessionWithCatalogs(CATALOG_NAME);
    String base = CATALOG_NAME + "." + SCHEMA_NAME + ".view_base";
    String view = CATALOG_NAME + "." + SCHEMA_NAME + ".ordinary_view";
    sql(
        "CREATE TABLE %s (id BIGINT) USING ICEBERG LOCATION '%s'",
        base, testDirectoryRoot.resolve("base_data").toUri());
    try {
      sql("INSERT INTO %s VALUES (1), (2), (3)", base);
      sql("CREATE VIEW %s AS SELECT id FROM %s WHERE id > 1", view, base);
      assertThat(sql("SELECT id FROM " + view + " ORDER BY id"))
          .extracting(row -> row.getLong(0))
          .containsExactly(2L, 3L);
      sql("DROP VIEW %s", view);
      assertThat(
              new IcebergRestClient(serverConfig)
                  .viewExists(CATALOG_NAME, SCHEMA_NAME, "ordinary_view"))
          .isFalse();
    } finally {
      sql("DROP VIEW IF EXISTS %s", view);
      sql("DROP TABLE IF EXISTS %s", base);
    }
  }
}
