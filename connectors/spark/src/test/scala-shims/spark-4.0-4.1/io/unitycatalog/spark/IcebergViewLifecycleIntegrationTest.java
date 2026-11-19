package io.unitycatalog.spark;

import static io.unitycatalog.server.utils.TestUtils.CATALOG_NAME;
import static io.unitycatalog.server.utils.TestUtils.SCHEMA_NAME;
import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.server.utils.IcebergRestClient;
import io.unitycatalog.server.utils.ServerProperties.Property;
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
