package io.unitycatalog.spark;

import static io.unitycatalog.server.utils.IcebergViewTestUtils.nativeRequest;
import static io.unitycatalog.server.utils.TestUtils.CATALOG_NAME;
import static io.unitycatalog.server.utils.TestUtils.SCHEMA_NAME;
import static io.unitycatalog.server.utils.TestUtils.createApiClient;
import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.api.TablesApi;
import io.unitycatalog.client.model.ColumnInfo;
import io.unitycatalog.client.model.ColumnTypeName;
import io.unitycatalog.server.utils.IcebergRestClient;
import io.unitycatalog.server.utils.ServerProperties.Property;
import java.util.List;
import java.util.Map;
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

  @Test
  public void sparkReadsANativeViewWithCreationContextAndDeclaredOutputNames() throws Exception {
    session = createSparkSessionWithCatalogs(CATALOG_NAME);
    String base = CATALOG_NAME + "." + SCHEMA_NAME + ".native_source";
    String view = CATALOG_NAME + "." + SCHEMA_NAME + ".native_read";
    sql(
        "CREATE TABLE %s (id BIGINT) USING ICEBERG LOCATION '%s'",
        base, testDirectoryRoot.resolve("native_data").toUri());
    sql("INSERT INTO %s VALUES (1), (2), (3)", base);
    var api = new TablesApi(createApiClient(serverConfig));
    var request =
        nativeRequest(
            CATALOG_NAME,
            SCHEMA_NAME,
            "native_read",
            "select id as query_id, array(id) as query_values from native_source where id > 1");
    ColumnInfo id =
        request
            .getColumns()
            .get(0)
            .name("declared_id")
            .typeJson(
                "{\"name\":\"declared_id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}}");
    request
        .columns(
            List.of(
                id,
                new ColumnInfo()
                    .name("declared_values")
                    .position(1)
                    .nullable(true)
                    .typeName(ColumnTypeName.ARRAY)
                    .typeText("array<bigint>")
                    .typeJson(
                        "{\"name\":\"declared_values\",\"type\":{\"type\":\"array\","
                            + "\"elementType\":\"long\","
                            + "\"containsNull\":true},\"nullable\":true,\"metadata\":{}}")))
        .properties(
            Map.of(
                "view.catalogAndNamespace.numParts", "2",
                "view.catalogAndNamespace.part.0", CATALOG_NAME,
                "view.catalogAndNamespace.part.1", SCHEMA_NAME,
                "view.query.out.numCols", "2",
                "view.query.out.col.0", "query_id",
                "view.query.out.col.1", "query_values"));
    var created = api.createTable(request);
    try {
      session.catalog().setCurrentCatalog(SPARK_CATALOG);
      session.catalog().setCurrentDatabase("default");
      var rows = sql("SELECT declared_id, declared_values FROM " + view + " ORDER BY declared_id");
      assertThat(rows)
          .extracting(row -> List.of(row.getLong(0), row.<Long>getList(1)))
          .containsExactly(List.of(2L, List.of(2L)), List.of(3L, List.of(3L)));
      assertThat(api.getTable(view, true, false, false)).isEqualTo(created);
      var client = new IcebergRestClient(serverConfig);
      var loaded = client.loadView(CATALOG_NAME, SCHEMA_NAME, "native_read");
      assertThat(loaded.metadata().uuid()).isEqualTo(created.getTableId());
      assertThat(client.loadView(CATALOG_NAME, SCHEMA_NAME, "native_read").metadataLocation())
          .isEqualTo(loaded.metadataLocation());
    } finally {
      api.deleteTable(view);
      sql("DROP TABLE IF EXISTS %s", base);
    }
  }
}
