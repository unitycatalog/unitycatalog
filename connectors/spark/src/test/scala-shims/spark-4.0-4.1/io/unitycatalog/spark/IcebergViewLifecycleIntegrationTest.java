package io.unitycatalog.spark;

import static io.unitycatalog.server.utils.IcebergViewTestUtils.nativeRequest;
import static io.unitycatalog.server.utils.TestUtils.CATALOG_NAME;
import static io.unitycatalog.server.utils.TestUtils.CATALOG_NAME2;
import static io.unitycatalog.server.utils.TestUtils.SCHEMA_NAME;
import static io.unitycatalog.server.utils.TestUtils.createApiClient;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.client.api.TablesApi;
import io.unitycatalog.client.model.ColumnInfo;
import io.unitycatalog.client.model.ColumnTypeName;
import io.unitycatalog.server.utils.IcebergRestClient;
import io.unitycatalog.server.utils.ServerProperties.Property;
import io.unitycatalog.spark.utils.OptionsUtil;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.RowFactory;
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
    request
        .columns(
            List.of(
                bigintColumn("declared_id", 0),
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

  @Test
  public void nativeAndIcebergReadersKeepSelectStarColumnsBoundByName() throws Exception {
    session = createSparkSessionWithCatalogs(CATALOG_NAME, CATALOG_NAME2);
    String sourceSchema = CATALOG_NAME2 + "." + SCHEMA_NAME;
    String source = sourceSchema + ".reordered";
    String view = CATALOG_NAME + "." + SCHEMA_NAME + ".saved_order";
    sql("CREATE NAMESPACE IF NOT EXISTS %s", sourceSchema);
    sql(
        "CREATE TABLE %s (a BIGINT, b BIGINT) USING ICEBERG LOCATION '%s'",
        source, testDirectoryRoot.resolve("reordered").toUri());
    var api = new TablesApi(createApiClient(serverConfig));
    try {
      sql("INSERT INTO %s VALUES (10, 20)", source);
      var request =
          nativeRequest(
              CATALOG_NAME,
              SCHEMA_NAME,
              "saved_order",
              "SELECT *, CAST(30 AS BIGINT) AS c FROM " + source);
      request
          .columns(
              List.of(
                  bigintColumn("first_value", 0),
                  bigintColumn("second_value", 1),
                  bigintColumn("last_value", 2)))
          .properties(
              Map.of(
                  "view.query.out.numCols",
                  "3",
                  "view.query.out.col.0",
                  "a",
                  "view.query.out.col.1",
                  "b",
                  "view.query.out.col.2",
                  "c"));
      api.createTable(request);
      try {
        String query = "SELECT first_value, second_value, last_value FROM " + view;
        var expected = RowFactory.create(10L, 20L, 30L);
        assertThat(sql(query)).containsExactly(expected);
        sql("ALTER TABLE %s ALTER COLUMN b FIRST", source);
        sql("REFRESH TABLE %s", source);
        assertThat(sql("SELECT * FROM %s", source)).containsExactly(RowFactory.create(20L, 10L));
        assertThat(sql(query)).containsExactly(expected);
        var nativeSession = nativeSession();
        assertThat(nativeSession.sql(query).collectAsList()).containsExactly(expected);

        // The wildcard now also produces c. Neither reader may silently pick one of the two.
        sql("ALTER TABLE %s ADD COLUMN c BIGINT", source);
        sql("REFRESH TABLE %s", source);
        nativeSession.sql("REFRESH TABLE " + source).collectAsList();
        assertThatThrownBy(() -> sql(query))
            .isInstanceOf(AnalysisException.class)
            .hasMessageContaining("AMBIGUOUS_REFERENCE");
        assertThatThrownBy(() -> nativeSession.sql(query).collectAsList())
            .isInstanceOf(AnalysisException.class)
            .hasMessageContaining("INCOMPATIBLE_VIEW_SCHEMA_CHANGE");
      } finally {
        api.deleteTable(view);
      }
    } finally {
      sql("DROP TABLE IF EXISTS %s", source);
    }
  }

  private static ColumnInfo bigintColumn(String name, int position) {
    return new ColumnInfo()
        .name(name)
        .position(position)
        .nullable(true)
        .typeName(ColumnTypeName.LONG)
        .typeText("bigint")
        .typeJson(
            "{\"name\":\"" + name + "\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}}");
  }
}
