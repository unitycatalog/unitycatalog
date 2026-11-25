package io.unitycatalog.spark;

import static io.unitycatalog.server.utils.IcebergViewTestUtils.nativeRequest;
import static io.unitycatalog.server.utils.IcebergViewTestUtils.request;
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
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.rest.requests.ImmutableCreateViewRequest;
import org.apache.iceberg.view.ImmutableViewVersion;
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

  @Test
  public void ucReaderResolvesQuotedNestedFieldNames() {
    session = createSparkSessionWithCatalogs(CATALOG_NAME);
    String view = CATALOG_NAME + "." + SCHEMA_NAME + ".nested_names";
    sql("CREATE VIEW %s AS SELECT named_struct('a.b', 1, 'c`d', 2) AS s", view);
    try {
      var row = nativeSession().sql("SELECT s.`a.b`, s.`c``d` FROM " + view).first();
      assertThat(row.getInt(0)).isEqualTo(1);
      assertThat(row.getInt(1)).isEqualTo(2);
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
            "select id as query_id, array(id) as query_values from native_source where id > 1"
                + " -- trailing comment \\");
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
        "CREATE TABLE %s (a BIGINT, `current_user` STRING) USING ICEBERG LOCATION '%s'",
        source, testDirectoryRoot.resolve("reordered").toUri());
    var api = new TablesApi(createApiClient(serverConfig));
    try {
      sql("INSERT INTO %s VALUES (10, 'saved user')", source);
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
                  new ColumnInfo()
                      .name("second_value")
                      .position(1)
                      .nullable(true)
                      .typeName(ColumnTypeName.STRING)
                      .typeText("string")
                      .typeJson(
                          "{\"name\":\"second_value\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}"),
                  bigintColumn("last_value", 2)))
          .properties(
              Map.of(
                  "view.query.out.numCols",
                  "3",
                  "view.query.out.col.0",
                  "a",
                  "view.query.out.col.1",
                  "current_user",
                  "view.query.out.col.2",
                  "c"));
      api.createTable(request);
      try {
        String query = "SELECT first_value, second_value, last_value FROM " + view;
        var expected = RowFactory.create(10L, "saved user", 30L);
        assertThat(sql(query)).containsExactly(expected);
        sql("ALTER TABLE %s ALTER COLUMN `current_user` FIRST", source);
        sql("REFRESH TABLE %s", source);
        assertThat(sql("SELECT * FROM %s", source))
            .containsExactly(RowFactory.create("saved user", 10L));
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

        // A missing output must not become the Spark built-in current_user.
        sql("ALTER TABLE %s DROP COLUMN c", source);
        sql("ALTER TABLE %s DROP COLUMN `current_user`", source);
        sql("REFRESH TABLE %s", source);
        assertThatThrownBy(() -> sql(query))
            .isInstanceOf(AnalysisException.class)
            .hasMessageContaining("FIELD_NOT_FOUND")
            .hasMessageContaining("`current_user`");

        // A missing output must not resolve to a struct field or a reader session variable.
        sql("ALTER TABLE %s ADD COLUMN `current_user` STRING", source);
        sql("DECLARE VARIABLE a BIGINT DEFAULT 42");
        sql("ALTER TABLE %s ADD COLUMN view_source STRUCT<a: BIGINT>", source);
        sql("ALTER TABLE %s DROP COLUMN a", source);
        sql("REFRESH TABLE %s", source);
        assertThatThrownBy(() -> sql(query))
            .isInstanceOf(AnalysisException.class)
            .hasMessageContaining("FIELD_NOT_FOUND")
            .hasMessageContaining("`a`");
      } finally {
        api.deleteTable(view);
      }
    } finally {
      sql("DROP TABLE IF EXISTS %s", source);
    }
  }

  @Test
  public void compensatesIntegerOutputsAndRejectsOverflowIndependentlyOfReaderAnsi()
      throws Exception {
    session = createSparkSessionWithCatalogs(CATALOG_NAME, CATALOG_NAME2);
    String sourceSchema = CATALOG_NAME2 + "." + SCHEMA_NAME;
    sql("CREATE NAMESPACE IF NOT EXISTS %s", sourceSchema);
    String source = sourceSchema + ".widened_source";
    String view = CATALOG_NAME + "." + SCHEMA_NAME + ".compensated";
    sql(
        "CREATE TABLE %s (id INT, amount DECIMAL(21, 1)) USING ICEBERG LOCATION '%s'",
        source, testDirectoryRoot.resolve("widened").toUri());
    var tables = new TablesApi(createApiClient(serverConfig));
    var id =
        new ColumnInfo()
            .name("id")
            .position(0)
            .nullable(true)
            .typeName(ColumnTypeName.INT)
            .typeText("int")
            .typeJson("{\"name\":\"id\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}");
    try {
      tables.createTable(
          nativeRequest(
                  CATALOG_NAME, SCHEMA_NAME, "compensated", "SELECT id, amount FROM " + source)
              .columns(List.of(id, bigintColumn("amount", 1)))
              .properties(Map.of("view.schemaMode", "COMPENSATION")));
      sql("ALTER TABLE %s ALTER COLUMN id TYPE BIGINT", source);
      sql("INSERT INTO %s VALUES (2147483647, -3.9), (-2147483648, 3.9), (NULL, NULL)", source);
      var nativeReader = nativeSession();
      for (var reader : List.of(session, nativeReader)) {
        for (String ansi : List.of("true", "false")) {
          reader.conf().set("spark.sql.ansi.enabled", ansi);
          assertThat(
                  reader.sql("SELECT * FROM " + view + " ORDER BY id NULLS LAST").collectAsList())
              .containsExactly(
                  RowFactory.create(Integer.MIN_VALUE, 3L),
                  RowFactory.create(Integer.MAX_VALUE, -3L),
                  RowFactory.create(null, null));
        }
      }
      sql("INSERT INTO %s VALUES (2147483648L, 9223372036854775808.0)", source);
      for (var reader : List.of(session, nativeReader)) {
        reader.sql("REFRESH TABLE " + source).collectAsList();
        for (String ansi : List.of("true", "false")) {
          reader.conf().set("spark.sql.ansi.enabled", ansi);
          for (String column : List.of("id", "amount")) {
            assertThatThrownBy(
                    () -> reader.sql("SELECT " + column + " FROM " + view).collectAsList())
                .hasStackTraceContaining(
                    reader == session ? "Cannot cast native view output" : "CAST_OVERFLOW");
          }
        }
      }
    } finally {
      tables.deleteTable(view);
      sql("DROP TABLE IF EXISTS %s", source);
    }
  }

  @Test
  public void aliasedReaderResolvesANativeViewWithoutSavedContext() throws Exception {
    String alias = "iceberg_alias";
    session = createSparkSessionWithCatalogs(alias);
    session.conf().set("spark.sql.catalog." + alias + ".warehouse", CATALOG_NAME);
    String schema = alias + "." + SCHEMA_NAME;
    String source = schema + ".local_source";
    sql(
        "CREATE TABLE %s (source_id BIGINT) USING ICEBERG LOCATION '%s'",
        source, testDirectoryRoot.resolve("source").toUri());
    var tables = new TablesApi(createApiClient(serverConfig));
    sql("INSERT INTO %s VALUES (42)", source);
    tables.createTable(
        nativeRequest(CATALOG_NAME, SCHEMA_NAME, "local_view", "SELECT source_id FROM local_source")
            .properties(
                Map.of("view.query.out.numCols", "1", "view.query.out.col.0", "source_id")));
    try {
      assertThat(sql("SELECT id FROM %s.local_view", schema))
          .extracting(row -> row.getLong(0))
          .containsExactly(42L);
    } finally {
      tables.deleteTable(CATALOG_NAME + "." + SCHEMA_NAME + ".local_view");
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
