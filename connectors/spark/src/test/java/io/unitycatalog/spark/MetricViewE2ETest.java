package io.unitycatalog.spark;

import static io.unitycatalog.server.utils.TestUtils.CATALOG_NAME;
import static io.unitycatalog.server.utils.TestUtils.SCHEMA_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.util.List;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * End-to-end test for metric views: drives the real {@code CREATE VIEW ... WITH METRICS} SQL DDL
 * (SPARK-56920) through the connector against an embedded {@code UnityCatalogServer}. Skipped on
 * Spark below 4.2, where the metric-view DDL does not exist.
 */
public class MetricViewE2ETest extends BaseSparkIntegrationTest {

  @TempDir private File sourceTableDir;

  /** Metric views require Spark 4.2+; skip (rather than fail) the whole class on older runtimes. */
  @BeforeAll
  public static void assumeMetricViewsSupported() {
    Assumptions.assumeTrue(
        isSparkAtLeast(4, 2),
        "Metric views require Spark 4.2+, but runtime is "
            + org.apache.spark.package$.MODULE$.SPARK_VERSION());
  }

  private String tbl(String name) {
    return CATALOG_NAME + "." + SCHEMA_NAME + "." + name;
  }

  /**
   * Creates the UC-backed source table the metric view reads from. The metric-view CREATE path
   * eagerly resolves the YAML {@code source:} relation, so this table must exist first. A plain
   * parquet external table keeps the source setup independent of the Delta create path.
   */
  private void createEventsSource() {
    sql(
        "CREATE TABLE %s (region STRING, cnt INT) USING parquet LOCATION '%s'",
        tbl("events"), sourceTableDir.toURI());
    sql("INSERT INTO %s VALUES ('us', 1), ('us', 2), ('eu', 3)", tbl("events"));
  }

  /** {@code CREATE VIEW ... WITH METRICS LANGUAGE YAML AS $$...$$} against the UC v2 catalog. */
  private void createMetricView(String name) {
    sql(
        "CREATE VIEW %s\n"
            + "WITH METRICS\n"
            + "LANGUAGE YAML\n"
            + "AS\n"
            + "$$\n"
            + "version: \"0.1\"\n"
            + "source: %s\n"
            + "dimensions:\n"
            + "  - name: region\n"
            + "    expr: region\n"
            + "measures:\n"
            + "  - name: cnt_sum\n"
            + "    expr: sum(cnt)\n"
            + "$$",
        tbl(name), tbl("events"));
  }

  @Test
  public void testMetricViewOperations() throws Exception {
    session = createSparkSessionWithCatalogs(SPARK_CATALOG, CATALOG_NAME);
    createEventsSource();

    // CREATE VIEW ... WITH METRICS -> CreateV2MetricViewExec -> connector.createView, persisted
    // by the embedded UC server.
    createMetricView("mv_e2e");

    // SHOW VIEWS shows views only: the metric view is listed, the source table `events` is not.
    List<String> views =
        sql("SHOW VIEWS IN %s.%s", CATALOG_NAME, SCHEMA_NAME).stream()
            .map(r -> r.getString(1))
            .collect(java.util.stream.Collectors.toList());
    assertThat(views).contains("mv_e2e").doesNotContain("events");

    // SHOW TABLES unions tables and views on Spark 4.2 (a RelationCatalog resolves it through
    // `listRelationSummaries`), so both the source table and the metric view are listed.
    List<String> tables =
        sql("SHOW TABLES IN %s.%s", CATALOG_NAME, SCHEMA_NAME).stream()
            .map(r -> r.getString(1))
            .collect(java.util.stream.Collectors.toList());
    assertThat(tables).contains("events", "mv_e2e");

    // DESCRIBE resolves the ident through the connector's `loadRelation`: `delegate.loadTable`
    // throws NoSuchTableException for a view, so it falls back to `ucProxy.loadView` and returns
    // the metric view's columns (the dimension + measure) with their resolved data types.
    List<Row> describe = sql("DESCRIBE %s", tbl("mv_e2e"));
    assertThat(describe.stream().map(r -> r.getString(0) + ":" + r.getString(1)))
        .contains("region:string", "cnt_sum:bigint");

    // DESCRIBE EXTENDED additionally reports the detailed view info -- notably that the object is
    // a METRIC_VIEW -- alongside the same column rows.
    List<Row> descExt = sql("DESCRIBE EXTENDED %s", tbl("mv_e2e"));
    assertThat(descExt.stream().map(r -> r.getString(0) + ":" + r.getString(1)))
        .contains("region:string", "cnt_sum:bigint", "Type:METRIC_VIEW");

    // SELECT through the metric view aggregates the source rows (eu=3, us=1+2=3). A measure is read
    // via the `measure(...)` function with GROUP BY on the dimensions.
    List<Row> rows =
        sql(
            "SELECT region, measure(cnt_sum) FROM %s GROUP BY region ORDER BY region",
            tbl("mv_e2e"));
    assertThat(rows).hasSize(2);
    assertThat(rows.get(0).getString(0)).isEqualTo("eu");
    assertThat(rows.get(0).getLong(1)).isEqualTo(3L);
    assertThat(rows.get(1).getString(0)).isEqualTo("us");
    assertThat(rows.get(1).getLong(1)).isEqualTo(3L);

    // ALTER VIEW ... RENAME TO delegates to UCSingleCatalogViewSupport.renameView ->
    // ucProxy.renameView, which is unsupported today, so the rename fails with a clear
    // UnsupportedOperationException and leaves the original view in place (target not created).
    assertThatThrownBy(() -> sql("ALTER VIEW %s RENAME TO %s", tbl("mv_e2e"), tbl("mv_renamed")))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("Renaming a view is not supported");
    List<String> afterRename =
        sql("SHOW VIEWS IN %s.%s", CATALOG_NAME, SCHEMA_NAME).stream()
            .map(r -> r.getString(1))
            .collect(java.util.stream.Collectors.toList());
    assertThat(afterRename).contains("mv_e2e").doesNotContain("mv_renamed");

    // DROP VIEW removes it; it no longer shows up.
    sql("DROP VIEW %s", tbl("mv_e2e"));
    List<Row> afterDrop = sql("SHOW VIEWS IN %s.%s", CATALOG_NAME, SCHEMA_NAME);
    assertThat(afterDrop.stream().anyMatch(r -> r.getString(1).equals("mv_e2e"))).isFalse();

    // DROP VIEW IF EXISTS on the now-missing view (and any absent view) is a no-op, not an error.
    sql("DROP VIEW IF EXISTS %s", tbl("mv_e2e"));
    sql("DROP VIEW IF EXISTS %s", tbl("does_not_exist"));
  }
}
