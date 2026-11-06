package io.unitycatalog.spark;

import static io.unitycatalog.server.utils.TestUtils.CATALOG_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static io.unitycatalog.server.utils.TestUtils.SCHEMA_NAME;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.sql.RowFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * This test suite runs all tests in ExternalTableReadWriteTest plus extra test. Tests that are only
 * for Delta external tables should live here.
 */
public class DeltaExternalTableReadWriteTest extends ExternalTableReadWriteTest {

  @Test
  public void testDeltaPathTable() throws IOException {
    // We must replace the `spark_catalog` in order to support Delta path tables.
    session = createSparkSessionWithCatalogs(SPARK_CATALOG);

    int tableCounter = 0;
    for (boolean ctas : List.of(true, false)) {
      TableSetupOptions options = new TableSetupOptions();
      if (ctas) {
        options.setAsSelect(1, "a");
      }
      String path = new File(dataDir, "test_delta_path" + tableCounter).getCanonicalPath();
      String tableName = String.format("delta.`%s`", path);
      tableCounter++;
      sql(options.createDeltaPathTableSql(path));
      if (ctas) {
        testTableReadWriteCreatedAsSelect(tableName, Pair.of(1, "a"));
      } else {
        testTableReadWrite(tableName);
      }
    }
  }

  @ParameterizedTest(name = "deltaRestApi={0}")
  @ValueSource(booleans = {true, false})
  public void testReplaceDeltaPathTable(boolean deltaRestApiEnabled) throws IOException {
    session = createSparkSessionWithCatalogs(SPARK_CATALOG);
    setDeltaRestApiEnabled(SPARK_CATALOG, deltaRestApiEnabled);
    String path = new File(dataDir, "test_delta_path").getCanonicalPath();
    String tableName = String.format("delta.`%s`", path);
    TableSetupOptions options =
        new TableSetupOptions().setDdlMode(TableDdlMode.CREATE_OR_REPLACE_MISSING);

    sql(options.createDeltaPathTableSql(path));
    testTableReadWrite(tableName);
    sql(options.setDdlMode(TableDdlMode.REPLACE_EXISTING).createDeltaPathTableSql(path));
    testTableReadWrite(tableName);
  }

  @Test
  public void testPartitionedTableKeepsDeclaredColumnOrder() {
    session = createSparkSessionWithCatalogs(CATALOG_NAME);
    TableSetupOptions options =
        new TableSetupOptions().setCatalogName(CATALOG_NAME).setTableName(TEST_TABLE);
    String fullTableName = options.fullTableName();
    sql(
        "CREATE TABLE %s (a INT, p INT, b STRING) USING DELTA PARTITIONED BY (p) LOCATION '%s'",
        fullTableName, getLocation(options));
    sql("INSERT INTO %s VALUES (1, 2, 'x')", fullTableName);

    assertThat(session.table(fullTableName).columns()).containsExactly("a", "p", "b");
    assertThat(sql("SELECT * FROM %s", fullTableName))
        .containsExactly(RowFactory.create(1, 2, "x"));
  }

  @Override
  protected String tableFormat() {
    return "DELTA";
  }

  /**
   * An external table on S3 in a folder with a space or a literal {@code %}, as a user writes it in
   * {@code LOCATION}: Delta writes and reads it through UC, and UC's stored location and credential
   * scope name the folder Delta wrote.
   */
  @ParameterizedTest
  @ValueSource(strings = {"reports", "team reports", "team%20reports"})
  public void testExternalTableInFolder(String folder) throws Exception {
    session = createSparkSessionWithCatalogs(SPARK_CATALOG, CATALOG_NAME);
    File dir = new File(new File(dataDir, folder), "ext");
    String table = CATALOG_NAME + "." + SCHEMA_NAME + ".ext_t";
    sql(
        "CREATE TABLE %s (i INT, s STRING) USING delta LOCATION '%s'",
        table, "s3://test-bucket0" + dir.getCanonicalPath());
    sql("INSERT INTO %s VALUES (1, 'a')", table);
    sql("INSERT INTO %s VALUES (2, 'b')", table);
    assertThat(sql("SELECT i FROM %s ORDER BY i", table))
        .extracting(row -> row.getInt(0))
        .containsExactly(1, 2);
    assertStoredLocationIsWrittenDirectory(
        tableOperations.getTable(table).getStorageLocation(), dataDir.toPath());
    assertThat(new File(dir, "_delta_log")).isDirectory();
  }

  /**
   * A {@code ?} or {@code #} in {@code LOCATION} is rejected through Spark: Iceberg FileIO and the
   * server's cleanup would cut the object key there.
   */
  @ParameterizedTest
  @ValueSource(strings = {"t?x", "t#x"})
  public void testExternalTableLocationWithQueryOrFragmentRejected(String name) throws Exception {
    session = createSparkSessionWithCatalogs(SPARK_CATALOG, CATALOG_NAME);
    String location = "s3://test-bucket0" + dataDir.getCanonicalPath() + "/" + name;
    assertThatThrownBy(
            () ->
                sql(
                    "CREATE TABLE %s (i INT) USING delta LOCATION '%s'",
                    CATALOG_NAME + "." + SCHEMA_NAME + ".bad_t", location))
        .hasMessageContaining("cannot contain '?' or '#'");
  }
}
