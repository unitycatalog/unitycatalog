package io.unitycatalog.spark;

import static io.unitycatalog.server.utils.TestUtils.CATALOG_NAME;
import static org.assertj.core.api.Assertions.assertThat;

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
}
