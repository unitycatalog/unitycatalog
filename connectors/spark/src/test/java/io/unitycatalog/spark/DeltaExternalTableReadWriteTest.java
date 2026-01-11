package io.unitycatalog.spark;

import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.Test;

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

  @Test
  public void testExternalDeltaTableWithDateType() throws IOException {
    session = createSparkSessionWithCatalogs(SPARK_CATALOG);

    TableSetupOptions options = new TableSetupOptions()
            .setCatalogName(SPARK_CATALOG)
            .setTableName(TEST_TABLE);

    sql(String.format(
            "CREATE TABLE %s (start_date DATE) USING delta LOCATION '%s'",
            options.fullTableName(), getLocation(options)));
    sql(String.format("INSERT INTO %s VALUES ('2025-12-22')", options.fullTableName()));

    validateTableSchema(
            session.table(options.fullTableName()).schema(),
            Pair.of("start_date", DataTypes.DateType));
  }

  @Override
  protected String tableFormat() {
    return "DELTA";
  }
}
