package io.unitycatalog.spark;

import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;

public class DeltaExternalTableReadWriteTest extends ExternalTableReadWriteTest {

  @Test
  public void testDeltaPathTable() throws IOException {
    // We must replace the `spark_catalog` in order to support Delta path tables.
    session = createSparkSessionWithCatalogs(SPARK_CATALOG);

    int tableCounter = 0;
    for (boolean ctas : List.of(true, false)) {
      SetupTableOptions options = new SetupTableOptions();
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

  @Override
  protected String tableFormat() {
    return "DELTA";
  }
}
