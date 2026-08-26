package io.unitycatalog.spark;

import static io.unitycatalog.server.utils.TestUtils.CATALOG_NAME;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Integration tests for path-based data source queries while a Unity Catalog is current. */
public class PathTableReadTest extends BaseSparkIntegrationTest {

  @TempDir protected File dataDir;

  @Test
  public void testBareParquetPathWithCurrentUcCatalog() throws IOException {
    session = createSparkSessionWithCatalogs(SPARK_CATALOG, CATALOG_NAME);
    String location = new File(dataDir, "parquet-path").getCanonicalPath();
    sql("INSERT OVERWRITE DIRECTORY '%s' USING parquet SELECT 1 AS id", location);

    sql("SET CATALOG %s", CATALOG_NAME);
    List<Row> rows = sql("SELECT * FROM parquet.`%s`", location);

    assertThat(rows).hasSize(1);
    assertThat(rows.get(0).getInt(0)).isEqualTo(1);
  }
}
