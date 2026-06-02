package io.unitycatalog.spark;

import static io.unitycatalog.server.utils.TestUtils.CATALOG_NAME;
import static io.unitycatalog.server.utils.TestUtils.SCHEMA_NAME;
import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Date;
import java.util.List;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;

public class LocalDeltaManagedColumnDefaultTest extends BaseSparkIntegrationTest {

  @Test
  public void testColumnDefaultOnManagedDeltaTable() {
    session = createSparkSessionWithCatalogs(SPARK_CATALOG, CATALOG_NAME);
    ensureSparkCatalogSchemaExists();
    String fullTableName = CATALOG_NAME + "." + SCHEMA_NAME + ".test_delta_column_default";

    sql(
        "CREATE TABLE %s ("
            + "id INT, "
            + "s STRING DEFAULT 'x', "
            + "i INT DEFAULT 42, "
            + "l BIGINT DEFAULT 9000000000, "
            + "b BOOLEAN DEFAULT true, "
            + "d DOUBLE DEFAULT 1.5, "
            + "dt DATE DEFAULT DATE '2024-01-02') USING DELTA "
            + "TBLPROPERTIES ('%s' = '%s', 'delta.feature.allowColumnDefaults' = 'enabled')",
        fullTableName,
        UCTableProperties.DELTA_CATALOG_MANAGED_KEY_NEW,
        UCTableProperties.DELTA_CATALOG_MANAGED_VALUE);
    sql("INSERT INTO %s (id) VALUES (1)", fullTableName);

    List<Row> rows = sql("SELECT id, s, i, l, b, d, dt FROM %s", fullTableName);
    assertThat(rows).hasSize(1);
    Row row = rows.get(0);
    assertThat(row.getInt(0)).isEqualTo(1);
    assertThat(row.getString(1)).isEqualTo("x");
    assertThat(row.getInt(2)).isEqualTo(42);
    assertThat(row.getLong(3)).isEqualTo(9000000000L);
    assertThat(row.getBoolean(4)).isTrue();
    assertThat(row.getDouble(5)).isEqualTo(1.5);
    assertThat(row.get(6)).isEqualTo(Date.valueOf("2024-01-02"));
  }

  private void ensureSparkCatalogSchemaExists() {
    sql("CREATE DATABASE IF NOT EXISTS spark_catalog.%s", SCHEMA_NAME);
  }
}
