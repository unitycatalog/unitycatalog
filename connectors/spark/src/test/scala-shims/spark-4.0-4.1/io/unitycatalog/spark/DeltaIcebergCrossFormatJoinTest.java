package io.unitycatalog.spark;

import static io.unitycatalog.server.utils.TestUtils.CATALOG_NAME;
import static io.unitycatalog.server.utils.TestUtils.SCHEMA_NAME;
import static io.unitycatalog.server.utils.TestUtils.createApiClient;
import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.DataSourceFormat;
import io.unitycatalog.server.base.table.TableOperations;
import io.unitycatalog.server.sdk.tables.SdkTableOperations;
import io.unitycatalog.server.utils.ServerProperties;
import java.util.List;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Verifies that a single SparkSession can be configured with both a Delta-capable catalog (the UC
 * connector, {@link UCSingleCatalog}) and an Iceberg catalog (Iceberg's own REST {@code
 * SparkCatalog} against UC's Iceberg REST endpoint) at the same time, and can join a Delta table
 * against an Iceberg table across the two catalogs.
 *
 * <p>Both catalogs point at the same UC server: the Delta table lives in the {@code spark_catalog}
 * UC catalog (reached through the connector, which also satisfies Delta's required-catalog check)
 * and the Iceberg table in {@code CATALOG_NAME} (reached through the Iceberg REST client). The
 * shared {@code createSparkSessionWithCatalogs} derives both session extensions from the catalog
 * mix, and storage is a local {@code file://} warehouse.
 *
 * <p>Compiled only for Spark 4.0 / 4.1: Iceberg 1.11.0 publishes no Spark 4.2 runtime, so this
 * source lives under {@code src/test/scala-shims/spark-4.0-4.1} (see {@link
 * IcebergTableReadWriteTest}).
 */
public class DeltaIcebergCrossFormatJoinTest extends BaseSparkIntegrationTest {

  private TableOperations tableOperations;

  @BeforeEach
  @Override
  public void setUp() {
    super.setUp();
    tableOperations = new SdkTableOperations(createApiClient(serverConfig));
  }

  @Override
  protected void setUpProperties() {
    super.setUpProperties();
    serverProperties.setProperty(ServerProperties.Property.ICEBERG_TABLE_ENABLED.getKey(), "true");
  }

  // Only CATALOG_NAME is Iceberg; SPARK_CATALOG stays on the UC connector for Delta, so the base
  // wires a mixed session with both extensions.
  @Override
  protected boolean isIcebergCatalog(String catalog) {
    return catalog.equals(CATALOG_NAME);
  }

  @Test
  public void testJoinDeltaAndIcebergTables() throws ApiException {
    session = createSparkSessionWithCatalogs(false, false, SPARK_CATALOG, CATALOG_NAME);

    String deltaTable = String.join(".", SPARK_CATALOG, SCHEMA_NAME, "delta_events");
    String icebergTable = String.join(".", CATALOG_NAME, SCHEMA_NAME, "iceberg_events");

    sql("CREATE TABLE %s (id INT, delta_val STRING) USING delta", deltaTable);
    sql("INSERT INTO %s VALUES (1, 'delta-one'), (2, 'delta-two')", deltaTable);

    sql("CREATE TABLE %s (id INT, iceberg_val STRING) USING iceberg", icebergTable);
    sql("INSERT INTO %s VALUES (1, 'iceberg-one'), (3, 'iceberg-three')", icebergTable);

    // The two tables are genuinely different formats in UC.
    assertThat(tableOperations.getTable(deltaTable).getDataSourceFormat())
        .isEqualTo(DataSourceFormat.DELTA);
    assertThat(tableOperations.getTable(icebergTable).getDataSourceFormat())
        .isEqualTo(DataSourceFormat.ICEBERG);

    // Cross-format inner join on id: only id = 1 exists in both tables.
    List<Row> joined =
        sql(
            "SELECT d.id, d.delta_val, i.iceberg_val FROM %s d JOIN %s i ON d.id = i.id",
            deltaTable, icebergTable);
    assertThat(joined).hasSize(1);
    Row row = joined.get(0);
    assertThat(row.getInt(0)).isEqualTo(1);
    assertThat(row.getString(1)).isEqualTo("delta-one");
    assertThat(row.getString(2)).isEqualTo("iceberg-one");
  }
}
