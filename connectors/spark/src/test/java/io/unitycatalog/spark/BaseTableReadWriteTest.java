package io.unitycatalog.spark;

import static io.unitycatalog.server.utils.TestUtils.CATALOG_NAME;
import static io.unitycatalog.server.utils.TestUtils.SCHEMA_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.client.ApiException;
import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public abstract class BaseTableReadWriteTest extends BaseSparkIntegrationTest {

  private static final String DELTA_TABLE = "test_delta";
  private static final String ANOTHER_DELTA_TABLE = "test_delta_another";
  private static final String DELTA_TABLE_PARTITIONED = "test_delta_partitioned";

  /**
   * Set up a delta table and returns the table full name. Subclasses need to override it
   * accordingly.
   */
  protected abstract String setupDeltaTable(
      String catalogName, String tableName, List<String> partitionColumns, SparkSession session)
      throws IOException, ApiException;

  /**
   * Set up a delta table with storage on emulated cloud and returns the table full name. Subclasses
   * need to override it accordingly.
   */
  protected abstract String setupDeltaTableForCloud(
      String scheme,
      String catalogName,
      String tableName,
      List<String> partitionColumns,
      SparkSession session)
      throws IOException, ApiException;

  @Test
  public void testDeltaTableReadWrite() throws IOException, ApiException {
    // Test both `spark_catalog` and other catalog names.
    session = createSparkSessionWithCatalogs(SPARK_CATALOG, CATALOG_NAME);

    String t1 = setupDeltaTable(SPARK_CATALOG, DELTA_TABLE, List.of(), session);
    testTableReadWrite(t1, session);

    String t2 =
        setupDeltaTable(SPARK_CATALOG, DELTA_TABLE_PARTITIONED, Arrays.asList("s"), session);
    testTableReadWrite(t2, session);

    String t3 = setupDeltaTable(CATALOG_NAME, DELTA_TABLE, List.of(), session);
    testTableReadWrite(t3, session);

    String t4 = setupDeltaTable(CATALOG_NAME, DELTA_TABLE_PARTITIONED, Arrays.asList("s"), session);
    testTableReadWrite(t4, session);
  }

  private void validateTimeTravelDeltaTable(Dataset<Row> df) {
    List<Row> rows = df.collectAsList();
    assertThat(rows).hasSize(1);
    assertThat(rows.get(0).getInt(0)).isEqualTo(1);
  }

  @Test
  public void testTimeTravelDeltaTable() throws ApiException, IOException {
    session = createSparkSessionWithCatalogs(SPARK_CATALOG);

    String t1 = setupDeltaTable(SPARK_CATALOG, DELTA_TABLE, List.of(), session);
    session.sql("INSERT INTO " + t1 + " SELECT 1, 'a'");

    String timestamp = Instant.now().toString();

    session.sql("INSERT INTO " + t1 + " SELECT 2, 'b'");

    // Time-travel to before the last insert, we should only see the first inserted row.
    validateTimeTravelDeltaTable(session.sql("SELECT * FROM " + t1 + " VERSION AS OF 1"));
    validateTimeTravelDeltaTable(
        session.sql("SELECT * FROM " + t1 + " TIMESTAMP AS OF '" + timestamp + "'"));
    validateTimeTravelDeltaTable(session.read().option("versionAsOf", 1).table(t1));
    validateTimeTravelDeltaTable(session.read().option("timestampAsOf", timestamp).table(t1));
  }

  @ParameterizedTest
  @MethodSource("cloudParameters")
  public void testCredentialDelta(String scheme, boolean renewCredEnabled)
      throws ApiException, IOException {
    session = createSparkSessionWithCatalogs(renewCredEnabled, SPARK_CATALOG, CATALOG_NAME);

    String t1 = setupDeltaTableForCloud(scheme, SPARK_CATALOG, DELTA_TABLE, List.of(), session);
    testTableReadWrite(t1, session);

    String t2 = setupDeltaTableForCloud(scheme, CATALOG_NAME, DELTA_TABLE, List.of(), session);
    testTableReadWrite(t2, session);

    Row row =
        session
            .sql(String.format("SELECT l.i FROM %s l JOIN %s r ON l.i = r.i", t1, t2))
            .collectAsList()
            .get(0);
    assertThat(row.getInt(0)).isEqualTo(1);
  }

  @ParameterizedTest
  @MethodSource("cloudParameters")
  public void testDeleteFromDeltaTable(String scheme, boolean renewCredEnabled)
      throws ApiException, IOException {
    session = createSparkSessionWithCatalogs(renewCredEnabled, SPARK_CATALOG);

    String t1 = setupDeltaTableForCloud(scheme, SPARK_CATALOG, DELTA_TABLE, List.of(), session);
    testTableReadWrite(t1, session);

    session.sql(String.format("DELETE FROM %s WHERE i = 1", t1));
    List<Row> rows = session.sql("SELECT * FROM " + t1).collectAsList();
    assertThat(rows).isEmpty();
  }

  @ParameterizedTest
  @MethodSource("cloudParameters")
  public void testMergeDeltaTable(String scheme, boolean renewCredEnabled)
      throws ApiException, IOException {
    session = createSparkSessionWithCatalogs(renewCredEnabled, SPARK_CATALOG, CATALOG_NAME);

    String t1 = setupDeltaTableForCloud(scheme, SPARK_CATALOG, DELTA_TABLE, List.of(), session);
    session.sql("INSERT INTO " + t1 + " SELECT 1, 'a'");

    String t2 =
        setupDeltaTableForCloud(scheme, CATALOG_NAME, ANOTHER_DELTA_TABLE, List.of(), session);
    session.sql("INSERT INTO " + t2 + " SELECT 2, 'b'");

    session.sql(
        String.format(
            "MERGE INTO %s USING %s ON %s.i = %s.i WHEN NOT MATCHED THEN INSERT *",
            t1, t2, t1, t2));
    List<Row> rows = session.sql("SELECT * FROM " + t1).collectAsList();
    assertThat(rows).hasSize(2);
  }

  @ParameterizedTest
  @MethodSource("cloudParameters")
  public void testUpdateDeltaTable(String scheme, boolean renewCredEnabled)
      throws ApiException, IOException {
    session = createSparkSessionWithCatalogs(renewCredEnabled, SPARK_CATALOG);

    String t1 = setupDeltaTableForCloud(scheme, SPARK_CATALOG, DELTA_TABLE, List.of(), session);
    session.sql("INSERT INTO " + t1 + " SELECT 1, 'a'");

    session.sql(String.format("UPDATE %s SET i = 2 WHERE i = 1", t1));
    List<Row> rows = session.sql("SELECT * FROM " + t1).collectAsList();
    assertThat(rows).hasSize(1);
    assertThat(rows.get(0).getInt(0)).isEqualTo(2);
  }

  @Test
  public void testShowTables() throws ApiException, IOException {
    session = createSparkSessionWithCatalogs(SPARK_CATALOG);
    setupDeltaTable(SPARK_CATALOG, DELTA_TABLE, List.of(), session);

    Row[] tables = (Row[]) session.sql("SHOW TABLES in " + SCHEMA_NAME).collect();
    assertThat(tables).hasSize(1);
    assertThat(tables[0].getString(0)).isEqualTo(SCHEMA_NAME);
    assertThat(tables[0].getString(1)).isEqualTo(DELTA_TABLE);

    assertThatThrownBy(() -> session.sql("SHOW TABLES in a.b.c").collect())
        .isInstanceOf(ApiException.class)
        .hasMessageContaining("Nested namespaces are not supported");
  }

  @Test
  public void testDropTable() throws ApiException, IOException {
    session = createSparkSessionWithCatalogs(SPARK_CATALOG);
    String fullName = setupDeltaTable(SPARK_CATALOG, DELTA_TABLE, List.of(), session);

    assertThat(session.catalog().tableExists(fullName)).isTrue();
    session.sql("DROP TABLE " + fullName).collect();
    assertThat(session.catalog().tableExists(fullName)).isFalse();
    assertThatThrownBy(() -> session.sql("DROP TABLE a.b.c.d").collect())
        .isInstanceOf(ApiException.class)
        .hasMessageContaining("Invalid table name");
  }

  protected void testTableReadWrite(String tableFullName, SparkSession session) {
    assertThat(session.sql("SELECT * FROM " + tableFullName).collectAsList()).isEmpty();
    session.sql("INSERT INTO " + tableFullName + " SELECT 1, 'a'");
    Row row = session.sql("SELECT * FROM " + tableFullName).collectAsList().get(0);
    assertThat(row.getInt(0)).isEqualTo(1);
    assertThat(row.getString(1)).isEqualTo("a");
  }
}
