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
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public abstract class BaseTableReadWriteTest extends BaseSparkIntegrationTest {

  private static final String DELTA_TABLE = "test_delta";
  private static final String ANOTHER_DELTA_TABLE = "test_delta_another";
  private static final String DELTA_TABLE_PARTITIONED = "test_delta_partitioned";

  /**
   * Set up a delta table and returns the table full name. This function is used by tests that
   * aren't cloud aware. It simply uses the same cloud as configured for managed storage.
   */
  protected final String setupDeltaTable(
      String catalogName, String tableName, List<String> partitionColumns)
      throws IOException, ApiException {
    return setupDeltaTable(managedStorageCloudScheme(), catalogName, tableName, partitionColumns);
  }

  /**
   * Set up a delta table with storage on emulated cloud and returns the table full name. Subclasses
   * need to override it accordingly.
   */
  protected abstract String setupDeltaTable(
      String cloudScheme, String catalogName, String tableName, List<String> partitionColumns)
      throws IOException, ApiException;

  @Test
  public void testDeltaTableReadWrite() throws IOException, ApiException {
    // Test both `spark_catalog` and other catalog names.
    session = createSparkSessionWithCatalogs(SPARK_CATALOG, CATALOG_NAME);

    String t1 = setupDeltaTable(SPARK_CATALOG, DELTA_TABLE, List.of());
    testTableReadWrite(t1);

    String t2 = setupDeltaTable(SPARK_CATALOG, DELTA_TABLE_PARTITIONED, Arrays.asList("s"));
    testTableReadWrite(t2);

    String t3 = setupDeltaTable(CATALOG_NAME, DELTA_TABLE, List.of());
    testTableReadWrite(t3);

    String t4 = setupDeltaTable(CATALOG_NAME, DELTA_TABLE_PARTITIONED, Arrays.asList("s"));
    testTableReadWrite(t4);
  }

  /**
   * This function checks that the first column of each row in {@code rows} matches {@code expected}
   * in both length and value.
   *
   * @param rows Rows to be checked
   * @param expected The expected integer values of first column
   */
  protected void validateRows(List<Row> rows, Integer... expected) {
    assertThat(rows).hasSize(expected.length);
    for (int i = 0; i < expected.length; i++) {
      assertThat(rows.get(i).getInt(0)).isEqualTo(expected[i]);
    }
  }

  /**
   * This function checks that the first and second column of each row in {@code rows} matches
   * {@code expected} in both length and value.
   *
   * @param rows Rows to be checked
   * @param expected The expected integer and string values of first two columns
   */
  protected void validateRows(List<Row> rows, Pair<Integer, String>... expected) {
    assertThat(rows).hasSize(expected.length);
    for (int i = 0; i < expected.length; i++) {
      Row row = rows.get(i);
      assertThat(row.getInt(0)).isEqualTo(expected[i].getLeft());
      assertThat(row.getString(1)).isEqualTo(expected[i].getRight());
    }
  }

  @Test
  public void testTimeTravelDeltaTable() throws ApiException, IOException {
    session = createSparkSessionWithCatalogs(SPARK_CATALOG);

    String t1 = setupDeltaTable(SPARK_CATALOG, DELTA_TABLE, List.of());
    sql("INSERT INTO %s SELECT 1, 'a'", t1);

    String timestamp = Instant.now().toString();

    sql("INSERT INTO %s SELECT 2, 'b'", t1);

    // Time-travel to before the last insert, we should only see the first inserted row.
    validateRows(sql("SELECT * FROM %s VERSION AS OF 1", t1), 1);
    validateRows(sql("SELECT * FROM %s TIMESTAMP AS OF '%s'", t1, timestamp), 1);
    validateRows(session.read().option("versionAsOf", 1).table(t1).collectAsList(), 1);
    validateRows(session.read().option("timestampAsOf", timestamp).table(t1).collectAsList(), 1);
  }

  @ParameterizedTest
  @MethodSource("cloudParameters")
  public void testCredentialDelta(String scheme, boolean renewCredEnabled)
      throws ApiException, IOException {
    session = createSparkSessionWithCatalogs(renewCredEnabled, SPARK_CATALOG, CATALOG_NAME);

    String t1 = setupDeltaTable(scheme, SPARK_CATALOG, DELTA_TABLE, List.of());
    testTableReadWrite(t1);

    String t2 = setupDeltaTable(scheme, CATALOG_NAME, DELTA_TABLE, List.of());
    testTableReadWrite(t2);

    validateRows(sql("SELECT l.i FROM %s l JOIN %s r ON l.i = r.i", t1, t2), 1);
  }

  @ParameterizedTest
  @MethodSource("cloudParameters")
  public void testMergeDeleteUpdateDeltaTable(String scheme, boolean renewCredEnabled)
      throws ApiException, IOException {
    session = createSparkSessionWithCatalogs(renewCredEnabled, SPARK_CATALOG, CATALOG_NAME);

    // Test MERGE. The table t1 will have (1, 'a') and (2, 'b')
    String t1 = setupDeltaTable(scheme, SPARK_CATALOG, DELTA_TABLE, List.of());
    sql("INSERT INTO %s SELECT 1, 'a'", t1);

    String t2 = setupDeltaTable(scheme, CATALOG_NAME, ANOTHER_DELTA_TABLE, List.of());
    sql("INSERT INTO %s SELECT 2, 'b'", t2);

    sql("MERGE INTO %s USING %s ON %s.i = %s.i WHEN NOT MATCHED THEN INSERT *", t1, t2, t1, t2);
    validateRows(sql("SELECT * FROM %s ORDER BY i", t1), Pair.of(1, "a"), Pair.of(2, "b"));

    // Test DELETE. The table t1 will have (1, 'a')
    sql("DELETE FROM %s WHERE i = 2", t1);
    validateRows(sql("SELECT * FROM %s", t1), Pair.of(1, "a"));

    // Test UPDATE. The table t1 will have (2, 'a')
    sql("UPDATE %s SET i = 2 WHERE i = 1", t1);
    validateRows(sql("SELECT * FROM %s", t1), Pair.of(2, "a"));
  }

  @Test
  public void testShowTables() throws ApiException, IOException {
    session = createSparkSessionWithCatalogs(SPARK_CATALOG);
    setupDeltaTable(SPARK_CATALOG, DELTA_TABLE, List.of());

    List<Row> tables = sql("SHOW TABLES in %s", SCHEMA_NAME);
    assertThat(tables).hasSize(1);
    assertThat(tables.get(0).getString(0)).isEqualTo(SCHEMA_NAME);
    assertThat(tables.get(0).getString(1)).isEqualTo(DELTA_TABLE);

    assertThatThrownBy(() -> sql("SHOW TABLES in a.b.c"))
        .isInstanceOf(ApiException.class)
        .hasMessageContaining("Nested namespaces are not supported");
  }

  @Test
  public void testDropTable() throws ApiException, IOException {
    session = createSparkSessionWithCatalogs(SPARK_CATALOG);
    String fullName = setupDeltaTable(SPARK_CATALOG, DELTA_TABLE, List.of());

    assertThat(session.catalog().tableExists(fullName)).isTrue();
    sql("DROP TABLE %s", fullName);
    assertThat(session.catalog().tableExists(fullName)).isFalse();
    assertThatThrownBy(() -> sql("DROP TABLE a.b.c.d"))
        .isInstanceOf(ApiException.class)
        .hasMessageContaining("Nested namespaces are not supported");
  }

  protected String quoteEntityName(String entityName) {
    return Arrays.stream(entityName.split("\\."))
        .map(x -> x.contains("-") ? String.format("`%s`", x) : x)
        .collect(Collectors.joining("."));
  }

  protected void testTableReadWrite(String tableFullName) {
    assertThat(sql("SELECT * FROM %s", quoteEntityName(tableFullName))).isEmpty();
    sql("INSERT INTO %s SELECT 1, 'a'", quoteEntityName(tableFullName));
    validateRows(sql("SELECT * FROM %s", quoteEntityName(tableFullName)), Pair.of(1, "a"));
  }
}
