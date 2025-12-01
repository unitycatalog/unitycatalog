package io.unitycatalog.spark;

import static io.unitycatalog.server.utils.TestUtils.CATALOG_NAME;
import static io.unitycatalog.server.utils.TestUtils.SCHEMA_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * This test suite exercise all tests in BaseTableReadWriteTest plus extra tests that are dedicated
 * to managed tables.
 *
 * <p>Specifically the managed table creation logic has distinct behavior to test in this class: 1.
 * Automatically allocate storage path by server 2. Automatically enables UC as Delta commit
 * coordinator These behaviors are only relevant to managed tables.
 *
 * <p>This test needs to start server with a single managed root location on a single emulated cloud
 * so it can not test all emulated clouds yet.
 */
public abstract class ManagedTableReadWriteTest extends BaseTableReadWriteTest {
  private static final String DELTA_TABLE = "test_delta";

  @Test
  public void testCreateManagedTableErrors() {
    session = createSparkSessionWithCatalogs(CATALOG_NAME);
    String fullTableName = CATALOG_NAME + "." + SCHEMA_NAME + "." + DELTA_TABLE;
    assertThatThrownBy(() -> sql("CREATE TABLE %s(name STRING) USING parquet", fullTableName))
        .hasMessageContaining("not support non-Delta managed table");
    assertThatThrownBy(
            () ->
                sql(
                    "CREATE TABLE %s(name STRING) USING delta TBLPROPERTIES ('%s' = 'disabled')",
                    fullTableName, UCTableProperties.CATALOG_MANAGED_KEY))
        .hasMessageContaining(
            String.format("Should not specify property %s", UCTableProperties.CATALOG_MANAGED_KEY));
    assertThatThrownBy(
            () ->
                sql(
                    "CREATE TABLE %s(name STRING) USING delta TBLPROPERTIES ('%s' = 'disabled')",
                    fullTableName, UCTableProperties.CATALOG_MANAGED_KEY_NEW))
        .hasMessageContaining(
            String.format(
                "Should not specify property %s", UCTableProperties.CATALOG_MANAGED_KEY_NEW));
    assertThatThrownBy(
            () ->
                sql(
                    "CREATE TABLE %s(name STRING) USING delta TBLPROPERTIES ('%s' = 'some_id')",
                    fullTableName, UCTableProperties.UC_TABLE_ID_KEY))
        .hasMessageContaining(UCTableProperties.UC_TABLE_ID_KEY);
    assertThatThrownBy(
            () ->
                sql(
                    "CREATE TABLE %s(name STRING) USING delta TBLPROPERTIES ('%s' = 'some_id')",
                    fullTableName, UCTableProperties.UC_TABLE_ID_KEY_OLD))
        .hasMessageContaining(UCTableProperties.UC_TABLE_ID_KEY_OLD);
    assertThatThrownBy(
            () ->
                sql(
                    "CREATE TABLE %s(name STRING) USING delta " + "TBLPROPERTIES ('%s' = 'false')",
                    fullTableName, TableCatalog.PROP_IS_MANAGED_LOCATION))
        .hasMessageContaining("is_managed_location");
  }

  @ParameterizedTest
  @MethodSource("cloudParameters")
  public void testCreateManagedDeltaTable(String scheme, boolean renewCredEnabled) {
    session = createSparkSessionWithCatalogs(renewCredEnabled, SPARK_CATALOG, CATALOG_NAME);

    int counter = 0;
    for (boolean setProperty : List.of(true, false)) {
      for (boolean ctas : List.of(true, false)) {
        for (String catalogName : List.of(SPARK_CATALOG, CATALOG_NAME)) {
          String fullTableName = catalogName + "." + SCHEMA_NAME + "." + DELTA_TABLE + counter;
          counter++;
          // Setting this table property isn't necessary, but we should not throw an error.
          String propertyClause =
              setProperty
                  ? String.format(
                      "TBLPROPERTIES ('%s' = '%s')",
                      UCTableProperties.CATALOG_MANAGED_KEY,
                      UCTableProperties.CATALOG_MANAGED_VALUE)
                  : "";

          if (ctas) {
            sql(
                "CREATE TABLE %s USING delta %s AS SELECT 'a' AS name",
                fullTableName, propertyClause);
          } else {
            sql("CREATE TABLE %s(name STRING) USING delta %s", fullTableName, propertyClause);
          }
          sql("INSERT INTO " + fullTableName + " SELECT 'b'");

          List<Row> rows = sql("DESC EXTENDED " + fullTableName);
          Map<String, String> describeResult =
              rows.stream()
                  .collect(Collectors.toMap(row -> row.getString(0), row -> row.getString(1)));

          // Make sure the table created is managed and catalogOwned
          assertThat(describeResult.get("Name")).isEqualTo(fullTableName);
          assertThat(describeResult.get("Type")).isEqualTo("MANAGED");
          assertThat(describeResult.get("Provider")).isEqualToIgnoringCase("delta");
          assertThat(describeResult.get("Is_managed_location")).isEqualTo("true");
          assertThat(describeResult).containsKey("Table Properties");
          // Check that it either has the old table ID key or the new one. Doesn't matter if it
          // has both.
          String tableProperties = describeResult.get("Table Properties");
          Assertions.assertTrue(
              tableProperties.contains(UCTableProperties.UC_TABLE_ID_KEY)
                  || tableProperties.contains(UCTableProperties.UC_TABLE_ID_KEY_OLD));
          // Check that it either has the old feature name or the new one. We don't care if it
          // has both but Delta won't allow that.
          Assertions.assertTrue(
              tableProperties.contains(
                      String.format(
                          "%s=%s",
                          UCTableProperties.CATALOG_MANAGED_KEY,
                          UCTableProperties.CATALOG_MANAGED_VALUE))
                  || tableProperties.contains(
                      String.format(
                          "%s=%s",
                          UCTableProperties.CATALOG_MANAGED_KEY_NEW,
                          UCTableProperties.CATALOG_MANAGED_VALUE)));
        }
      }
    }
  }

  @Test
  public void hyphenInTableName() {
    testHyphenInTableNameBase(Optional.empty());
  }

  @Override
  protected String setupDeltaTable(
      String cloudScheme, String catalogName, String tableName, List<String> partitionColumns) {
    // For now, we only support testing one cloud, which is the one configured by
    // managedStorageCloudScheme(). Tests are only supposed to call this function with the correct
    // cloud scheme.
    assert cloudScheme.equals(managedStorageCloudScheme());
    String partitionClause;
    if (partitionColumns.isEmpty()) {
      partitionClause = "";
    } else {
      partitionClause = String.format(" PARTITIONED BY (%s)", String.join(", ", partitionColumns));
    }
    sql(
        "CREATE TABLE %s.%s.%s(i INT, s STRING) USING delta %s",
        catalogName, SCHEMA_NAME, tableName, partitionClause);
    return String.join(".", catalogName, SCHEMA_NAME, tableName);
  }
}
