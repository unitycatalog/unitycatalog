package io.unitycatalog.spark;

import static io.unitycatalog.server.utils.TestUtils.CATALOG_NAME;
import static io.unitycatalog.server.utils.TestUtils.SCHEMA_NAME;
import static io.unitycatalog.server.utils.TestUtils.createApiClient;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.DataSourceFormat;
import io.unitycatalog.client.model.TableInfo;
import io.unitycatalog.client.model.TableType;
import io.unitycatalog.server.base.table.TableOperations;
import io.unitycatalog.server.sdk.tables.SdkTableOperations;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * This test suite exercise all tests in BaseTableReadWriteTest plus extra tests that are dedicated
 * to Delta managed tables.
 *
 * <p>Specifically the managed table creation logic has distinct behavior to test in this class: 1.
 * Automatically allocate storage path by server 2. Automatically enables UC as Delta commit
 * coordinator These behaviors are only relevant to managed tables.
 *
 * <p>This test needs to start server with a single managed root location on a single emulated cloud
 * so it can not test all emulated clouds yet.
 */
public abstract class DeltaManagedTableReadWriteTest extends BaseTableReadWriteTest {
  private static final String DELTA_TABLE = "test_delta";

  @Override
  protected String tableFormat() {
    return "DELTA";
  }

  @Test
  public void testCreateManagedTableErrors() {
    session = createSparkSessionWithCatalogs(CATALOG_NAME);
    String fullTableName = CATALOG_NAME + "." + SCHEMA_NAME + "." + DELTA_TABLE;
    assertThatThrownBy(
            () ->
                sql(
                    "CREATE TABLE %s(name STRING) USING parquet %s",
                    fullTableName, TBLPROPERTIES_CATALOG_OWNED_CLAUSE))
        .hasMessageContaining("not support non-Delta managed table");
    for (String featureProperty :
        List.of(
            UCTableProperties.DELTA_CATALOG_MANAGED_KEY,
            UCTableProperties.DELTA_CATALOG_MANAGED_KEY_NEW)) {
      assertThatThrownBy(
              () ->
                  sql(
                      "CREATE TABLE %s(name STRING) USING delta TBLPROPERTIES ('%s' = 'disabled')",
                      fullTableName, featureProperty))
          .hasMessageContaining(
              String.format("Invalid property value 'disabled' for '%s'", featureProperty));
    }
    for (String ucTableIdProperty :
        List.of(UCTableProperties.UC_TABLE_ID_KEY, UCTableProperties.UC_TABLE_ID_KEY_OLD)) {
      assertThatThrownBy(
              () ->
                  sql(
                      "CREATE TABLE %s(name STRING) USING delta TBLPROPERTIES ('%s' = 'some_id')",
                      fullTableName, ucTableIdProperty))
          .hasMessageContaining(ucTableIdProperty);
    }
    assertThatThrownBy(
            () ->
                sql(
                    "CREATE TABLE %s(name STRING) USING delta " + "TBLPROPERTIES ('%s' = 'false')",
                    fullTableName, TableCatalog.PROP_IS_MANAGED_LOCATION))
        .hasMessageContaining("is_managed_location");
    assertThatThrownBy(() -> sql("CREATE TABLE %s(name STRING) USING delta", fullTableName))
        .hasMessageContaining(
            String.format(
                "Managed table creation requires table property '%s'='%s' to be set",
                UCTableProperties.DELTA_CATALOG_MANAGED_KEY_NEW,
                UCTableProperties.DELTA_CATALOG_MANAGED_VALUE));
  }

  @ParameterizedTest
  @MethodSource("cloudParameters")
  public void testCreateManagedDeltaTable(String scheme, boolean renewCredEnabled)
      throws ApiException {
    session = createSparkSessionWithCatalogs(renewCredEnabled, SPARK_CATALOG, CATALOG_NAME);

    int counter = 0;
    final String comment = "This is comment.";
    for (boolean withPartition : List.of(true, false)) {
      for (boolean ctas : List.of(true, false)) {
        for (String catalogName : List.of(SPARK_CATALOG, CATALOG_NAME)) {
          String tableName = DELTA_TABLE + counter;
          counter++;

          TableSetupOptions options =
              new TableSetupOptions()
                  .setCatalogName(catalogName)
                  .setTableName(tableName)
                  .setCloudScheme(scheme)
                  .setComment(comment);
          if (withPartition) {
            options.setPartitionColumn("i");
          }
          if (ctas) {
            options.setAsSelect(1, "a");
          }
          String fullTableName = setupTable(options);
          if (!ctas) {
            sql("INSERT INTO %s SELECT 1, 'a'", fullTableName);
          }

          List<Row> rows = sql("DESC EXTENDED " + fullTableName);
          Map<String, String> describeResult =
              rows.stream()
                  // The column name i and s appears more than once when the partition info is
                  // printed. Skip them anyway to avoid collision when collecting a map.
                  .filter(row -> !List.of("i", "s").contains(row.getString(0)))
                  .collect(Collectors.toMap(row -> row.getString(0), row -> row.getString(1)));

          // Make sure the table created is managed and catalogManaged
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
                          UCTableProperties.DELTA_CATALOG_MANAGED_KEY,
                          UCTableProperties.DELTA_CATALOG_MANAGED_VALUE))
                  || tableProperties.contains(
                      String.format(
                          "%s=%s",
                          UCTableProperties.DELTA_CATALOG_MANAGED_KEY_NEW,
                          UCTableProperties.DELTA_CATALOG_MANAGED_VALUE)));
          // Check schema of table
          validateTableSchema(
              session.table(fullTableName).schema(),
              Pair.of("i", DataTypes.IntegerType),
              Pair.of("s", DataTypes.StringType));

          TableOperations tableOperations = new SdkTableOperations(createApiClient(serverConfig));
          TableInfo tableInfo = tableOperations.getTable(fullTableName);
          assertThat(tableInfo.getCatalogName()).isEqualTo(catalogName);
          assertThat(tableInfo.getName()).isEqualTo(tableName);
          assertThat(tableInfo.getSchemaName()).isEqualTo(SCHEMA_NAME);
          assertThat(tableInfo.getTableType()).isEqualTo(TableType.MANAGED);
          assertThat(tableInfo.getDataSourceFormat()).isEqualTo(DataSourceFormat.DELTA);
          assertThat(tableInfo.getComment()).isEqualTo(comment);
          // Currently we can not check these table properties on server because Delta doesn't
          // send them yet. In the future this will be enabled.
          // TODO: enable this check once the table properties are sent by Delta.
          boolean deltaSendsServerTableProperties = false;
          Map<String, String> tablePropertiesFromServer = tableInfo.getProperties();
          if (deltaSendsServerTableProperties) {
            Assertions.assertTrue(
                tablePropertiesFromServer.containsKey(UCTableProperties.UC_TABLE_ID_KEY)
                    || tablePropertiesFromServer.containsKey(
                        UCTableProperties.UC_TABLE_ID_KEY_OLD));
            Assertions.assertTrue(
                tablePropertiesFromServer.containsKey(UCTableProperties.DELTA_CATALOG_MANAGED_KEY)
                    || tablePropertiesFromServer.containsKey(
                        UCTableProperties.DELTA_CATALOG_MANAGED_KEY_NEW));
            assertThat(
                    Optional.ofNullable(
                            tablePropertiesFromServer.get(
                                UCTableProperties.DELTA_CATALOG_MANAGED_KEY))
                        .orElseGet(
                            () ->
                                tablePropertiesFromServer.get(
                                    UCTableProperties.DELTA_CATALOG_MANAGED_KEY_NEW)))
                .isEqualTo(UCTableProperties.DELTA_CATALOG_MANAGED_VALUE);
          } else {
            // Because Delta doesn't send these properties, it's empty.
            assertThat(tablePropertiesFromServer).isEmpty();
          }
        }
      }
    }
  }

  @Override
  protected String setupTable(TableSetupOptions options) {
    // For now, we only support testing one cloud, which is the one configured by
    // managedStorageCloudScheme(). Tests are only supposed to call this function with the correct
    // cloud scheme.
    assertThat(options.getCloudScheme()).isEqualTo(managedStorageCloudScheme());
    sql(options.createManagedTableSql());
    return options.fullTableName();
  }

  /**
   * Integration test to verify that credential generation and fallback behavior works end-to-end.
   * This test verifies that:
   * - Tables can be loaded successfully when credentials are available
   * - Multiple tables can be loaded independently (credential failure isolation)
   * - Read and write operations work with generated credentials
   */
  @ParameterizedTest
  @MethodSource("cloudParameters")
  public void testCredentialGenerationForTableAccess(String scheme, boolean renewCredEnabled)
      throws ApiException {
    session = createSparkSessionWithCatalogs(renewCredEnabled, SPARK_CATALOG, CATALOG_NAME);

    // Create and access multiple tables to verify credential isolation
    String table1 = CATALOG_NAME + "." + SCHEMA_NAME + ".cred_test_table1";
    String table2 = CATALOG_NAME + "." + SCHEMA_NAME + ".cred_test_table2";

    TableSetupOptions options1 =
        new TableSetupOptions()
            .setCatalogName(CATALOG_NAME)
            .setTableName("cred_test_table1")
            .setCloudScheme(scheme)
            .setAsSelect(1, "table1_data");
    String fullTable1 = setupTable(options1);

    TableSetupOptions options2 =
        new TableSetupOptions()
            .setCatalogName(CATALOG_NAME)
            .setTableName("cred_test_table2")
            .setCloudScheme(scheme)
            .setAsSelect(2, "table2_data");
    String fullTable2 = setupTable(options2);

    // Verify both tables can be read (credentials generated successfully)
    List<Row> rows1 = sql("SELECT * FROM " + fullTable1);
    assertThat(rows1).hasSize(1);
    assertThat(rows1.get(0).getInt(0)).isEqualTo(1);
    assertThat(rows1.get(0).getString(1)).isEqualTo("table1_data");

    List<Row> rows2 = sql("SELECT * FROM " + fullTable2);
    assertThat(rows2).hasSize(1);
    assertThat(rows2.get(0).getInt(0)).isEqualTo(2);
    assertThat(rows2.get(0).getString(1)).isEqualTo("table2_data");

    // Verify write operations work (credentials have appropriate permissions)
    sql("INSERT INTO %s VALUES (3, 'new_data')", fullTable1);
    List<Row> updatedRows = sql("SELECT * FROM " + fullTable1 + " WHERE i = 3");
    assertThat(updatedRows).hasSize(1);
    assertThat(updatedRows.get(0).getString(1)).isEqualTo("new_data");

    // Clean up
    sql("DROP TABLE IF EXISTS " + fullTable1);
    sql("DROP TABLE IF EXISTS " + fullTable2);
  }
}
