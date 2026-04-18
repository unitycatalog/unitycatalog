package io.unitycatalog.spark;

import static io.unitycatalog.server.utils.TestUtils.CATALOG_NAME;
import static io.unitycatalog.server.utils.TestUtils.SCHEMA_NAME;
import static io.unitycatalog.server.utils.TestUtils.createApiClient;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.ColumnInfo;
import io.unitycatalog.client.model.DataSourceFormat;
import io.unitycatalog.client.model.TableInfo;
import io.unitycatalog.client.model.TableType;
import io.unitycatalog.server.base.table.TableOperations;
import io.unitycatalog.server.sdk.tables.SdkTableOperations;
import java.util.List;
import java.util.Map;
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
  protected boolean isManagedTable() {
    return true;
  }

  @Override
  protected List<TableDdlMode> supportedTableDdlModes() {
    return List.of(TableDdlMode.CREATE);
  }

  @Override
  protected List<String> supportedCatalogNames() {
    return List.of(CATALOG_NAME);
  }

  @Override
  protected List<String> sessionCatalogNames() {
    return List.of(SPARK_CATALOG, CATALOG_NAME);
  }

  @Override
  protected void initializeSessionForTableTests() {
    ensureSparkCatalogSchemaExists();
  }

  @Override
  protected String tableFormat() {
    return "DELTA";
  }

  @Test
  public void testCreateManagedTableErrors() {
    session = createSparkSessionWithCatalogs(CATALOG_NAME);
    ensureSparkCatalogSchemaExists();
    String fullTableName = CATALOG_NAME + "." + SCHEMA_NAME + "." + DELTA_TABLE;
    assertThatThrownBy(
            () ->
                sql(
                    "CREATE TABLE %s(name STRING) USING parquet %s",
                    fullTableName, TBLPROPERTIES_CATALOG_OWNED_CLAUSE))
        .hasMessageContaining("not support non-Delta managed table");
    assertThatThrownBy(
            () ->
                sql(
                    "CREATE TABLE %s(name STRING) USING delta TBLPROPERTIES ('%s' = 'disabled')",
                    fullTableName, UCTableProperties.DELTA_CATALOG_MANAGED_KEY_NEW))
        .hasMessageContaining(
            String.format(
                "Invalid property value 'disabled' for '%s'",
                UCTableProperties.DELTA_CATALOG_MANAGED_KEY_NEW));
    assertThatThrownBy(
            () ->
                sql(
                    "CREATE TABLE %s(name STRING) USING delta TBLPROPERTIES ('%s' = 'some_id')",
                    fullTableName, UCTableProperties.UC_TABLE_ID_KEY))
        .hasMessageContaining(UCTableProperties.UC_TABLE_ID_KEY);
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
  public void testCreateManagedDeltaTable(
      String scheme, boolean renewCredEnabled, boolean credScopedFsEnabled) throws ApiException {
    session =
        createSparkSessionWithCatalogs(
            renewCredEnabled, credScopedFsEnabled, SPARK_CATALOG, CATALOG_NAME);
    ensureSparkCatalogSchemaExists();

    int counter = 0;
    final String comment = "This is comment.";
    for (boolean withPartition : List.of(true, false)) {
      for (boolean ctas : List.of(false)) {
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
          String tableProperties = describeResult.get("Table Properties");
          assertThat(tableProperties).contains(UCTableProperties.UC_TABLE_ID_KEY);
          assertThat(tableProperties)
              .contains(
                  String.format(
                      "%s=%s",
                      UCTableProperties.DELTA_CATALOG_MANAGED_KEY_NEW,
                      UCTableProperties.DELTA_CATALOG_MANAGED_VALUE));
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
          Map<String, String> tablePropertiesFromServer = tableInfo.getProperties();
          assertThat(tablePropertiesFromServer)
              .containsEntry(UCTableProperties.UC_TABLE_ID_KEY, tableInfo.getTableId())
              .containsEntry(
                  UCTableProperties.DELTA_CATALOG_MANAGED_KEY_NEW,
                  UCTableProperties.DELTA_CATALOG_MANAGED_VALUE);
        }
      }
    }
  }

  @Test
  public void testManagedDeltaReplaceRejectsMetadataChanges() {
    session = createSparkSessionWithCatalogs(SPARK_CATALOG, CATALOG_NAME);
    ensureSparkCatalogSchemaExists();
    String fullTableName =
        setupTable(new TableSetupOptions().setCatalogName(CATALOG_NAME).setTableName(TEST_TABLE));

    assertManagedReplaceRejected(
        List.of(
            String.format("REPLACE TABLE %s (i INT, renamed STRING) USING DELTA", fullTableName),
            String.format(
                "REPLACE TABLE %s (i INT, s STRING) USING DELTA PARTITIONED BY (s)", fullTableName),
            String.format(
                "CREATE OR REPLACE TABLE %s (i INT COMMENT 'new comment', s STRING) USING DELTA",
                fullTableName),
            String.format(
                "REPLACE TABLE %s USING DELTA AS SELECT 1 AS i, 2 AS extra_col", fullTableName),
            String.format(
                "CREATE OR REPLACE TABLE %s (i INT, extra_col INT) USING DELTA", fullTableName)));
  }

  @Test
  public void testManagedDeltaAlterTablePropertiesAndAppend() throws ApiException {
    session = createSparkSessionWithCatalogs(SPARK_CATALOG, CATALOG_NAME);
    ensureSparkCatalogSchemaExists();
    String fullTableName =
        setupTable(new TableSetupOptions().setCatalogName(CATALOG_NAME).setTableName(TEST_TABLE));

    sql("INSERT INTO %s SELECT 1, 'a'", fullTableName);
    sql("ALTER TABLE %s SET TBLPROPERTIES ('demo.flag' = 'on')", fullTableName);
    sql("INSERT INTO %s SELECT 2, 'b'", fullTableName);

    validateRows(
        sql("SELECT * FROM %s ORDER BY i", fullTableName), Pair.of(1, "a"), Pair.of(2, "b"));
    TableInfo tableInfo = loadTableInfo(fullTableName);
    assertManagedTableHasUcProperties(tableInfo, tableInfo.getTableId());
    assertThat(tableInfo.getProperties()).containsEntry("demo.flag", "on");
  }

  @Test
  public void testManagedDeltaAddColumnAndAppend() throws ApiException {
    session = createSparkSessionWithCatalogs(SPARK_CATALOG, CATALOG_NAME);
    ensureSparkCatalogSchemaExists();
    String fullTableName =
        setupTable(new TableSetupOptions().setCatalogName(CATALOG_NAME).setTableName(TEST_TABLE));

    sql("INSERT INTO %s SELECT 1, 'a'", fullTableName);
    sql("ALTER TABLE %s ADD COLUMNS (extra STRING)", fullTableName);
    sql("INSERT INTO %s SELECT 2, 'b', 'new'", fullTableName);

    assertThat(session.table(fullTableName).schema().fieldNames())
        .containsExactly("i", "s", "extra");
    List<Row> rows = sql("SELECT * FROM %s ORDER BY i", fullTableName);
    assertThat(rows).hasSize(2);
    assertThat(rows.get(0).getInt(0)).isEqualTo(1);
    assertThat(rows.get(0).getString(1)).isEqualTo("a");
    assertThat(rows.get(0).isNullAt(2)).isTrue();
    assertThat(rows.get(1).getInt(0)).isEqualTo(2);
    assertThat(rows.get(1).getString(1)).isEqualTo("b");
    assertThat(rows.get(1).getString(2)).isEqualTo("new");

    TableInfo tableInfo = loadTableInfo(fullTableName);
    assertManagedTableHasUcProperties(tableInfo, tableInfo.getTableId());
    assertThat(tableInfo.getColumns())
        .extracting(ColumnInfo::getName)
        .containsExactly("i", "s", "extra");
  }

  @Test
  public void testManagedDeltaReplaceRejectsProviderChangeWithoutDroppingTable()
      throws ApiException {
    session = createSparkSessionWithCatalogs(SPARK_CATALOG, CATALOG_NAME);
    ensureSparkCatalogSchemaExists();
    String fullTableName =
        setupTable(new TableSetupOptions().setCatalogName(CATALOG_NAME).setTableName(TEST_TABLE));
    sql("INSERT INTO %s SELECT 1, 'old'", fullTableName);
    String originalTableId = loadTableInfo(fullTableName).getTableId();

    assertThatThrownBy(
            () -> sql("REPLACE TABLE %s USING PARQUET AS SELECT 2 AS i, 'new' AS s", fullTableName))
        .hasMessageContaining("requires USING DELTA")
        .hasMessageContaining("Cannot change table format from DELTA to PARQUET");

    validateRows(sql("SELECT * FROM %s", fullTableName), Pair.of(1, "old"));
    assertManagedTableHasUcProperties(loadTableInfo(fullTableName), originalTableId);
  }

  @Override
  protected void prepareExistingTableForDdl(TableSetupOptions options) {
    TableSetupOptions existingOptions =
        new TableSetupOptions()
            .setCatalogName(options.getCatalogName())
            .setTableName(options.getTableName())
            .setCloudScheme(options.getCloudScheme())
            .setComment("existing table");
    if (!options.getPartitionColumns().isEmpty()) {
      existingOptions.setPartitionColumn(options.getPartitionColumns().get(0));
    }
    if (options.getAsSelect().isPresent()) {
      existingOptions.setAsSelect(0, "existing");
    }
    String fullTableName = setupTable(existingOptions);
    if (options.getAsSelect().isEmpty()) {
      sql("INSERT INTO %s SELECT 0, 'existing'", fullTableName);
    }
  }

  @Override
  protected void validateCreatedTable(String fullTableName, TableSetupOptions options)
      throws ApiException {
    assertManagedTableHasUcProperties(
        loadTableInfo(fullTableName), options.getExpectedTableId().orElse(null));
  }

  @Override
  protected List<String> expectedCreateFailureMessages(TableSetupOptions options) {
    return super.expectedCreateFailureMessages(options);
  }

  private TableInfo loadTableInfo(String fullTableName) throws ApiException {
    TableOperations tableOperations = new SdkTableOperations(createApiClient(serverConfig));
    return tableOperations.getTable(fullTableName);
  }

  private void assertManagedTableHasUcProperties(TableInfo tableInfo, String expectedTableId) {
    if (expectedTableId != null) {
      Assertions.assertEquals(expectedTableId, tableInfo.getTableId());
    }

    Map<String, String> tablePropertiesFromServer = tableInfo.getProperties();
    assertThat(tablePropertiesFromServer).containsKey(UCTableProperties.UC_TABLE_ID_KEY);
    assertThat(tablePropertiesFromServer)
        .containsEntry(
            UCTableProperties.DELTA_CATALOG_MANAGED_KEY_NEW,
            UCTableProperties.DELTA_CATALOG_MANAGED_VALUE);
  }

  private void assertManagedReplaceRejected(List<String> statements) {
    for (String statement : statements) {
      Throwable thrown = org.assertj.core.api.Assertions.catchThrowable(() -> sql(statement));
      assertThat(thrown).isNotNull();
      assertThat(thrown.getMessage())
          .containsAnyOf(
              "DELTA_OPERATION_NOT_ALLOWED",
              "DELTA_CANNOT_REPLACE_MISSING_TABLE",
              "DELTA_CONFIGURE_SPARK_SESSION_WITH_EXTENSION_AND_CATALOG",
              "SCHEMA_NOT_FOUND");
    }
  }

  private void ensureSparkCatalogSchemaExists() {
    sql("CREATE DATABASE IF NOT EXISTS spark_catalog.%s", SCHEMA_NAME);
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
}
