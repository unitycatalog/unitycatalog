package io.unitycatalog.server.base.table;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.*;
import io.unitycatalog.server.base.BaseCRUDTest;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.schema.SchemaOperations;
import io.unitycatalog.server.utils.TestUtils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public abstract class BaseTableCRUDTest extends BaseCRUDTest {

  protected SchemaOperations schemaOperations;
  protected TableOperations tableOperations;
  private String schemaId;

  ColumnInfo columnInfo1 =
      new ColumnInfo()
          .name("as_int")
          .typeText("INTEGER")
          .typeJson("{\"type\": \"integer\"}")
          .typeName(ColumnTypeName.INT)
          .typePrecision(10)
          .typeScale(0)
          .position(0)
          .comment("Integer column")
          .nullable(true);
  ColumnInfo columnInfo2 =
      new ColumnInfo()
          .name("as_string")
          .typeText("VARCHAR(255)")
          .typeJson("{\"type\": \"string\", \"length\": \"255\"}")
          .typeName(ColumnTypeName.STRING)
          .position(1)
          .comment("String column")
          .nullable(true);

  protected abstract SchemaOperations createSchemaOperations(ServerConfig serverConfig);

  protected abstract TableOperations createTableOperations(ServerConfig serverConfig);

  @BeforeEach
  @Override
  public void setUp() {
    super.setUp();
    schemaOperations = createSchemaOperations(serverConfig);
    tableOperations = createTableOperations(serverConfig);
  }

  protected void createCommonResources() throws ApiException {
    CreateCatalog createCatalog =
        new CreateCatalog().name(TestUtils.CATALOG_NAME).comment(TestUtils.COMMENT);
    catalogOperations.createCatalog(createCatalog);

    SchemaInfo schemaInfo =
        schemaOperations.createSchema(
            new CreateSchema().name(TestUtils.SCHEMA_NAME).catalogName(TestUtils.CATALOG_NAME));
    schemaId = schemaInfo.getSchemaId();
  }

  @Test
  public void testTableCRUD() throws IOException, ApiException {
    assertThatThrownBy(() -> tableOperations.getTable(TestUtils.TABLE_FULL_NAME))
        .isInstanceOf(Exception.class);
    createCommonResources();

    // Create and verify a table
    TableInfo createdTable = createAndVerifyTable();

    // Get table and verify columns
    TableInfo retrievedTable = tableOperations.getTable(TestUtils.TABLE_FULL_NAME);
    verifyTableInfo(retrievedTable, createdTable);

    // Create multiple tables and verify pagination
    List<TableInfo> createdTables = createMultipleTestingTables(111);
    verifyTablePagination();
    // Sort and verify tables
    verifyTableSorting();
    // Clean up list tables
    cleanUpTables(createdTables);

    // Test delete table functionality
    testDeleteTable();

    // Test managed table retrieval
    testManagedTableRetrieval();

    // Test schema update and deletion scenarios
    testTableAfterSchemaUpdateAndDeletion();
  }

  private TableInfo createAndVerifyTable() throws IOException, ApiException {
    TableInfo tableInfo = createTestingTable(TestUtils.TABLE_NAME, TestUtils.STORAGE_LOCATION);
    assertThat(tableInfo.getName()).isEqualTo(TestUtils.TABLE_NAME);
    assertThat(tableInfo.getCatalogName()).isEqualTo(TestUtils.CATALOG_NAME);
    assertThat(tableInfo.getSchemaName()).isEqualTo(TestUtils.SCHEMA_NAME);
    assertThat(tableInfo.getTableId()).isNotNull();
    return tableInfo;
  }

  private void verifyTableInfo(TableInfo retrievedTable, TableInfo expectedTable) {
    assertThat(retrievedTable).isEqualTo(expectedTable);
    Collection<ColumnInfo> columns = retrievedTable.getColumns();
    assertThat(columns)
        .hasSize(2)
        .extracting(ColumnInfo::getName)
        .as("Table should contain two colums with names '%s' and '%s'", "as_int", "as_string")
        .contains("as_int", "as_string");
  }

  private void verifyTablePagination() throws ApiException {
    Iterable<TableInfo> tables =
        tableOperations.listTables(TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME);
    assertThat(tables).hasSize(100);
  }

  private void verifyTableSorting() throws ApiException {
    Iterable<TableInfo> tables =
        tableOperations.listTables(TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME);
    List<TableInfo> sortedTables = new ArrayList<>();
    tables.forEach(sortedTables::add);
    assertThat(sortedTables).isSortedAccordingTo(Comparator.comparing(TableInfo::getName));
  }

  private void cleanUpTables(List<TableInfo> tables) {
    tables.forEach(
        table -> {
          try {
            tableOperations.deleteTable(
                TestUtils.CATALOG_NAME + "." + TestUtils.SCHEMA_NAME + "." + table.getName());
          } catch (ApiException e) {
            fail("Failed to delete table: " + e.getMessage());
          }
        });
  }

  private void testDeleteTable() throws ApiException {
    tableOperations.deleteTable(TestUtils.TABLE_FULL_NAME);
    assertThatThrownBy(() -> tableOperations.getTable(TestUtils.TABLE_FULL_NAME))
        .isInstanceOf(Exception.class);
  }

  private void testManagedTableRetrieval() throws ApiException, IOException {
    CreateTable createTableRequest =
        new CreateTable()
            .name(TestUtils.TABLE_NAME)
            .catalogName(TestUtils.CATALOG_NAME)
            .schemaName(TestUtils.SCHEMA_NAME)
            .columns(List.of(columnInfo1, columnInfo2))
            .properties(TestUtils.PROPERTIES)
            .comment(TestUtils.COMMENT)
            .tableType(TableType.MANAGED)
            .dataSourceFormat(DataSourceFormat.DELTA);
    TableInfo tableInfo = tableOperations.createTable(createTableRequest);

    TableInfo managedTable = tableOperations.getTable(TestUtils.TABLE_FULL_NAME);
    assertThat(managedTable.getName()).isEqualTo(TestUtils.TABLE_NAME);
    assertThat(managedTable.getCatalogName()).isEqualTo(TestUtils.CATALOG_NAME);
    assertThat(managedTable.getSchemaName()).isEqualTo(TestUtils.SCHEMA_NAME);
    assertThat(managedTable.getStorageLocation())
        .isEqualTo("file:///tmp/tables/" + tableInfo.getTableId());
    assertThat(managedTable.getTableType()).isEqualTo(TableType.MANAGED);
    assertThat(managedTable.getDataSourceFormat()).isEqualTo(DataSourceFormat.DELTA);
    assertThat(managedTable.getCreatedAt()).isNotNull();
    assertThat(managedTable.getTableId()).isNotNull();
  }

  private void testTableAfterSchemaUpdateAndDeletion() throws ApiException {
    TableInfo tableBeforeSchemaUpdate = tableOperations.getTable(TestUtils.TABLE_FULL_NAME);
    schemaOperations.updateSchema(
        TestUtils.SCHEMA_FULL_NAME,
        new UpdateSchema()
            .newName(TestUtils.SCHEMA_NEW_NAME)
            .comment(TestUtils.SCHEMA_NEW_COMMENT));

    TableInfo tableAfterSchemaUpdate =
        tableOperations.getTable(
            TestUtils.CATALOG_NAME + "." + TestUtils.SCHEMA_NEW_NAME + "." + TestUtils.TABLE_NAME);
    assertThat(tableAfterSchemaUpdate.getTableId()).isEqualTo(tableBeforeSchemaUpdate.getTableId());

    assertThatThrownBy(
            () ->
                schemaOperations.deleteSchema(
                    TestUtils.CATALOG_NAME + "." + TestUtils.SCHEMA_NEW_NAME, Optional.of(false)))
        .isInstanceOf(Exception.class);

    String newTableFullName =
        TestUtils.CATALOG_NAME + "." + TestUtils.SCHEMA_NEW_NAME + "." + TestUtils.TABLE_NAME;
    schemaOperations.deleteSchema(
        TestUtils.CATALOG_NAME + "." + TestUtils.SCHEMA_NEW_NAME, Optional.of(true));
    assertThatThrownBy(() -> tableOperations.getTable(newTableFullName))
        .isInstanceOf(Exception.class);
    assertThatThrownBy(
            () ->
                schemaOperations.getSchema(
                    TestUtils.CATALOG_NAME + "." + TestUtils.SCHEMA_NEW_NAME))
        .isInstanceOf(Exception.class);
  }

  protected TableInfo createTestingTable(String tableName, String storageLocation)
      throws IOException, ApiException {
    ColumnInfo columnInfo1 =
        new ColumnInfo()
            .name("as_int")
            .typeText("INTEGER")
            .typeJson("{\"type\": \"integer\"}")
            .typeName(ColumnTypeName.INT)
            .typePrecision(10)
            .typeScale(0)
            .position(0)
            .comment("Integer column")
            .nullable(true);

    ColumnInfo columnInfo2 =
        new ColumnInfo()
            .name("as_string")
            .typeText("VARCHAR(255)")
            .typeJson("{\"type\": \"string\", \"length\": \"255\"}")
            .typeName(ColumnTypeName.STRING)
            .position(1)
            .comment("String column")
            .nullable(true);

    CreateTable createTableRequest =
        new CreateTable()
            .name(tableName)
            .catalogName(TestUtils.CATALOG_NAME)
            .schemaName(TestUtils.SCHEMA_NAME)
            .columns(List.of(columnInfo1, columnInfo2))
            .properties(TestUtils.PROPERTIES)
            .comment(TestUtils.COMMENT)
            .storageLocation(storageLocation)
            .tableType(TableType.EXTERNAL)
            .dataSourceFormat(DataSourceFormat.DELTA);

    return tableOperations.createTable(createTableRequest);
  }

  protected List<TableInfo> createMultipleTestingTables(int numberOfTables)
      throws IOException, ApiException {
    List<TableInfo> createdTables = new ArrayList<>();
    for (int i = numberOfTables; i > 0; i--) {
      String tableName = TestUtils.TABLE_NAME + "_" + i;
      String storageLocation = TestUtils.STORAGE_LOCATION + "/" + tableName;
      createdTables.add(createTestingTable(tableName, storageLocation));
    }
    return createdTables;
  }
}
