package io.unitycatalog.server.base.table;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.ColumnInfo;
import io.unitycatalog.client.model.TableInfo;
import io.unitycatalog.client.model.TableType;
import io.unitycatalog.client.model.UpdateSchema;
import io.unitycatalog.server.utils.TestUtils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * Abstract base class that provides some test cases for table CRUD operations in Unity Catalog.
 *
 * <p>This class extends {@link BaseTableCRUDTestEnv} and implements test suite that verifies the
 * correct behavior of table operations including creation, retrieval, update, and deletion. It is
 * derived by both CliTableCRUDTest and SdkTableCRUDTest which would exercise CLI and SDK with the
 * test cases defined in this base class.
 */
public abstract class BaseTableCRUDTest extends BaseTableCRUDTestEnv {

  @Test
  public void testTableCRUD() throws IOException, ApiException {
    assertThatThrownBy(() -> tableOperations.getTable(TestUtils.TABLE_FULL_NAME))
        .isInstanceOf(Exception.class);

    // Create and verify a table
    TableInfo createdTable = createAndVerifyExternalTable();

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

    // Test managed table creation and verify
    createAndVerifyManagedTable();

    // Test schema update and deletion scenarios
    testTableAfterSchemaUpdateAndDeletion();
  }

  private void verifyTableInfo(TableInfo retrievedTable, TableInfo expectedTable) {
    assertThat(retrievedTable).isEqualTo(expectedTable);
    Collection<ColumnInfo> columns = retrievedTable.getColumns();
    assertThat(columns)
        .hasSize(2)
        .extracting(ColumnInfo::getName)
        .as("Table should contain two columns with names '%s' and '%s'", "as_int", "as_string")
        .contains("as_int", "as_string");
  }

  private void verifyTablePagination() throws ApiException {
    Iterable<TableInfo> tables =
        tableOperations.listTables(TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, Optional.empty());
    assertThat(tables).hasSize(100);

    // Test list tables with page token
    tables =
        tableOperations.listTables(
            TestUtils.CATALOG_NAME,
            TestUtils.SCHEMA_NAME,
            Optional.of(TestUtils.TABLE_NAME + "_1"));
    assertThat(tables).noneMatch(table -> table.getName().equals(TestUtils.TABLE_NAME + "_1"));
  }

  private void verifyTableSorting() throws ApiException {
    Iterable<TableInfo> tables =
        tableOperations.listTables(TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, Optional.empty());
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

  protected List<TableInfo> createMultipleTestingTables(int numberOfTables)
      throws IOException, ApiException {
    List<TableInfo> createdTables = new ArrayList<>();
    for (int i = numberOfTables; i > 0; i--) {
      String tableName = TestUtils.TABLE_NAME + "_" + i;
      String storageLocation = TestUtils.STORAGE_LOCATION + "/" + tableName;
      createdTables.add(
          createTestingTable(
              tableName, TableType.EXTERNAL, Optional.of(storageLocation), tableOperations));
    }
    return createdTables;
  }
}
