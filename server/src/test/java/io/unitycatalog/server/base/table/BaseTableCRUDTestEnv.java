package io.unitycatalog.server.base.table;

import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.model.ColumnInfo;
import io.unitycatalog.client.model.ColumnTypeName;
import io.unitycatalog.client.model.CreateTable;
import io.unitycatalog.client.model.DataSourceFormat;
import io.unitycatalog.client.model.TableInfo;
import io.unitycatalog.client.model.TableType;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.schema.BaseSchemaCRUDTestEnv;
import io.unitycatalog.server.utils.TestUtils;
import java.nio.file.Files;
import java.util.List;
import java.util.Optional;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;

/**
 * Abstract base class that provides the test environment setup for table CRUD operations.
 *
 * <p>This class extends {@link BaseSchemaCRUDTestEnv} and serves as a foundation for testing
 * table-related operations in Unity Catalog, for any CRUD test that needs to create test tables.
 */
public abstract class BaseTableCRUDTestEnv extends BaseSchemaCRUDTestEnv {

  protected TableOperations tableOperations;

  protected abstract TableOperations createTableOperations(ServerConfig serverConfig);

  protected static final List<ColumnInfo> COLUMNS =
      List.of(
          new ColumnInfo()
              .name("as_int")
              .typeText("INTEGER")
              .typeJson(
                  "{\"name\":\"as_int\",\"type\":\"integer\","
                      + "\"nullable\":true,\"metadata\":{}}")
              .typeName(ColumnTypeName.INT)
              .position(0)
              .comment("Integer column")
              .nullable(true),
          new ColumnInfo()
              .name("as_string")
              .typeText("VARCHAR(255)")
              .typeJson(
                  "{\"name\":\"as_string\",\"type\":\"string\","
                      + "\"nullable\":true,\"metadata\":{}}")
              .typeName(ColumnTypeName.STRING)
              .position(1)
              .comment("String column")
              .nullable(true));

  @BeforeEach
  @Override
  public void setUp() {
    super.setUp();
    tableOperations = createTableOperations(serverConfig);
  }

  @SneakyThrows
  protected TableInfo createAndVerifyExternalTable() {
    TableInfo tableInfo =
        createTestingTable(
            TestUtils.TABLE_NAME,
            TableType.EXTERNAL,
            Optional.of(Files.createTempDirectory(testDirectoryRoot, "table").toString()),
            tableOperations);
    assertThat(tableInfo.getName()).isEqualTo(TestUtils.TABLE_NAME);
    assertThat(tableInfo.getCatalogName()).isEqualTo(TestUtils.CATALOG_NAME);
    assertThat(tableInfo.getSchemaName()).isEqualTo(TestUtils.SCHEMA_NAME);
    assertThat(tableInfo.getTableId()).isNotNull();
    assertThat(tableInfo.getTableType()).isEqualTo(TableType.EXTERNAL);
    return tableInfo;
  }

  protected TableInfo createAndVerifyManagedTable() {
    TableInfo managedTable =
        createTestingTable(
            TestUtils.TABLE_NAME, TableType.MANAGED, Optional.empty(), tableOperations);
    assertThat(managedTable.getName()).isEqualTo(TestUtils.TABLE_NAME);
    assertThat(managedTable.getCatalogName()).isEqualTo(TestUtils.CATALOG_NAME);
    assertThat(managedTable.getSchemaName()).isEqualTo(TestUtils.SCHEMA_NAME);
    assertThat(managedTable.getStorageLocation())
        .isEqualTo(tableStorageRoot + "/__unitystorage/tables/" + managedTable.getTableId());
    assertThat(managedTable.getTableType()).isEqualTo(TableType.MANAGED);
    assertThat(managedTable.getDataSourceFormat()).isEqualTo(DataSourceFormat.DELTA);
    assertThat(managedTable.getCreatedAt()).isNotNull();
    assertThat(managedTable.getTableId()).isNotNull();
    return managedTable;
  }

  @SneakyThrows
  public static TableInfo createTestingTable(
      String tableName,
      TableType tableType,
      Optional<String> storageLocation,
      TableOperations tableOperations) {
    return createTestingTable(
        tableName, tableType, storageLocation, DataSourceFormat.DELTA, tableOperations);
  }

  @SneakyThrows
  public static TableInfo createTestingTable(
      String tableName,
      TableType tableType,
      Optional<String> storageLocation,
      DataSourceFormat dataSourceFormat,
      TableOperations tableOperations) {
    if (tableType == TableType.MANAGED) {
      assert storageLocation.isEmpty();
    } else {
      assert storageLocation.isPresent();
    }

    CreateTable createTableRequest =
        new CreateTable()
            .name(tableName)
            .catalogName(TestUtils.CATALOG_NAME)
            .schemaName(TestUtils.SCHEMA_NAME)
            .columns(COLUMNS)
            .properties(TestUtils.PROPERTIES)
            .comment(TestUtils.COMMENT)
            .storageLocation(storageLocation.orElse(null))
            .tableType(tableType)
            .dataSourceFormat(dataSourceFormat);

    return tableOperations.createTable(createTableRequest);
  }
}
