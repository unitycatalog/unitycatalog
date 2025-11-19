package io.unitycatalog.server.base.table;

import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.ColumnInfo;
import io.unitycatalog.client.model.ColumnTypeName;
import io.unitycatalog.client.model.CreateCatalog;
import io.unitycatalog.client.model.CreateSchema;
import io.unitycatalog.client.model.CreateTable;
import io.unitycatalog.client.model.DataSourceFormat;
import io.unitycatalog.client.model.SchemaInfo;
import io.unitycatalog.client.model.TableInfo;
import io.unitycatalog.client.model.TableType;
import io.unitycatalog.server.base.BaseCRUDTest;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.schema.SchemaOperations;
import io.unitycatalog.server.utils.TestUtils;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;

/**
 * Abstract base class that provides the test environment setup for table CRUD operations.
 *
 * <p>This class extends {@link BaseCRUDTest} and serves as a foundation for testing table-related
 * operations in Unity Catalog. It's useful for any CRUD test that needs to create test tables.
 */
public abstract class BaseTableCRUDTestEnv extends BaseCRUDTest {

  protected SchemaOperations schemaOperations;
  protected TableOperations tableOperations;
  protected String schemaId;

  protected abstract SchemaOperations createSchemaOperations(ServerConfig serverConfig);

  protected abstract TableOperations createTableOperations(ServerConfig serverConfig);

  protected static final List<ColumnInfo> COLUMNS =
      List.of(
          new ColumnInfo()
              .name("as_int")
              .typeText("INTEGER")
              .typeJson("{\"type\": \"integer\"}")
              .typeName(ColumnTypeName.INT)
              .position(0)
              .comment("Integer column")
              .nullable(true),
          new ColumnInfo()
              .name("as_string")
              .typeText("VARCHAR(255)")
              .typeJson("{\"type\": \"string\", \"length\": \"255\"}")
              .typeName(ColumnTypeName.STRING)
              .position(1)
              .comment("String column")
              .nullable(true));

  @BeforeEach
  @Override
  public void setUp() {
    super.setUp();
    schemaOperations = createSchemaOperations(serverConfig);
    tableOperations = createTableOperations(serverConfig);
    createCommonResources();
  }

  private void createCommonResources() {
    CreateCatalog createCatalog =
        new CreateCatalog().name(TestUtils.CATALOG_NAME).comment(TestUtils.COMMENT);
    try {
      catalogOperations.createCatalog(createCatalog);
      SchemaInfo schemaInfo =
          schemaOperations.createSchema(
              new CreateSchema().name(TestUtils.SCHEMA_NAME).catalogName(TestUtils.CATALOG_NAME));
      schemaId = schemaInfo.getSchemaId();
    } catch (ApiException e) {
      throw new RuntimeException(e);
    }
  }

  protected TableInfo createAndVerifyExternalTable() throws IOException, ApiException {
    TableInfo tableInfo =
        createTestingTable(
            TestUtils.TABLE_NAME,
            TableType.EXTERNAL,
            Optional.of(TestUtils.STORAGE_LOCATION),
            tableOperations);
    assertThat(tableInfo.getName()).isEqualTo(TestUtils.TABLE_NAME);
    assertThat(tableInfo.getCatalogName()).isEqualTo(TestUtils.CATALOG_NAME);
    assertThat(tableInfo.getSchemaName()).isEqualTo(TestUtils.SCHEMA_NAME);
    assertThat(tableInfo.getTableId()).isNotNull();
    assertThat(tableInfo.getTableType()).isEqualTo(TableType.EXTERNAL);
    return tableInfo;
  }

  protected TableInfo createAndVerifyManagedTable() throws ApiException, IOException {
    TableInfo managedTable =
        createTestingTable(
            TestUtils.TABLE_NAME, TableType.MANAGED, Optional.empty(), tableOperations);
    assertThat(managedTable.getName()).isEqualTo(TestUtils.TABLE_NAME);
    assertThat(managedTable.getCatalogName()).isEqualTo(TestUtils.CATALOG_NAME);
    assertThat(managedTable.getSchemaName()).isEqualTo(TestUtils.SCHEMA_NAME);
    assertThat(managedTable.getStorageLocation())
        .isEqualTo("file:///tmp/ucroot/tables/" + managedTable.getTableId());
    assertThat(managedTable.getTableType()).isEqualTo(TableType.MANAGED);
    assertThat(managedTable.getDataSourceFormat()).isEqualTo(DataSourceFormat.DELTA);
    assertThat(managedTable.getCreatedAt()).isNotNull();
    assertThat(managedTable.getTableId()).isNotNull();
    return managedTable;
  }

  public static TableInfo createTestingTable(
      String tableName,
      TableType tableType,
      Optional<String> storageLocation,
      TableOperations tableOperations)
      throws IOException, ApiException {
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
            .dataSourceFormat(DataSourceFormat.DELTA);

    return tableOperations.createTable(createTableRequest);
  }
}
