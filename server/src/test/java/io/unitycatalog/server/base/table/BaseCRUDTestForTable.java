package io.unitycatalog.server.base.table;

import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.*;
import io.unitycatalog.server.base.BaseCRUDTest;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.schema.SchemaOperations;
import io.unitycatalog.server.utils.TestUtils;
import java.io.IOException;
import java.util.List;
import javax.annotation.Nullable;
import org.junit.jupiter.api.BeforeEach;

public abstract class BaseCRUDTestForTable extends BaseCRUDTest {

  protected SchemaOperations schemaOperations;
  protected TableOperations tableOperations;
  protected String schemaId;

  protected abstract SchemaOperations createSchemaOperations(ServerConfig serverConfig);

  protected abstract TableOperations createTableOperations(ServerConfig serverConfig);

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
            TestUtils.TABLE_NAME, TableType.EXTERNAL, TestUtils.STORAGE_LOCATION, tableOperations);
    assertThat(tableInfo.getName()).isEqualTo(TestUtils.TABLE_NAME);
    assertThat(tableInfo.getCatalogName()).isEqualTo(TestUtils.CATALOG_NAME);
    assertThat(tableInfo.getSchemaName()).isEqualTo(TestUtils.SCHEMA_NAME);
    assertThat(tableInfo.getTableId()).isNotNull();
    assertThat(tableInfo.getTableType()).isEqualTo(TableType.EXTERNAL);
    return tableInfo;
  }

  public static TableInfo createTestingTable(
      String tableName,
      TableType tableType,
      @Nullable String storageLocation,
      TableOperations tableOperations)
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
            .tableType(tableType)
            .dataSourceFormat(DataSourceFormat.DELTA);

    return tableOperations.createTable(createTableRequest);
  }
}
