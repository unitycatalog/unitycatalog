package io.unitycatalog.server.base.table;

import static org.junit.Assert.*;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.*;
import io.unitycatalog.server.base.BaseCRUDTest;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.schema.SchemaOperations;
import io.unitycatalog.server.persist.dao.ColumnInfoDAO;
import io.unitycatalog.server.persist.dao.TableInfoDAO;
import io.unitycatalog.server.persist.utils.FileUtils;
import io.unitycatalog.server.persist.utils.HibernateUtils;
import io.unitycatalog.server.utils.TestUtils;
import java.io.IOException;
import java.util.*;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.junit.*;

public abstract class BaseTableCRUDTest extends BaseCRUDTest {
  protected SchemaOperations schemaOperations;
  protected TableOperations tableOperations;
  private String schemaId = null;

  protected abstract SchemaOperations createSchemaOperations(ServerConfig serverConfig);

  protected abstract TableOperations createTableOperations(ServerConfig serverConfig);

  @Before
  @Override
  public void setUp() {
    super.setUp();
    schemaOperations = createSchemaOperations(serverConfig);
    tableOperations = createTableOperations(serverConfig);
  }

  protected void createCommonResources() throws ApiException {
    // Common setup operations such as creating a catalog and schema
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
    Assert.assertThrows(Exception.class, () -> tableOperations.getTable(TestUtils.TABLE_FULL_NAME));
    createCommonResources();

    // Create a table
    System.out.println("Testing create table..");
    TableInfo tableInfo = createDefaultTestingTable();
    assertEquals(TestUtils.TABLE_NAME, tableInfo.getName());
    Assert.assertEquals(TestUtils.CATALOG_NAME, tableInfo.getCatalogName());
    Assert.assertEquals(TestUtils.SCHEMA_NAME, tableInfo.getSchemaName());
    assertNotNull(tableInfo.getTableId());

    // Get table
    System.out.println("Testing get table..");
    TableInfo tableInfo2 = tableOperations.getTable(TestUtils.TABLE_FULL_NAME);
    assertEquals(tableInfo, tableInfo2);

    Collection<ColumnInfo> columnInfos2 = tableInfo2.getColumns();
    assertEquals(2, columnInfos2.size());
    assertEquals(1, columnInfos2.stream().filter(c -> c.getName().equals("as_int")).count());
    assertEquals(1, columnInfos2.stream().filter(c -> c.getName().equals("as_string")).count());

    // List tables
    System.out.println("Testing list tables..");
    Iterable<TableInfo> tableInfos =
        tableOperations.listTables(TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME);
    assertTrue(TestUtils.contains(tableInfos, tableInfo2, table -> table.equals(tableInfo2)));

    // Delete table
    System.out.println("Testing delete table..");
    tableOperations.deleteTable(TestUtils.TABLE_FULL_NAME);
    assertThrows(Exception.class, () -> tableOperations.getTable(TestUtils.TABLE_FULL_NAME));

    try (Session session = HibernateUtils.getSessionFactory().openSession()) {
      Transaction tx = session.beginTransaction();

      UUID tableId = UUID.randomUUID();

      TableInfoDAO managedTableInfo =
          TableInfoDAO.builder()
              .name(TestUtils.TABLE_NAME)
              .schemaId(UUID.fromString(schemaId))
              .comment(TestUtils.COMMENT)
              .url("/tmp/managedStagingLocation")
              .type(TableType.MANAGED.name())
              .dataSourceFormat(DataSourceFormat.DELTA.name())
              .id(tableId)
              .createdAt(new Date())
              .updatedAt(new Date())
              .build();

      ColumnInfoDAO columnInfoDAO1 =
          ColumnInfoDAO.builder()
              .name("as_int")
              .typeText("INTEGER")
              .typeJson("{\"type\": \"integer\"}")
              .typeName(ColumnTypeName.INT.name())
              .typePrecision(10)
              .typeScale(0)
              .ordinalPosition((short) 0)
              .comment("Integer column")
              .nullable(true)
              .table(managedTableInfo)
              .build();

      ColumnInfoDAO columnInfoDAO2 =
          ColumnInfoDAO.builder()
              .name("as_string")
              .typeText("VARCHAR(255)")
              .typeJson("{\"type\": \"string\", \"length\": \"255\"}")
              .typeName(ColumnTypeName.STRING.name())
              .ordinalPosition((short) 1)
              .comment("String column")
              .nullable(true)
              .table(managedTableInfo)
              .build();

      managedTableInfo.setColumns(List.of(columnInfoDAO1, columnInfoDAO2));

      session.persist(managedTableInfo);
      session.flush();
      tx.commit();
    } catch (Exception e) {
      fail(e.getMessage());
    }

    System.out.println("Testing get managed table..");
    TableInfo managedTable = tableOperations.getTable(TestUtils.TABLE_FULL_NAME);
    assertEquals(TestUtils.TABLE_NAME, managedTable.getName());
    Assert.assertEquals(TestUtils.CATALOG_NAME, managedTable.getCatalogName());
    Assert.assertEquals(TestUtils.SCHEMA_NAME, managedTable.getSchemaName());
    Assert.assertEquals(
        FileUtils.convertRelativePathToURI("/tmp/managedStagingLocation"),
        managedTable.getStorageLocation());
    Assert.assertEquals(TableType.MANAGED, managedTable.getTableType());
    Assert.assertEquals(DataSourceFormat.DELTA, managedTable.getDataSourceFormat());
    assertNotNull(managedTable.getCreatedAt());
    assertNotNull(managedTable.getTableId());

    System.out.println("Testing list managed tables..");
    List<TableInfo> managedTables =
        tableOperations.listTables(TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME);
    TableInfo managedListTable = managedTables.get(0);
    assertEquals(TestUtils.TABLE_NAME, managedListTable.getName());
    Assert.assertEquals(TestUtils.CATALOG_NAME, managedListTable.getCatalogName());
    Assert.assertEquals(TestUtils.SCHEMA_NAME, managedListTable.getSchemaName());
    Assert.assertEquals(
        FileUtils.convertRelativePathToURI("/tmp/managedStagingLocation"),
        managedListTable.getStorageLocation());
    Assert.assertEquals(TableType.MANAGED, managedListTable.getTableType());
    Assert.assertEquals(DataSourceFormat.DELTA, managedListTable.getDataSourceFormat());
    assertNotNull(managedListTable.getCreatedAt());
    assertNotNull(managedListTable.getTableId());

    // Now update the parent schema name
    schemaOperations.updateSchema(
        TestUtils.SCHEMA_FULL_NAME,
        new UpdateSchema().newName(TestUtils.SCHEMA_NEW_NAME).comment(TestUtils.SCHEMA_COMMENT));
    // now fetch the table again
    TableInfo managedTableAfterSchemaUpdate =
        tableOperations.getTable(
            TestUtils.CATALOG_NAME + "." + TestUtils.SCHEMA_NEW_NAME + "." + TestUtils.TABLE_NAME);
    assertEquals(managedTable.getTableId(), managedTableAfterSchemaUpdate.getTableId());

    // test delete parent schema when table exists
    assertThrows(
        Exception.class,
        () ->
            schemaOperations.deleteSchema(
                TestUtils.CATALOG_NAME + "." + TestUtils.SCHEMA_NEW_NAME, Optional.of(false)));

    // test force delete parent schema when table exists
    String newTableFullName =
        TestUtils.CATALOG_NAME + "." + TestUtils.SCHEMA_NEW_NAME + "." + TestUtils.TABLE_NAME;
    schemaOperations.deleteSchema(
        TestUtils.CATALOG_NAME + "." + TestUtils.SCHEMA_NEW_NAME, Optional.of(true));
    assertThrows(Exception.class, () -> tableOperations.getTable(newTableFullName));
    assertThrows(
        Exception.class,
        () -> schemaOperations.getSchema(TestUtils.CATALOG_NAME + "." + TestUtils.SCHEMA_NEW_NAME));
  }

  protected TableInfo createDefaultTestingTable() throws IOException, ApiException {
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
            .name(TestUtils.TABLE_NAME)
            .catalogName(TestUtils.CATALOG_NAME)
            .schemaName(TestUtils.SCHEMA_NAME)
            .columns(List.of(columnInfo1, columnInfo2))
            .properties(TestUtils.PROPERTIES)
            .comment(TestUtils.COMMENT)
            .storageLocation("/tmp/stagingLocation")
            .tableType(TableType.EXTERNAL)
            .dataSourceFormat(DataSourceFormat.DELTA);

    return tableOperations.createTable(createTableRequest);
  }
}
