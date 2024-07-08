package io.unitycatalog.server.base.table;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.*;
import io.unitycatalog.server.base.BaseCRUDTest;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.schema.SchemaOperations;
import io.unitycatalog.server.persist.FileUtils;
import io.unitycatalog.server.persist.HibernateUtil;
import io.unitycatalog.server.persist.dao.ColumnInfoDAO;
import io.unitycatalog.server.persist.dao.TableInfoDAO;
import io.unitycatalog.server.utils.TestUtils;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.junit.*;

import java.io.IOException;
import java.util.*;

import static org.junit.Assert.*;

public abstract class BaseTableCRUDTest extends BaseCRUDTest {
    protected SchemaOperations schemaOperations;
    protected TableOperations tableOperations;
    protected static final Map<String, String> PROPERTIES = Map.of("prop1", "value1", "prop2", "value2");
    protected static final String TABLE_NAME = "uc_test_table";
    protected static final String TABLE_FULL_NAME = TestUtils.CATALOG_NAME + "." + TestUtils.SCHEMA_NAME + "." + TABLE_NAME;

    private String schemaId = null;

    @Before
    public void setUp() {
        super.setUp();
        schemaOperations = createSchemaOperations(serverConfig);
        tableOperations = createTableOperations(serverConfig);
        cleanUp();
    }
    protected abstract SchemaOperations createSchemaOperations(ServerConfig serverConfig);
    protected abstract TableOperations createTableOperations(ServerConfig serverConfig);

    protected void cleanUp() {
        try {
            if (tableOperations.getTable(TABLE_FULL_NAME) != null) {
                tableOperations.deleteTable(TABLE_FULL_NAME);
            }
        } catch (Exception e) {
            // Ignore
        }
        try {
            if (schemaOperations.getSchema(TestUtils.SCHEMA_FULL_NAME) != null) {
                schemaOperations.deleteSchema(TestUtils.SCHEMA_FULL_NAME);
            }
        } catch (Exception e) {
            // Ignore
        }
        try {
            if (schemaOperations.getSchema(TestUtils.CATALOG_NEW_NAME + "." + TestUtils.SCHEMA_NEW_NAME) != null) {
                schemaOperations.deleteSchema(TestUtils.CATALOG_NEW_NAME + "." + TestUtils.SCHEMA_NEW_NAME);
            }
        } catch (Exception e) {
            // Ignore
        }
        try {
            if (schemaOperations.getSchema(TestUtils.CATALOG_NEW_NAME + "." + TestUtils.SCHEMA_NAME) != null) {
                schemaOperations.deleteSchema(TestUtils.CATALOG_NEW_NAME + "." + TestUtils.SCHEMA_NAME);
            }
        } catch (Exception e) {
            // Ignore
        }

        super.cleanUp();
    }

    protected void createCommonResources() throws ApiException {
        // Common setup operations such as creating a catalog and schema
        catalogOperations.createCatalog(TestUtils.CATALOG_NAME, "Common catalog for tables");
        SchemaInfo schemaInfo = schemaOperations.createSchema(new CreateSchema().name(TestUtils.SCHEMA_NAME).catalogName(TestUtils.CATALOG_NAME));
        schemaId = schemaInfo.getSchemaId();
    }

    @Test
    public void testTableCRUD() throws IOException, ApiException {

        Assert.assertThrows(Exception.class, () -> tableOperations.getTable(TABLE_FULL_NAME));
        createCommonResources();

        // Create a table
        System.out.println("Testing create table..");
        TableInfo tableInfo = createDefaultTestingTable();
        assertEquals(TABLE_NAME, tableInfo.getName());
        Assert.assertEquals(TestUtils.CATALOG_NAME, tableInfo.getCatalogName());
        Assert.assertEquals(TestUtils.SCHEMA_NAME, tableInfo.getSchemaName());
        assertNotNull(tableInfo.getTableId());

        // Get table
        System.out.println("Testing get table..");
        TableInfo tableInfo2 = tableOperations.getTable(TABLE_FULL_NAME);
        assertEquals(TABLE_NAME, tableInfo2.getName());
        Assert.assertEquals(TestUtils.CATALOG_NAME, tableInfo2.getCatalogName());
        Assert.assertEquals(TestUtils.SCHEMA_NAME, tableInfo2.getSchemaName());
        Assert.assertEquals(FileUtils.convertRelativePathToURI("/tmp/stagingLocation"), tableInfo2.getStorageLocation());
        Assert.assertEquals(TableType.EXTERNAL, tableInfo2.getTableType());
        Assert.assertEquals(DataSourceFormat.DELTA, tableInfo2.getDataSourceFormat());
        assertNotNull(tableInfo2.getCreatedAt());
        assertNotNull(tableInfo2.getTableId());

        Collection<ColumnInfo> columnInfos2 = tableInfo2.getColumns();
        assertEquals(2, columnInfos2.size());
        assertEquals(1, columnInfos2.stream().filter(c -> c.getName().equals("as_int")).count());
        assertEquals(1, columnInfos2.stream().filter(c -> c.getName().equals("as_string")).count());

        // List tables
        System.out.println("Testing list tables..");
        Iterable<TableInfo> tableInfos = tableOperations.listTables(TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME);
        assertTrue(TestUtils.contains(tableInfos, tableInfo2, (table) -> {
            assertNotNull(table.getName());
            return table.getName().equals(TABLE_NAME) && table.getSchemaName().equals(TestUtils.SCHEMA_NAME)
                    && table.getCatalogName().equals(TestUtils.CATALOG_NAME) /*&& table.getComment().equals(COMMENT)*/
                    && table.getColumns().stream().anyMatch(c -> c.getName().equals("as_int"))
                    && table.getColumns().stream().anyMatch(c -> c.getName().equals("as_string"));
        }));

        // Delete table
        System.out.println("Testing delete table..");
        tableOperations.deleteTable(TABLE_FULL_NAME);
        assertThrows(Exception.class, () -> tableOperations.getTable(TABLE_FULL_NAME));

        try (Session session = HibernateUtil.getSessionFactory().openSession()) {
            Transaction tx = session.beginTransaction();

            UUID tableId =  UUID.randomUUID();

            TableInfoDAO managedTableInfo = TableInfoDAO.builder()
                    .name( TABLE_NAME)
                    .schemaId(UUID.fromString(schemaId))
                    .comment(TestUtils.COMMENT)
                    .url("/tmp/managedStagingLocation")
                    .type(TableType.MANAGED.name())
                    .dataSourceFormat(DataSourceFormat.DELTA.name())
                    .id(tableId)
                    .createdAt(new Date())
                    .updatedAt(new Date())
                    .build();


            ColumnInfoDAO columnInfoDAO1 = ColumnInfoDAO.builder()
                    .name("as_int")
                    .typeText("INTEGER")
                    .typeJson("{\"type\": \"integer\"}")
                    .typeName(ColumnTypeName.INT.name())
                    .typePrecision(10)
                    .typeScale(0)
                    .ordinalPosition((short)0)
                    .comment("Integer column")
                    .nullable(true)
                    .tableId(managedTableInfo)
                    .build();

            ColumnInfoDAO columnInfoDAO2 = ColumnInfoDAO.builder()
                    .name("as_string")
                    .typeText("VARCHAR(255)")
                    .typeJson("{\"type\": \"string\", \"length\": \"255\"}")
                    .typeName(ColumnTypeName.STRING.name())
                    .ordinalPosition((short)1)
                    .comment("String column")
                    .nullable(true)
                    .tableId(managedTableInfo)
                    .build();

            managedTableInfo.setColumns(List.of(columnInfoDAO1, columnInfoDAO2));

            session.persist(managedTableInfo);
            session.flush();
            tx.commit();
        } catch (Exception e) {
            fail(e.getMessage());
        }

        System.out.println("Testing get managed table..");
        TableInfo managedTable = tableOperations.getTable(TABLE_FULL_NAME);
        assertEquals(TABLE_NAME, managedTable.getName());
        Assert.assertEquals(TestUtils.CATALOG_NAME, managedTable.getCatalogName());
        Assert.assertEquals(TestUtils.SCHEMA_NAME, managedTable.getSchemaName());
        Assert.assertEquals(FileUtils.convertRelativePathToURI("/tmp/managedStagingLocation"), managedTable.getStorageLocation());
        Assert.assertEquals(TableType.MANAGED, managedTable.getTableType());
        Assert.assertEquals(DataSourceFormat.DELTA, managedTable.getDataSourceFormat());
        assertNotNull(managedTable.getCreatedAt());
        assertNotNull(managedTable.getTableId());

        System.out.println("Testing list managed tables..");
        List<TableInfo> managedTables = tableOperations.listTables(TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME);
        TableInfo managedListTable = managedTables.get(0);
        assertEquals(TABLE_NAME, managedListTable.getName());
        Assert.assertEquals(TestUtils.CATALOG_NAME, managedListTable.getCatalogName());
        Assert.assertEquals(TestUtils.SCHEMA_NAME, managedListTable.getSchemaName());
        Assert.assertEquals(FileUtils.convertRelativePathToURI("/tmp/managedStagingLocation"), managedListTable.getStorageLocation());
        Assert.assertEquals(TableType.MANAGED, managedListTable.getTableType());
        Assert.assertEquals(DataSourceFormat.DELTA, managedListTable.getDataSourceFormat());
        assertNotNull(managedListTable.getCreatedAt());
        assertNotNull(managedListTable.getTableId());

        // Now update the parent schema name
        schemaOperations.updateSchema(TestUtils.SCHEMA_FULL_NAME, new UpdateSchema().newName(TestUtils.SCHEMA_NEW_NAME).comment(TestUtils.SCHEMA_COMMENT));
        // now fetch the table again
        TableInfo managedTableAfterSchemaUpdate = tableOperations.getTable(TestUtils.CATALOG_NAME + "." + TestUtils.SCHEMA_NEW_NAME + "." + TABLE_NAME);
        assertEquals(managedTable.getTableId(), managedTableAfterSchemaUpdate.getTableId());


        // Delete managed table
        String newTableFullName = TestUtils.CATALOG_NAME + "." + TestUtils.SCHEMA_NEW_NAME + "." + TABLE_NAME;
        System.out.println("Testing delete table..");
        tableOperations.deleteTable(newTableFullName);
        assertThrows(Exception.class, () -> tableOperations.getTable(newTableFullName));

    }

    protected TableInfo createDefaultTestingTable() throws IOException, ApiException {
        ColumnInfo columnInfo1 = new ColumnInfo().name("as_int").typeText("INTEGER")
                .typeJson("{\"type\": \"integer\"}")
                .typeName(ColumnTypeName.INT).typePrecision(10).typeScale(0).position(0)
                .comment("Integer column").nullable(true);
        ColumnInfo columnInfo2 = new ColumnInfo().name("as_string").typeText("VARCHAR(255)")
                .typeJson("{\"type\": \"string\", \"length\": \"255\"}")
                .typeName(ColumnTypeName.STRING).position(1)
                .comment("String column").nullable(true);

        CreateTable createTableRequest = new CreateTable()
                .name(TABLE_NAME)
                .catalogName(TestUtils.CATALOG_NAME)
                .schemaName(TestUtils.SCHEMA_NAME)
                .columns(List.of(columnInfo1, columnInfo2))
                .properties(PROPERTIES)
                .comment(TestUtils.COMMENT)
                .storageLocation("/tmp/stagingLocation")
                .tableType(TableType.EXTERNAL)
                .dataSourceFormat(DataSourceFormat.DELTA);

        return tableOperations.createTable(createTableRequest);
    }
}