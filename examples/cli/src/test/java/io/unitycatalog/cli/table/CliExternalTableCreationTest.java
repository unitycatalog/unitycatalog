package io.unitycatalog.cli.table;

import io.unitycatalog.cli.catalog.CliCatalogOperations;
import io.unitycatalog.cli.delta.DeltaKernelUtils;
import io.unitycatalog.cli.schema.CliSchemaOperations;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.*;
import io.unitycatalog.server.base.BaseServerTest;
import io.unitycatalog.server.base.catalog.CatalogOperations;
import io.unitycatalog.server.base.schema.SchemaOperations;
import io.unitycatalog.server.base.table.TableOperations;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.AfterEach;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.List;
import java.util.UUID;

import static io.unitycatalog.server.utils.TestUtils.*;
import static org.junit.Assert.*;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

public class CliExternalTableCreationTest extends BaseServerTest {

    private CatalogOperations catalogOperations;
    private SchemaOperations schemaOperations;
    private TableOperations tableOperations;

    private static final String SCHEMA_FULL_NAME = CATALOG_NAME + "." + SCHEMA_NAME;
    private static final String TABLE_FULL_NAME = CATALOG_NAME + "." + SCHEMA_NAME + "." + TABLE_NAME;

    private static final List<ColumnInfo> columns = List.of(
            new ColumnInfo().name("id").typeName(ColumnTypeName.INT).comment("id"),
            new ColumnInfo().name("name").typeName(ColumnTypeName.STRING).comment("name"));

    @Before
    public void setUp() {
        super.setUp();
        catalogOperations = new CliCatalogOperations(serverConfig);
        schemaOperations = new CliSchemaOperations(serverConfig);
        tableOperations = new CliTableOperations(serverConfig);
        try {
            CreateCatalog createCatalog = new CreateCatalog().name(CATALOG_NAME).comment(COMMENT);
            catalogOperations.createCatalog(createCatalog);
            schemaOperations.createSchema(new CreateSchema().name(SCHEMA_NAME).catalogName(CATALOG_NAME).comment(COMMENT));
        } catch (Exception e) {
            fail("Setup failed: " + e.getMessage());
        }
    }

    @Override
    public void cleanUp() {
        try {
            if (tableOperations.getTable(TABLE_FULL_NAME) != null) {
                tableOperations.deleteTable(TABLE_FULL_NAME);
            }
        } catch (Exception e) {
            // Ignore
        }
        try {
            if (schemaOperations.getSchema(SCHEMA_FULL_NAME) != null) {
                schemaOperations.deleteSchema(SCHEMA_FULL_NAME);
            }
        } catch (Exception e) {
            // Ignore
        }
        try {
            if (catalogOperations.getCatalog(CATALOG_NAME) != null) {
                catalogOperations.deleteCatalog(CATALOG_NAME);
            }
        } catch (Exception e) {
            // Ignore
        }
    }

    @Test
    public void testCreateTableLocalDirectoryDoesNotExist() throws IOException, ApiException {
        String tablePath = "/tmp/" + UUID.randomUUID();
        createTableAndAssertReadTableSucceeds(tablePath, columns);
    }

    @Test
    public void testCreateTableLocalDirectoryExistsWithoutDeltaLog() throws IOException, ApiException {
        String tablePath = "/tmp/" + UUID.randomUUID();
        Path dir = Paths.get(tablePath);
        Files.createDirectory(dir);
        createTableAndAssertReadTableSucceeds(tablePath, columns);
    }

    @Test
    public void testCreateTableLocalDirectoryExistsWithDeltaLog() throws ApiException, IOException {
        // First create a table with delta log
        String tablePath = "/tmp/" + UUID.randomUUID();
        assertDoesNotThrow(() -> DeltaKernelUtils.createDeltaTable(Paths.get(tablePath).toUri().toString(), columns, null));
        assert(Files.exists(Paths.get(tablePath + "/_delta_log")));
        createTableAndAssertReadTableSucceeds(tablePath, columns);
    }

    private void createTableAndAssertReadTableSucceeds(String tablePath, List<ColumnInfo> columns) throws IOException, ApiException {
        TableInfo tableInfo = tableOperations.createTable(new CreateTable().catalogName(CATALOG_NAME).schemaName(SCHEMA_NAME).name(TABLE_NAME).storageLocation(tablePath).columns(columns));
        assertNotNull(tableInfo);
        assertNotNull(tableInfo.getTableId());
        assertEquals(tableInfo.getTableId(), tableOperations.getTable(TABLE_FULL_NAME).getTableId());
        assertDoesNotThrow(() -> DeltaKernelUtils.readDeltaTable(tableOperations.getTable(TABLE_FULL_NAME).getStorageLocation(), null, 100));
        assertDoesNotThrow(()-> deleteDirectory(Paths.get(tablePath)));
        assertDoesNotThrow(() -> tableOperations.deleteTable(TABLE_FULL_NAME));
    }

    public static void deleteDirectory(Path path) throws IOException {
        Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                Files.delete(dir);
                return FileVisitResult.CONTINUE;
            }
        });
    }
}