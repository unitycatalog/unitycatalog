package io.unitycatalog.cli.table;

import static io.unitycatalog.server.utils.TestUtils.*;
import static org.apache.commons.io.FileUtils.deleteDirectory;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.fail;

import io.unitycatalog.cli.catalog.CliCatalogOperations;
import io.unitycatalog.cli.delta.DeltaKernelUtils;
import io.unitycatalog.cli.schema.CliSchemaOperations;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.ColumnInfo;
import io.unitycatalog.client.model.ColumnTypeName;
import io.unitycatalog.client.model.CreateCatalog;
import io.unitycatalog.client.model.CreateSchema;
import io.unitycatalog.client.model.CreateTable;
import io.unitycatalog.client.model.DataSourceFormat;
import io.unitycatalog.client.model.TableInfo;
import io.unitycatalog.client.model.TableType;
import io.unitycatalog.server.base.BaseServerTest;
import io.unitycatalog.server.base.catalog.CatalogOperations;
import io.unitycatalog.server.base.schema.SchemaOperations;
import io.unitycatalog.server.base.table.TableOperations;
import java.io.IOException;
import java.nio.file.*;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class CliTableCreationTest extends BaseServerTest {

  private CatalogOperations catalogOperations;
  private SchemaOperations schemaOperations;
  private TableOperations tableOperations;

  private static final List<ColumnInfo> COLUMNS =
      List.of(
          new ColumnInfo().name("id").typeName(ColumnTypeName.INT).comment("id"),
          new ColumnInfo().name("name").typeName(ColumnTypeName.STRING).comment("name"));

  @BeforeEach
  @Override
  public void setUp() {
    super.setUp();
    catalogOperations = new CliCatalogOperations(serverConfig);
    schemaOperations = new CliSchemaOperations(serverConfig);
    tableOperations = new CliTableOperations(serverConfig);
    try {
      CreateCatalog createCatalog = new CreateCatalog().name(CATALOG_NAME).comment(COMMENT);
      catalogOperations.createCatalog(createCatalog);
      schemaOperations.createSchema(
          new CreateSchema().name(SCHEMA_NAME).catalogName(CATALOG_NAME).comment(COMMENT));
    } catch (Exception e) {
      fail("Setup failed: " + e.getMessage());
    }
  }

  @AfterEach
  public void cleanUp() {
    try {
      catalogOperations.deleteCatalog(CATALOG_NAME, Optional.of(true));
    } catch (Exception e) {
      // Ignore
    }
  }

  @Test
  public void testCreateTableLocalDirectoryDoesNotExist() throws IOException, ApiException {
    String tablePath = "/tmp/" + UUID.randomUUID();
    createTableAndAssertReadTableSucceeds(TableType.EXTERNAL, Optional.of(tablePath), COLUMNS);
  }

  @Test
  public void testCreateTableLocalDirectoryExistsWithoutDeltaLog()
      throws IOException, ApiException {
    String tablePath = "/tmp/" + UUID.randomUUID();
    Path dir = Paths.get(tablePath);
    Files.createDirectory(dir);
    createTableAndAssertReadTableSucceeds(TableType.EXTERNAL, Optional.of(tablePath), COLUMNS);
  }

  @Test
  public void testCreateTableLocalDirectoryExistsWithDeltaLog() throws ApiException, IOException {
    // First create a table with delta log
    String tablePath = "/tmp/" + UUID.randomUUID();
    assertThatCode(
            () ->
                DeltaKernelUtils.createDeltaTable(
                    Paths.get(tablePath).toUri().toString(), COLUMNS, null))
        .doesNotThrowAnyException();
    assertThat(Paths.get(tablePath + "/_delta_log")).isDirectory();
    createTableAndAssertReadTableSucceeds(TableType.EXTERNAL, Optional.of(tablePath), COLUMNS);
  }

  @Test
  public void testCreateManagedTable() throws IOException, ApiException {
    createTableAndAssertReadTableSucceeds(TableType.MANAGED, Optional.empty(), COLUMNS);
  }

  @Test
  public void testCreateManagedTableAndFail() throws IOException, ApiException {
    // This fails due to setting a storage location for managed table.
    assertThat(
            tableOperations.createTable(
                new CreateTable()
                    .catalogName(CATALOG_NAME)
                    .schemaName(SCHEMA_NAME)
                    .name(TABLE_NAME + "_1")
                    .tableType(TableType.MANAGED)
                    .storageLocation("/tmp/some_path")
                    .columns(COLUMNS)))
        .isNull();
    // This fails due to setting PARQUET. Only DELTA is supported for managed table right now.
    assertThat(
            tableOperations.createTable(
                new CreateTable()
                    .catalogName(CATALOG_NAME)
                    .schemaName(SCHEMA_NAME)
                    .name(TABLE_NAME + "_2")
                    .tableType(TableType.MANAGED)
                    .dataSourceFormat(DataSourceFormat.PARQUET)
                    .columns(COLUMNS)))
        .isNull();
  }

  private void createTableAndAssertReadTableSucceeds(
      TableType tableType, Optional<String> tablePath, List<ColumnInfo> columns)
      throws IOException, ApiException {
    TableInfo tableInfo =
        tableOperations.createTable(
            new CreateTable()
                .catalogName(CATALOG_NAME)
                .schemaName(SCHEMA_NAME)
                .name(TABLE_NAME)
                .tableType(tableType)
                .storageLocation(tablePath.orElse(null))
                .columns(columns));
    assertThat(tableInfo).isNotNull();
    assertThat(tableInfo.getTableId()).isNotNull();
    if (tableType == TableType.EXTERNAL) {
      assertThat(tableInfo.getStorageLocation()).isEqualTo("file://" + tablePath.get());
      assertThat(tableInfo.getTableType()).isEqualTo(TableType.EXTERNAL);
    } else {
      assert tableType == TableType.MANAGED;
      assertThat(tableInfo.getStorageLocation())
          .isEqualTo(tableStorageRoot + "/tables/" + tableInfo.getTableId());
      assertThat(tableInfo.getTableType()).isEqualTo(TableType.MANAGED);
    }
    assertThat(tableOperations.getTable(TABLE_FULL_NAME).getTableId())
        .isEqualTo(tableInfo.getTableId());
    assertThatCode(
            () ->
                DeltaKernelUtils.readDeltaTable(
                    tableOperations.getTable(TABLE_FULL_NAME).getStorageLocation(), null, 100))
        .doesNotThrowAnyException();
    assertThatCode(() -> tableOperations.deleteTable(TABLE_FULL_NAME)).doesNotThrowAnyException();
    // Managed table deletion does not delete the directory yet. So the test would delete the
    // directory anyway
    assertThatCode(() -> deleteDirectory(Paths.get(tableInfo.getStorageLocation()).toFile()))
        .doesNotThrowAnyException();
  }
}
