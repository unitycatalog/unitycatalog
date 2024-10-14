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
import io.unitycatalog.client.model.*;
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

public class CliExternalTableCreationTest extends BaseServerTest {

  private CatalogOperations catalogOperations;
  private SchemaOperations schemaOperations;
  private TableOperations tableOperations;

  private static final List<ColumnInfo> columns =
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
    createTableAndAssertReadTableSucceeds(tablePath, columns);
  }

  @Test
  public void testCreateTableLocalDirectoryExistsWithoutDeltaLog()
      throws IOException, ApiException {
    String tablePath = "/tmp/" + UUID.randomUUID();
    Path dir = Paths.get(tablePath);
    Files.createDirectory(dir);
    createTableAndAssertReadTableSucceeds(tablePath, columns);
  }

  @Test
  public void testCreateTableLocalDirectoryExistsWithDeltaLog() throws ApiException, IOException {
    // First create a table with delta log
    String tablePath = "/tmp/" + UUID.randomUUID();
    assertThatCode(
            () ->
                DeltaKernelUtils.createDeltaTable(
                    Paths.get(tablePath).toUri().toString(), columns, null))
        .doesNotThrowAnyException();
    assertThat(Paths.get(tablePath + "/_delta_log")).isDirectory();
    createTableAndAssertReadTableSucceeds(tablePath, columns);
  }

  private void createTableAndAssertReadTableSucceeds(String tablePath, List<ColumnInfo> columns)
      throws IOException, ApiException {
    TableInfo tableInfo =
        tableOperations.createTable(
            new CreateTable()
                .catalogName(CATALOG_NAME)
                .schemaName(SCHEMA_NAME)
                .name(TABLE_NAME)
                .storageLocation(tablePath)
                .columns(columns));
    assertThat(tableInfo).isNotNull();
    assertThat(tableInfo.getTableId()).isNotNull();
    assertThat(tableOperations.getTable(TABLE_FULL_NAME).getTableId())
        .isEqualTo(tableInfo.getTableId());
    assertThatCode(
            () ->
                DeltaKernelUtils.readDeltaTable(
                    tableOperations.getTable(TABLE_FULL_NAME).getStorageLocation(), null, 100))
        .doesNotThrowAnyException();
    assertThatCode(() -> deleteDirectory(Paths.get(tablePath).toFile())).doesNotThrowAnyException();
    assertThatCode(() -> tableOperations.deleteTable(TABLE_FULL_NAME)).doesNotThrowAnyException();
  }
}
