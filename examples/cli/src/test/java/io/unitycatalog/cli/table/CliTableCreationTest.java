package io.unitycatalog.cli.table;

import static io.unitycatalog.server.utils.TestUtils.CATALOG_NAME;
import static io.unitycatalog.server.utils.TestUtils.COMMENT;
import static io.unitycatalog.server.utils.TestUtils.SCHEMA_NAME;
import static io.unitycatalog.server.utils.TestUtils.TABLE_FULL_NAME;
import static io.unitycatalog.server.utils.TestUtils.TABLE_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.fail;
import static org.assertj.core.api.Assertions.tuple;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.delta.kernel.Snapshot;
import io.delta.kernel.Table;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.types.ByteType;
import io.delta.kernel.types.DoubleType;
import io.delta.kernel.types.FloatType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.ShortType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.unitycatalog.cli.catalog.CliCatalogOperations;
import io.unitycatalog.cli.delta.DeltaKernelReadUtils;
import io.unitycatalog.cli.delta.DeltaKernelUtils;
import io.unitycatalog.cli.schema.CliSchemaOperations;
import io.unitycatalog.cli.utils.CliException;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.TablesApi;
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
import io.unitycatalog.server.utils.TestUtils;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class CliTableCreationTest extends BaseServerTest {

  private CatalogOperations catalogOperations;
  private SchemaOperations schemaOperations;
  private CliTableOperations tableOperations;

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
    String tablePath = testDirectoryRoot.resolve("table").toString();
    createTableAndAssertReadTableSucceeds(TableType.EXTERNAL, Optional.of(tablePath), COLUMNS);
  }

  @Test
  public void testCreateTableLocalDirectoryExistsWithoutDeltaLog()
      throws IOException, ApiException {
    Path dir = testDirectoryRoot.resolve("table");
    Files.createDirectory(dir);
    createTableAndAssertReadTableSucceeds(TableType.EXTERNAL, Optional.of(dir.toString()), COLUMNS);
  }

  @Test
  public void testCreateTableLocalDirectoryExistsWithDeltaLog() throws ApiException, IOException {
    // First create a table with delta log
    Path tableDir = testDirectoryRoot.resolve("table");
    String tablePath = tableDir.toString();
    assertThatCode(
            () ->
                // null tableId/serverUrl/authToken = external table (no UC commit coordination)
                DeltaKernelUtils.createDeltaTable(
                    tableDir.toUri().toString(), COLUMNS, null, null, null, null, null))
        .doesNotThrowAnyException();
    assertThat(tableDir.resolve("_delta_log")).isDirectory();
    createTableAndAssertReadTableSucceeds(TableType.EXTERNAL, Optional.of(tablePath), COLUMNS);
  }

  @Test
  public void testCreateManagedTable() throws IOException, ApiException {
    createTableAndAssertReadTableSucceeds(TableType.MANAGED, Optional.empty(), COLUMNS);

    // Verify CCv2 (catalog-managed) properties are persisted on the server via direct RPC call
    TablesApi tablesApi = new TablesApi(TestUtils.createApiClient(serverConfig));
    Map<String, String> props =
        tablesApi.getTable(TABLE_FULL_NAME, true, true, /* includeBrowse= */ false).getProperties();
    assertThat(props).isNotNull();
    assertThat(props).containsEntry("delta.feature.catalogManaged", "supported");
    assertThat(props).containsEntry("delta.feature.vacuumProtocolCheck", "supported");
    assertThat(props).containsEntry("delta.minReaderVersion", "3");
    assertThat(props).containsEntry("delta.minWriterVersion", "7");
    assertThat(props).containsEntry("delta.lastUpdateVersion", "0");
  }

  @Test
  public void testCreateTableWithDecimalColumns() throws IOException, ApiException {
    Path tableDir = testDirectoryRoot.resolve("table");
    List<ColumnInfo> columns =
        List.of(
            new ColumnInfo()
                .name("price")
                .typeName(ColumnTypeName.DECIMAL)
                .typePrecision(10)
                .typeScale(2),
            new ColumnInfo().name("amount").typeName(ColumnTypeName.DECIMAL));
    tableOperations.createTable(
        new CreateTable()
            .catalogName(CATALOG_NAME)
            .schemaName(SCHEMA_NAME)
            .name(TABLE_NAME)
            .tableType(TableType.EXTERNAL)
            .storageLocation(tableDir.toUri().toString())
            .columns(columns));

    List<ColumnInfo> created = tableOperations.getTable(TABLE_FULL_NAME).getColumns();
    assertThat(created)
        .extracting(
            ColumnInfo::getName,
            ColumnInfo::getTypeText,
            ColumnInfo::getTypePrecision,
            ColumnInfo::getTypeScale)
        .containsExactly(
            tuple("price", "decimal(10,2)", 10, 2), tuple("amount", "decimal(10,0)", 10, 0));
    assertThat(created.get(0).getTypeJson()).contains("\"type\":\"decimal(10,2)\"");

    ObjectMapper mapper = new ObjectMapper();
    JsonNode metaData = null;
    for (String line :
        Files.readAllLines(tableDir.resolve("_delta_log").resolve("00000000000000000000.json"))) {
      JsonNode action = mapper.readTree(line);
      if (action.has("metaData")) {
        metaData = action.get("metaData");
      }
    }
    assertThat(metaData).isNotNull();
    JsonNode fields = mapper.readTree(metaData.get("schemaString").asText()).get("fields");
    assertThat(fields)
        .extracting(field -> field.get("name").asText(), field -> field.get("type").asText())
        .containsExactly(tuple("price", "decimal(10,2)"), tuple("amount", "decimal(10,0)"));
  }

  @Test
  public void testWriteTableAndReadBackSupportedColumnTypes() throws IOException, ApiException {
    Path tableDir = testDirectoryRoot.resolve("table");
    tableOperations.createTable(
        new CreateTable()
            .catalogName(CATALOG_NAME)
            .schemaName(SCHEMA_NAME)
            .name(TABLE_NAME)
            .tableType(TableType.EXTERNAL)
            .storageLocation(tableDir.toUri().toString())
            .columns(
                List.of(
                    new ColumnInfo().name("id").typeName(ColumnTypeName.INT),
                    new ColumnInfo().name("b").typeName(ColumnTypeName.BYTE),
                    new ColumnInfo().name("s").typeName(ColumnTypeName.SHORT),
                    new ColumnInfo().name("l").typeName(ColumnTypeName.LONG),
                    new ColumnInfo().name("f").typeName(ColumnTypeName.FLOAT),
                    new ColumnInfo().name("d").typeName(ColumnTypeName.DOUBLE),
                    new ColumnInfo().name("str").typeName(ColumnTypeName.STRING))));

    tableOperations.writeTable(TABLE_FULL_NAME);

    String storageLocation = tableOperations.getTable(TABLE_FULL_NAME).getStorageLocation();
    Engine engine = DeltaKernelUtils.getEngine(URI.create(storageLocation), null);
    Snapshot snapshot = Table.forPath(engine, storageLocation).getLatestSnapshot(engine);
    StructType schema = snapshot.getSchema();
    assertThat(schema.fields())
        .extracting(StructField::getName, StructField::getDataType)
        .containsExactly(
            tuple("id", IntegerType.INTEGER),
            tuple("b", ByteType.BYTE),
            tuple("s", ShortType.SHORT),
            tuple("l", LongType.LONG),
            tuple("f", FloatType.FLOAT),
            tuple("d", DoubleType.DOUBLE),
            tuple("str", StringType.STRING));
    List<List<String>> rows =
        DeltaKernelReadUtils.readData(
                engine, schema, snapshot.getScanBuilder().withReadSchema(schema).build(), 100)
            .stream()
            .map(
                row ->
                    IntStream.range(0, schema.length())
                        .mapToObj(col -> DeltaKernelReadUtils.getValue(row, col))
                        .toList())
            .toList();

    assertThat(rows)
        .extracting(row -> row.get(0))
        .containsExactlyInAnyOrderElementsOf(
            IntStream.rangeClosed(1, 15).mapToObj(String::valueOf).toList());
    for (List<String> row : rows) {
      assertThat(row).doesNotContainNull();
      assertThat(Byte.parseByte(row.get(1))).isNotNegative();
      assertThat(Short.parseShort(row.get(2))).isNotNegative();
      assertThat(Long.parseLong(row.get(3))).isNotNegative();
      assertThat(Float.parseFloat(row.get(4))).isNotNegative();
      assertThat(Double.parseDouble(row.get(5))).isNotNegative();
      assertThat(row.get(6)).hasSize(10);
    }
  }

  @Test
  public void testCreateManagedTableAndFail() {
    // This fails due to setting a storage location for managed table.
    CliException ex1 =
        assertThrows(
            CliException.class,
            () ->
                tableOperations.createTable(
                    new CreateTable()
                        .catalogName(CATALOG_NAME)
                        .schemaName(SCHEMA_NAME)
                        .name(TABLE_NAME + "_1")
                        .tableType(TableType.MANAGED)
                        .storageLocation("/tmp/some_path")
                        .columns(COLUMNS)));
    assertThat(ex1.getMessage()).contains("Storage location is not allowed for managed tables");
    // This fails due to setting PARQUET. Only DELTA is supported for managed table right now.
    CliException ex2 =
        assertThrows(
            CliException.class,
            () ->
                tableOperations.createTable(
                    new CreateTable()
                        .catalogName(CATALOG_NAME)
                        .schemaName(SCHEMA_NAME)
                        .name(TABLE_NAME + "_2")
                        .tableType(TableType.MANAGED)
                        .dataSourceFormat(DataSourceFormat.PARQUET)
                        .columns(COLUMNS)));
    assertThat(ex2.getMessage())
        .contains("Only Delta tables are supported for managed tables: PARQUET");
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
          .isEqualTo(tableStorageRoot + "/__unitystorage/tables/" + tableInfo.getTableId());
      assertThat(tableInfo.getTableType()).isEqualTo(TableType.MANAGED);
    }
    assertThat(tableOperations.getTable(TABLE_FULL_NAME).getTableId())
        .isEqualTo(tableInfo.getTableId());
    assertThatCode(
            () ->
                DeltaKernelUtils.readDeltaTable(
                    tableOperations.getTable(TABLE_FULL_NAME).getStorageLocation(), null, 100))
        .doesNotThrowAnyException();
    // Table and directory cleanup is handled by @AfterEach via cascade-deleting the catalog.
    // For managed tables, the temp directory is also cleaned up by JUnit @TempDir.
  }
}
