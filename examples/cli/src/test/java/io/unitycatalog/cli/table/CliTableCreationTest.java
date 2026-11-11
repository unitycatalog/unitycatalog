package io.unitycatalog.cli.table;

import static io.unitycatalog.server.utils.TestUtils.CATALOG_NAME;
import static io.unitycatalog.server.utils.TestUtils.COMMENT;
import static io.unitycatalog.server.utils.TestUtils.SCHEMA_NAME;
import static io.unitycatalog.server.utils.TestUtils.TABLE_FULL_NAME;
import static io.unitycatalog.server.utils.TestUtils.TABLE_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;
import static org.assertj.core.api.Assertions.tuple;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.delta.kernel.types.DecimalType;
import io.delta.kernel.types.StructType;
import io.unitycatalog.cli.catalog.CliCatalogOperations;
import io.unitycatalog.cli.delta.DeltaKernelUtils;
import io.unitycatalog.cli.delta.DeltaKernelWriteUtils;
import io.unitycatalog.cli.schema.CliSchemaOperations;
import io.unitycatalog.cli.utils.CliException;
import io.unitycatalog.cli.utils.CliUtils;
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
import io.unitycatalog.server.base.table.TableOperations;
import io.unitycatalog.server.utils.TestUtils;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

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
  public void testParseDecimalColumns() {
    List<ColumnInfo> columns =
        CliUtils.parseColumns(
            "price DECIMAL(12, 3), amount DECIMAL, tax DECIMAL (10,2), qty DECIMAL(7), id INT,"
                + " code CHAR(10)");
    assertThat(columns)
        .extracting(
            ColumnInfo::getName,
            ColumnInfo::getTypeText,
            ColumnInfo::getTypePrecision,
            ColumnInfo::getTypeScale,
            ColumnInfo::getPosition)
        .containsExactly(
            tuple("price", "decimal(12,3)", 12, 3, 0),
            tuple("amount", "decimal(10,0)", 10, 0, 1),
            tuple("tax", "decimal(10,2)", 10, 2, 2),
            tuple("qty", "decimal(7,0)", 7, 0, 3),
            tuple("id", "int", 0, 0, 4),
            tuple("code", "char", null, null, 5));
    assertThat(columns.get(0).getTypeJson()).contains("\"type\":\"decimal(12,3)\"");
    assertThat(columns.get(1).getTypeJson()).contains("\"type\":\"decimal(10,0)\"");
    assertThat(DeltaKernelUtils.getSchema(columns.subList(0, 4)))
        .isEqualTo(
            new StructType()
                .add("price", new DecimalType(12, 3))
                .add("amount", new DecimalType(10, 0))
                .add("tax", new DecimalType(10, 2))
                .add("qty", new DecimalType(7, 0)));
  }

  @ParameterizedTest
  @CsvSource(
      delimiter = '|',
      value = {
        "price DECIMAL(12,3,4) | DECIMAL takes at most 2 parameters: (12,3,4)",
        "price DECIMAL(12,)    | Invalid DECIMAL parameters: (12,)",
        "price DECIMAL(,)      | Invalid DECIMAL parameters: (,)",
        "price DECIMAL()       | Invalid DECIMAL parameters: ()",
        "price DECIMAL(a,2)    | Invalid DECIMAL parameters: (a,2)",
        "price DECIMAL(5,-1)   | Invalid DECIMAL parameters: (5,-1)",
        "price DECIMAL(0,0)    | DECIMAL precision must be between 1 and 38: 0",
        "price DECIMAL(39,0)   | DECIMAL precision must be between 1 and 38: 39",
        "price DECIMAL(5,6)    | DECIMAL scale must be between 0 and precision 5: 6",
        "price DECIMAL(12,3    | Malformed DECIMAL parameters, expected DECIMAL(p) or DECIMAL(p,s)",
        "price DECIMAL[10,2]   | Malformed DECIMAL parameters, expected DECIMAL(p) or DECIMAL(p,s)",
        "price DECIMAL 10      | Malformed DECIMAL parameters, expected DECIMAL(p) or DECIMAL(p,s)"
      })
  public void testParseInvalidDecimalColumns(String columns, String reason) {
    assertThatThrownBy(() -> CliUtils.parseColumns(columns))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("Invalid column definition")
        .cause()
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(reason);
  }

  @ParameterizedTest
  @CsvSource(
      delimiter = '|',
      value = {"DECIMAL(38,38) | 38 | 38", "DECIMAL(1,1) | 1 | 1", "DECIMAL(38,0) | 38 | 0"})
  public void testParseDecimalBoundaries(String type, int precision, int scale) {
    ColumnInfo column = CliUtils.parseColumns("price " + type).get(0);
    assertThat(column.getTypePrecision()).isEqualTo(precision);
    assertThat(column.getTypeScale()).isEqualTo(scale);
    assertThat(column.getTypeText()).isEqualTo("decimal(" + precision + "," + scale + ")");
    assertThat(DeltaKernelUtils.getDataType(column)).isEqualTo(new DecimalType(precision, scale));
  }

  @Test
  public void testParseNonDecimalColumnsStaysLenient() {
    assertThat(CliUtils.parseColumns("id INT name STRING, "))
        .extracting(ColumnInfo::getName, ColumnInfo::getTypeText)
        .containsExactly(tuple("id", "int"), tuple("name", "string"));
    assertThat(CliUtils.parseColumns("a INT (5), b INT(), c DECIMAL extra"))
        .extracting(
            ColumnInfo::getName,
            ColumnInfo::getTypeText,
            ColumnInfo::getTypePrecision,
            ColumnInfo::getTypeScale)
        .containsExactly(
            tuple("a", "int", 0, 0), tuple("b", "int", 0, 0), tuple("c", "decimal(10,0)", 10, 0));
  }

  @ParameterizedTest
  @CsvSource({"price, 'DECIMAL(10,2)'", "flag, BOOLEAN", "day, DATE"})
  public void testWriteRejectsUnsupportedColumnType(String column, String type) {
    StructType schema =
        DeltaKernelUtils.getSchema(CliUtils.parseColumns("id INT, " + column + " " + type));
    assertThatThrownBy(() -> DeltaKernelWriteUtils.createVectorsFromSchema(schema, 1, 0))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage(
            "table write does not support column "
                + column
                + " of type "
                + schema.get(column).getDataType());
  }

  @ParameterizedTest
  @CsvSource({",", "10,", ",2"})
  public void testDecimalColumnWithoutPrecisionOrScaleFails(Integer precision, Integer scale) {
    ColumnInfo column =
        new ColumnInfo()
            .name("price")
            .typeName(ColumnTypeName.DECIMAL)
            .typePrecision(precision)
            .typeScale(scale);
    assertThatThrownBy(() -> DeltaKernelUtils.getDataType(column))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("DECIMAL column is missing precision or scale: price");
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
