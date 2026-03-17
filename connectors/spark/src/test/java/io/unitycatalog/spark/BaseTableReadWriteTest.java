package io.unitycatalog.spark;

import static io.unitycatalog.server.utils.TestUtils.CATALOG_NAME;
import static io.unitycatalog.server.utils.TestUtils.SCHEMA_NAME;
import static io.unitycatalog.server.utils.TestUtils.createApiClient;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.ColumnInfo;
import io.unitycatalog.client.model.ColumnTypeName;
import io.unitycatalog.client.model.TableInfo;
import io.unitycatalog.server.base.table.TableOperations;
import io.unitycatalog.server.sdk.tables.SdkTableOperations;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.With;
import lombok.experimental.Accessors;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public abstract class BaseTableReadWriteTest extends BaseSparkIntegrationTest {

  protected enum TableDdlMode {
    CREATE("CREATE", false),
    REPLACE_EXISTING("REPLACE", true),
    CREATE_OR_REPLACE_EXISTING("CREATE OR REPLACE", true),
    CREATE_OR_REPLACE_MISSING("CREATE OR REPLACE", false);

    private final String sql;
    private final boolean withExistingTable;

    TableDdlMode(String sql, boolean withExistingTable) {
      this.sql = sql;
      this.withExistingTable = withExistingTable;
    }

    public String sql() {
      return sql;
    }

    public boolean withExistingTable() {
      return withExistingTable;
    }
  }

  protected static final String TEST_TABLE = "test_table";
  protected TableOperations tableOperations;

  @BeforeEach
  @Override
  public void setUp() {
    super.setUp();
    tableOperations = new SdkTableOperations(createApiClient(serverConfig));
  }

  protected static final String ANOTHER_TEST_TABLE = "test_table_another";
  protected static final String TBLPROPERTIES_CATALOG_OWNED_CLAUSE =
      String.format(
          "TBLPROPERTIES ('%s'='%s')",
          UCTableProperties.DELTA_CATALOG_MANAGED_KEY_NEW,
          UCTableProperties.DELTA_CATALOG_MANAGED_VALUE);

  /**
   * This class is used for control various options for table creation during test. The tableFormat
   * is controlled at the testsuite level by tableFormat().
   */
  @NoArgsConstructor
  @AllArgsConstructor
  @Accessors(chain = true)
  @Getter
  @Setter
  protected class TableSetupOptions {
    private String catalogName;
    private String schemaName = SCHEMA_NAME;
    private String tableName;
    private String cloudScheme = managedStorageCloudScheme();
    private List<Pair<String, String>> columns =
        List.of(Pair.of("i", "INT"), Pair.of("s", "STRING"));
    private List<String> partitionColumns = List.of();
    @With private Optional<Pair<Integer, String>> asSelect = Optional.empty();
    private Optional<String> comment = Optional.empty();
    private Optional<String> expectedTableId = Optional.empty();
    private TableDdlMode ddlMode = TableDdlMode.CREATE;

    public TableSetupOptions setCatalogName(String name) {
      catalogName = quoteEntityName(name);
      return this;
    }

    public TableSetupOptions setSchemaName(String name) {
      schemaName = quoteEntityName(name);
      return this;
    }

    public TableSetupOptions setTableName(String name) {
      tableName = quoteEntityName(name);
      return this;
    }

    public TableSetupOptions setPartitionColumn(String column) {
      assert columns.stream().map(Pair::getLeft).anyMatch(column::equals);
      partitionColumns = List.of(column);
      return this;
    }

    public TableSetupOptions setAsSelect(int i, String s) {
      asSelect = Optional.of(Pair.of(i, s));
      return this;
    }

    public TableSetupOptions setComment(String c) {
      comment = Optional.of(c);
      return this;
    }

    public String partitionClause() {
      if (partitionColumns.isEmpty()) {
        return "";
      } else {
        return partitionColumns.stream().collect(Collectors.joining(", ", "PARTITIONED BY (", ")"));
      }
    }

    public String columnsClause() {
      if (asSelect.isEmpty()) {
        return columns.stream()
            .map(p -> p.getLeft() + " " + p.getRight())
            .collect(Collectors.joining(", ", "(", ")"));
      } else {
        // CTAS can't specify columns
        return "";
      }
    }

    public String asSelectClause() {
      return asSelect
          .map(x -> String.format("AS SELECT %d AS i, '%s' AS s", x.getLeft(), x.getRight()))
          .orElse("");
    }

    public String commentClause() {
      return comment.map(c -> String.format("COMMENT '%s'", c)).orElse("");
    }

    public String ddlCommand() {
      return ddlMode.sql();
    }

    public String createManagedTableSql() {
      return String.format(
          "%s TABLE %s.%s.%s %s USING %s %s %s %s %s",
          ddlCommand(),
          catalogName,
          schemaName,
          tableName,
          columnsClause(),
          tableFormat(),
          partitionClause(),
          TBLPROPERTIES_CATALOG_OWNED_CLAUSE,
          commentClause(),
          asSelectClause());
    }

    public String createExternalTableSql(String location) {
      return String.format(
          "%s TABLE %s.%s.%s%s USING %s %s %s LOCATION '%s' %s",
          ddlCommand(),
          catalogName,
          schemaName,
          tableName,
          columnsClause(),
          tableFormat(),
          partitionClause(),
          commentClause(),
          location,
          asSelectClause());
    }

    public String createDeltaPathTableSql(String location) {
      assert testingDelta();
      return String.format(
          "%s TABLE %s.`%s`%s USING %s %s %s %s",
          ddlCommand(),
          tableFormat(),
          location,
          columnsClause(),
          tableFormat(),
          partitionClause(),
          commentClause(),
          asSelectClause());
    }

    public String fullTableName() {
      return String.join(".", catalogName, schemaName, tableName);
    }
  }

  /**
   * Set up a table and returns the table full name. This function is used by tests that aren't
   * cloud aware. It simply uses the same cloud as configured for managed storage. The table format
   * is controlled by tableFormat().
   */
  protected final String setupTable(String catalogName, String tableName) {
    return setupTable(new TableSetupOptions().setCatalogName(catalogName).setTableName(tableName));
  }

  /**
   * Set up a table with storage on emulated cloud and returns the table full name. The table format
   * is controlled by tableFormat().
   */
  protected final String setupTable(String cloudScheme, String catalogName, String tableName) {
    return setupTable(
        new TableSetupOptions()
            .setCloudScheme(cloudScheme)
            .setCatalogName(catalogName)
            .setTableName(tableName));
  }

  /**
   * Set up a table with storage on emulated cloud and returns the table full name. The table format
   * is controlled by tableFormat(). Subclasses need to override this function accordingly.
   */
  protected abstract String setupTable(TableSetupOptions options);

  /**
   * The table format to be tested. Subclasses override this function to test different table
   * formats.
   */
  protected abstract String tableFormat();

  protected final boolean testingDelta() {
    return tableFormat().equalsIgnoreCase("DELTA");
  }

  protected final boolean canUpdateColumnsToUC() {
    // DELTA tables can't update columns to UC yet.
    return !tableFormat().equalsIgnoreCase("DELTA") || DeltaVersionUtils.isDeltaAtLeast("4.2.0");
  }

  protected List<TableDdlMode> supportedTableDdlModes() {
    return List.of(TableDdlMode.CREATE);
  }

  protected List<String> supportedCatalogNames() {
    return List.of(SPARK_CATALOG, CATALOG_NAME);
  }

  protected List<String> sessionCatalogNames() {
    return List.of(SPARK_CATALOG, CATALOG_NAME);
  }

  protected void prepareExistingTableForDdl(TableSetupOptions options) {
    throw new UnsupportedOperationException("Existing-table DDL setup is not supported");
  }

  protected void validateCreatedTable(String fullTableName, TableSetupOptions options)
      throws ApiException {}

  protected String qualifiedTableName(String catalogName, String tableName) {
    return String.join(".", catalogName, SCHEMA_NAME, tableName);
  }

  /**
   * Returns the expected failure messages for a table setup attempt.
   *
   * @return {@code null} if success is expected, an empty list to expect any failure without
   *     checking the message, or a non-empty list to expect a failure whose message contains at
   *     least one of the strings.
   */
  protected List<String> expectedCreateFailureMessages(TableSetupOptions options) {
    // Non-Delta tables only support plain CREATE TABLE.
    if (!testingDelta()
        && (options.getDdlMode() != TableDdlMode.CREATE || options.getAsSelect().isPresent())) {
      return List.of();
    }
    return null;
  }

  protected void initializeSessionForTableTests() {}

  /**
   * This test creates table in different ways:
   *
   * <ul>
   *   <li>in both SPARK_CATALOG and CATALOG_NAME
   *   <li>with and without partition columns
   *   <li>with and without CTAS
   *   <li>using UPDATE TABLE and CREATE TABLE
   * </ul>
   *
   * For all the 16 (2^4) tables it does a simple read and write test to make sure they all work.
   */
  @Test
  public void testTableCreateReadWrite() throws ApiException {
    // Test both `spark_catalog` and other catalog names.
    session = createSparkSessionWithCatalogs(sessionCatalogNames().toArray(new String[0]));
    initializeSessionForTableTests();

    int tableNameCounter = 0;
    for (String catalogName : supportedCatalogNames()) {
      for (boolean withPartitionColumns : List.of(true, false)) {
        for (boolean withCtas : List.of(true, false)) {
          for (TableDdlMode ddlMode : supportedTableDdlModes()) {
            String tableName = TEST_TABLE + tableNameCounter;
            tableNameCounter++;
            TableSetupOptions options =
                new TableSetupOptions()
                    .setCatalogName(catalogName)
                    .setTableName(tableName)
                    .setDdlMode(ddlMode);
            if (withPartitionColumns) {
              options.setPartitionColumn("s");
            }
            if (withCtas) {
              options.setAsSelect(1, "a");
            }
            List<String> expectedFailureMessages = expectedCreateFailureMessages(options);
            if (ddlMode.withExistingTable()) {
              prepareExistingTableForDdl(options);
              options.setExpectedTableId(
                  Optional.of(
                      tableOperations
                          .getTable(qualifiedTableName(catalogName, tableName))
                          .getTableId()));
            }

            if (expectedFailureMessages != null) {
              if (expectedFailureMessages.isEmpty()) {
                assertThatThrownBy(() -> setupTable(options));
              } else {
                List<String> expected = expectedFailureMessages;
                assertThatThrownBy(() -> setupTable(options))
                    .satisfies(
                        t ->
                            assertThat(t.getMessage())
                                .containsAnyOf(expected.toArray(new CharSequence[0])));
              }
              continue;
            }

            String t1 = setupTable(options);
            if (withCtas) {
              testTableReadWriteCreatedAsSelect(t1, Pair.of(1, "a"));
            } else {
              testTableReadWrite(t1);
            }
            validateCreatedTable(t1, options);
          }
        }
      }
    }
  }

  /**
   * This function checks that the first column of each row in {@code rows} matches {@code expected}
   * in both length and value.
   *
   * @param rows Rows to be checked
   * @param expected The expected integer values of first column
   */
  protected void validateRows(List<Row> rows, Integer... expected) {
    assertThat(rows).hasSize(expected.length);
    for (int i = 0; i < expected.length; i++) {
      assertThat(rows.get(i).getInt(0)).isEqualTo(expected[i]);
    }
  }

  /**
   * This function checks that the first and second column of each row in {@code rows} matches
   * {@code expected} in both length and value.
   *
   * @param rows Rows to be checked
   * @param expected The expected integer and string values of first two columns
   */
  protected void validateRows(List<Row> rows, Pair<Integer, String>... expected) {
    assertThat(rows).hasSize(expected.length);
    for (int i = 0; i < expected.length; i++) {
      Row row = rows.get(i);
      assertThat(row.getInt(0)).isEqualTo(expected[i].getLeft());
      assertThat(row.getString(1)).isEqualTo(expected[i].getRight());
    }
  }

  protected void validateTableEmpty(String fullTableName) {
    assertThat(session.table(fullTableName).collectAsList()).isEmpty();
  }

  protected void validateTableSchema(StructType tableSchema, Pair<String, DataType>... expected) {
    assertThat(tableSchema.size()).isEqualTo(expected.length);
    for (int i = 0; i < expected.length; i++) {
      assertThat(tableSchema.apply(i).name()).isEqualTo(expected[i].getLeft());
      assertThat(tableSchema.apply(i).dataType()).isEqualTo(expected[i].getRight());
    }
  }

  @Test
  @EnabledIf("testingDelta")
  public void testTimeTravelDeltaTable() {
    session = createSparkSessionWithCatalogs(SPARK_CATALOG);

    String t1 = setupTable(SPARK_CATALOG, TEST_TABLE);
    sql("INSERT INTO %s SELECT 1, 'a'", t1);

    String timestamp = Instant.now().toString();

    sql("INSERT INTO %s SELECT 2, 'b'", t1);

    // Time-travel to before the last insert, we should only see the first inserted row.
    validateRows(sql("SELECT * FROM %s VERSION AS OF 1", t1), 1);
    validateRows(sql("SELECT * FROM %s TIMESTAMP AS OF '%s'", t1, timestamp), 1);
    validateRows(session.read().option("versionAsOf", 1).table(t1).collectAsList(), 1);
    validateRows(session.read().option("timestampAsOf", timestamp).table(t1).collectAsList(), 1);
  }

  @ParameterizedTest
  @MethodSource("cloudParameters")
  public void testTableOperations(
      String scheme, boolean renewCredEnabled, boolean credScopedFsEnabled) {
    session =
        createSparkSessionWithCatalogs(renewCredEnabled, credScopedFsEnabled, SPARK_CATALOG, CATALOG_NAME);

    // t1 has (1, 'a')
    String t1 = setupTable(scheme, SPARK_CATALOG, TEST_TABLE);
    testTableReadWrite(t1);

    // t2 has (2, 'a')
    String t2 = setupTable(scheme, CATALOG_NAME, ANOTHER_TEST_TABLE);
    sql("INSERT INTO %s SELECT 2, 'a'", t2);
    validateRows(sql("SELECT * FROM %s", t2), Pair.of(2, "a"));

    if (testingDelta()) {
      // UPDATE, MERGE INTO and DELETE are only supported in Delta tables.

      // Test UPDATE. The table t2 will have (2, 'b')
      sql("UPDATE %s SET s = 'b' WHERE i = 2", t2);
      validateRows(sql("SELECT * FROM %s", t2), Pair.of(2, "b"));

      // Test MERGE. The table t1 will have (1, 'a') and (2, 'b')
      sql("MERGE INTO %s USING %s ON %s.i = %s.i WHEN NOT MATCHED THEN INSERT *", t1, t2, t1, t2);
      validateRows(sql("SELECT * FROM %s ORDER BY i", t1), Pair.of(1, "a"), Pair.of(2, "b"));

      // Test DELETE. The table t1 will have (1, 'a')
      sql("DELETE FROM %s WHERE i = 2", t1);
      validateRows(sql("SELECT * FROM %s", t1), Pair.of(1, "a"));
    }

    // Test SHOW TABLES
    List<Row> tables1 = sql("SHOW TABLES in %s.%s", SPARK_CATALOG, SCHEMA_NAME);
    assertThat(tables1).hasSize(1);
    assertThat(tables1.get(0).getString(0)).isEqualTo(SCHEMA_NAME);
    assertThat(tables1.get(0).getString(1)).isEqualTo(TEST_TABLE);
    List<Row> tables2 = sql("SHOW TABLES in %s.%s", CATALOG_NAME, SCHEMA_NAME);
    assertThat(tables2).hasSize(1);
    assertThat(tables2.get(0).getString(0)).isEqualTo(SCHEMA_NAME);
    assertThat(tables2.get(0).getString(1)).isEqualTo(ANOTHER_TEST_TABLE);

    assertThatThrownBy(() -> sql("SHOW TABLES in a.b.c"))
        .isInstanceOf(ApiException.class)
        .hasMessageContaining("Nested namespaces are not supported");

    // DROP TABLE
    for (String fullTableName : List.of(t1, t2)) {
      assertThat(session.catalog().tableExists(fullTableName)).isTrue();
      sql("DROP TABLE %s", fullTableName);
      assertThat(session.catalog().tableExists(fullTableName)).isFalse();
    }
    assertThatThrownBy(() -> sql("DROP TABLE a.b.c.d"))
        .isInstanceOf(ApiException.class)
        .hasMessageContaining("Nested namespaces are not supported");
  }

  /**
   * This test is used for testing table names with a hyphen to make sure that it doesn't quote with
   * backtick incorrectly. It can be testing against an external or managed table.
   */
  @Test
  public void testHyphenInTableName() {
    String catalogName = "test-catalog-name";
    String schemaName = "test-schema-name";
    String tableName = "test-table-name";
    session = createSparkSessionWithCatalogs(SPARK_CATALOG, catalogName);
    sql("CREATE SCHEMA `%s`.`%s`", catalogName, schemaName);
    String fullTableName =
        setupTable(
            new TableSetupOptions()
                .setCatalogName(catalogName)
                .setSchemaName(schemaName)
                .setTableName(tableName));

    testTableReadWrite(fullTableName);

    List<Row> tables1 = sql("SHOW TABLES in `%s`.`%s`", catalogName, schemaName);
    assertThat(tables1).hasSize(1);
    assertThat(tables1.get(0).getString(0)).isEqualTo(quoteEntityName(schemaName));
    assertThat(tables1.get(0).getString(1)).isEqualTo(tableName);

    sql("DROP TABLE %s", fullTableName);
    List<Row> tables2 = sql("SHOW TABLES in `%s`.`%s`", catalogName, schemaName);
    assertThat(tables2).isEmpty();
  }

  /**
   * Specification for one column in testTableWithSupportedDataTypes: its SQL DDL type, the SQL
   * literal used in the INSERT, the expected toString() from the result row (null = byte-array ref
   * check via startsWith("[B@")), and the expected UC catalog metadata fields.
   */
  @AllArgsConstructor
  @Getter
  private static final class ColSpec {
    private final String name;
    private final String sqlType;
    private final String insertValue;
    private final String rowValue; // null = byte-array object ref, checked with startsWith
    private final ColumnTypeName typeName;
    private final String typeText;
    private final String typeJson;
  }

  // Currently this test only works for non-Delta tables. Later it will work for more.
  @Test
  @EnabledIf("canUpdateColumnsToUC")
  public void testTableWithSupportedDataTypes() throws ApiException {
    // All data types to test
    String arrJson = "{\"type\":\"array\",\"elementType\":\"integer\",\"containsNull\":true}";
    String mapJson =
        "{\"type\":\"map\",\"keyType\":\"string\","
            + "\"valueType\":\"integer\",\"valueContainsNull\":true}";
    String structJson =
        "{\"type\":\"struct\",\"fields\":["
            + "{\"name\":\"a\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},"
            + "{\"name\":\"b\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}";
    List<ColSpec> cols =
        List.of(
            new ColSpec(
                "col_tinyint",
                "TINYINT",
                "CAST(1 AS TINYINT)",
                "1",
                ColumnTypeName.BYTE,
                "tinyint",
                "\"byte\""),
            new ColSpec(
                "col_smallint",
                "SMALLINT",
                "CAST(100 AS SMALLINT)",
                "100",
                ColumnTypeName.SHORT,
                "smallint",
                "\"short\""),
            new ColSpec("col_int", "INT", "1000", "1000", ColumnTypeName.INT, "int", "\"integer\""),
            new ColSpec(
                "col_bigint",
                "BIGINT",
                "100000",
                "100000",
                ColumnTypeName.LONG,
                "bigint",
                "\"long\""),
            new ColSpec(
                "col_float", "FLOAT", "2.5", "2.5", ColumnTypeName.FLOAT, "float", "\"float\""),
            new ColSpec(
                "col_double",
                "DOUBLE",
                "1.5",
                "1.5",
                ColumnTypeName.DOUBLE,
                "double",
                "\"double\""),
            new ColSpec(
                "col_decimal",
                "DECIMAL(10,2)",
                "123.45",
                "123.45",
                ColumnTypeName.DECIMAL,
                "decimal(10,2)",
                "\"decimal(10,2)\""),
            new ColSpec(
                "col_string",
                "STRING",
                "'test'",
                "test",
                ColumnTypeName.STRING,
                "string",
                "\"string\""),
            new ColSpec(
                "col_char",
                "CHAR(10)",
                "'char_test'",
                "char_test ",
                ColumnTypeName.CHAR,
                "char(10)",
                "\"char(10)\""),
            new ColSpec(
                "col_varchar",
                "VARCHAR(20)",
                "'varchar_test'",
                "varchar_test",
                ColumnTypeName.STRING,
                "varchar(20)",
                "\"varchar(20)\""),
            new ColSpec(
                "col_binary",
                "BINARY",
                "X'CAFEBABE'",
                null,
                ColumnTypeName.BINARY,
                "binary",
                "\"binary\""),
            new ColSpec(
                "col_boolean",
                "BOOLEAN",
                "true",
                "true",
                ColumnTypeName.BOOLEAN,
                "boolean",
                "\"boolean\""),
            new ColSpec(
                "col_date",
                "DATE",
                "DATE'2025-01-01'",
                "2025-01-01",
                ColumnTypeName.DATE,
                "date",
                "\"date\""),
            new ColSpec(
                "col_timestamp",
                "TIMESTAMP",
                "TIMESTAMP'2025-01-01 12:00:00'",
                "2025-01-01 12:00:00.0",
                ColumnTypeName.TIMESTAMP,
                "timestamp",
                "\"timestamp\""),
            new ColSpec(
                "col_timestamp_ntz",
                "TIMESTAMP_NTZ",
                "TIMESTAMP_NTZ'2025-01-01 12:00:00'",
                "2025-01-01T12:00",
                ColumnTypeName.TIMESTAMP_NTZ,
                "timestamp_ntz",
                "\"timestamp_ntz\""),
            new ColSpec(
                "col_daytime_interval",
                "INTERVAL DAY TO SECOND",
                "INTERVAL '1 00:00:00' DAY TO SECOND",
                "PT24H",
                ColumnTypeName.INTERVAL,
                "interval day to second",
                "\"interval day to second\""),
            new ColSpec(
                "col_yearmonth_interval",
                "INTERVAL YEAR TO MONTH",
                "INTERVAL '0-1' YEAR TO MONTH",
                "P1M",
                ColumnTypeName.INTERVAL,
                "interval year to month",
                "\"interval year to month\""),
            new ColSpec(
                "col_arr",
                "ARRAY<INT>",
                "array(1, 2, 3)",
                "ArraySeq(1, 2, 3)",
                ColumnTypeName.ARRAY,
                "array<int>",
                arrJson),
            new ColSpec(
                "col_map",
                "MAP<STRING, INT>",
                "map('key1', 10, 'key2', 20)",
                "Map(key1 -> 10, key2 -> 20)",
                ColumnTypeName.MAP,
                "map<string,int>",
                mapJson),
            new ColSpec(
                "col_struct",
                "STRUCT<a: INT, b: STRING>",
                "struct(42, 'test')",
                "[42,test]",
                ColumnTypeName.STRUCT,
                "struct<a:int,b:string>",
                structJson));

    session = createSparkSessionWithCatalogs(SPARK_CATALOG, CATALOG_NAME);
    String tableName = TEST_TABLE + "_complex_type";
    List<String> partitionColumns = List.of("col_daytime_interval", "col_string", "col_bigint");
    String fullTableName =
        setupTable(
            new TableSetupOptions()
                .setCatalogName(CATALOG_NAME)
                .setSchemaName(SCHEMA_NAME)
                .setTableName(tableName)
                .setColumns(
                    cols.stream()
                        .map(c -> Pair.of(c.getName(), c.getSqlType()))
                        .collect(Collectors.toList()))
                .setPartitionColumns(partitionColumns));
    String colNames = cols.stream().map(ColSpec::getName).collect(Collectors.joining(", "));
    sql(
        "INSERT INTO %s (%s) VALUES (%s)",
        fullTableName,
        colNames,
        cols.stream().map(ColSpec::getInsertValue).collect(Collectors.joining(", ")));

    List<Row> queryResult = sql("SELECT %s FROM %s", colNames, fullTableName);
    assertThat(queryResult).hasSize(1);
    List<String> row =
        IntStream.range(0, queryResult.get(0).length())
            .mapToObj(
                i -> queryResult.get(0).isNullAt(i) ? null : queryResult.get(0).get(i).toString())
            .collect(Collectors.toList());

    TableInfo tableInfo = tableOperations.getTable(fullTableName);
    List<ColumnInfo> columns = tableInfo.getColumns();
    assertThat(columns).hasSize(cols.size());

    for (int i = 0; i < cols.size(); i++) {
      ColSpec spec = cols.get(i);
      if (spec.getTypeName() == ColumnTypeName.BINARY) {
        // BINARY: row value is a Java byte array — toString() produces a ref like "[B@..."
        assertThat(row.get(i)).as("row value for %s", spec.getName()).startsWith("[B@");
      } else {
        assertThat(row.get(i)).as("row value for %s", spec.getName()).isEqualTo(spec.getRowValue());
      }
      ColumnInfo col = columns.get(i);
      assertThat(col.getName()).as("name[%d]", i).isEqualTo(spec.getName());
      assertThat(col.getTypeName())
          .as("typeName for %s", spec.getName())
          .isEqualTo(spec.getTypeName());
      assertThat(col.getTypeText())
          .as("typeText for %s", spec.getName())
          .isEqualTo(spec.getTypeText());
      assertThat(col.getTypeJson())
          .as("typeJson for %s", spec.getName())
          .isEqualTo(spec.getTypeJson());
      int partitionIndex = partitionColumns.indexOf(col.getName());
      if (partitionIndex != -1) {
        assertThat(col.getPartitionIndex()).isEqualTo(partitionIndex);
      } else {
        assertThat(col.getPartitionIndex()).isNull();
      }
    }
  }

  protected String quoteEntityName(String entityName) {
    return entityName.contains("-") ? String.format("`%s`", entityName) : entityName;
  }

  protected void testTableReadWrite(String tableFullName) {
    assertThat(sql("SELECT * FROM %s", tableFullName)).isEmpty();
    sql("INSERT INTO %s SELECT 1, 'a'", tableFullName);
    validateRows(sql("SELECT * FROM %s", tableFullName), Pair.of(1, "a"));
  }

  protected void testTableReadWriteCreatedAsSelect(
      String tableFullName, Pair<Integer, String> asSelect) {
    validateRows(sql("SELECT * FROM %s", tableFullName), asSelect);
    Pair<Integer, String> newValue = Pair.of(asSelect.getLeft() + 1, asSelect.getRight() + "1");
    sql("INSERT INTO %s SELECT %d, '%s'", tableFullName, newValue.getLeft(), newValue.getRight());
    validateRows(sql("SELECT * FROM %s ORDER BY i", tableFullName), asSelect, newValue);
  }
}
