package io.unitycatalog.spark;

import static io.unitycatalog.server.utils.TestUtils.CATALOG_NAME;
import static io.unitycatalog.server.utils.TestUtils.SCHEMA_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.client.ApiException;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public abstract class BaseTableReadWriteTest extends BaseSparkIntegrationTest {

  protected static final String TEST_TABLE = "test_table";
  protected static final String ANOTHER_TEST_TABLE = "test_table_another";
  protected static final String TBLPROPERTIES_CATALOG_OWNED_CLAUSE =
      String.format(
          "TBLPROPERTIES ('%s'='%s')",
          UCTableProperties.DELTA_CATALOG_MANAGED_KEY,
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
    private Optional<String> partitionColumn = Optional.empty();
    @With private Optional<Pair<Integer, String>> asSelect = Optional.empty();
    private Optional<String> comment = Optional.empty();
    private boolean replaceTable = false;

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
      assert List.of("i", "s").contains(column);
      partitionColumn = Optional.of(column);
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
      return partitionColumn.map(c -> String.format("PARTITIONED BY (%s)", c)).orElse("");
    }

    public String columnsClause() {
      if (asSelect.isEmpty()) {
        return "(i INT, s STRING)";
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
      return replaceTable ? "REPLACE" : "CREATE";
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
   * Creates a managed table in a catalog with an unconfigured storage root. This is useful for
   * testing credential fallback behavior when credentials are unavailable.
   *
   * <p>Creates a new catalog with storage root pointing to an unconfigured bucket (no credentials
   * configured in the server), then creates a schema and managed table in that catalog. The managed
   * table will inherit the catalog's storage root and trigger credential failure when accessed.
   *
   * @param tableName the table name
   * @return the full table name
   */
  protected String setupTableWithUnconfiguredStorage(String tableName) throws ApiException {
    // Create catalog with unconfigured storage root (no credentials for this bucket)
    String unconfiguredCatalogName = "catalog_no_creds";
    String unconfiguredStorageRoot = "s3://unconfigured-bucket-no-creds";

    catalogOperations.createCatalog(
        new io.unitycatalog.client.model.CreateCatalog()
            .name(unconfiguredCatalogName)
            .comment("Catalog with unconfigured storage for testing")
            .storageRoot(unconfiguredStorageRoot));
    createdCatalogs.add(unconfiguredCatalogName);

    // Create schema in the unconfigured catalog
    schemaOperations.createSchema(
        new io.unitycatalog.client.model.CreateSchema()
            .name(SCHEMA_NAME)
            .catalogName(unconfiguredCatalogName));

    // Create managed table - will use catalog's unconfigured storage root
    String fullTableName =
        String.format("%s.%s.%s", unconfiguredCatalogName, SCHEMA_NAME, tableName);
    sql(
        "CREATE TABLE %s (id INT, name STRING) USING DELTA %s",
        fullTableName, TBLPROPERTIES_CATALOG_OWNED_CLAUSE);

    return fullTableName;
  }

  /**
   * The table format to be tested. Subclasses override this function to test different table
   * formats.
   */
  protected abstract String tableFormat();

  protected final boolean testingDelta() {
    return tableFormat().equalsIgnoreCase("DELTA");
  }

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
  public void testTableCreateReadWrite() {
    // Test both `spark_catalog` and other catalog names.
    session = createSparkSessionWithCatalogs(SPARK_CATALOG, CATALOG_NAME);

    int tableNameCounter = 0;
    for (String catalogName : List.of(SPARK_CATALOG, CATALOG_NAME)) {
      for (boolean withPartitionColumns : List.of(true, false)) {
        for (boolean withCtas : List.of(true, false)) {
          for (boolean replaceTable : List.of(true, false)) {
            String tableName = TEST_TABLE + tableNameCounter;
            tableNameCounter++;
            if (replaceTable) {
              if (withCtas && !testingDelta()) {
                // "REPLACE TABLE AS SELECT" is only supported for Delta.
                continue;
              }
              // First, create a different table to replace.
              sql(
                  "CREATE TABLE %s.%s.%s USING DELTA %s AS SELECT 0.1 AS col1",
                  catalogName, SCHEMA_NAME, tableName, TBLPROPERTIES_CATALOG_OWNED_CLAUSE);
            }
            TableSetupOptions options =
                new TableSetupOptions()
                    .setCatalogName(catalogName)
                    .setTableName(tableName)
                    .setReplaceTable(replaceTable);
            if (withPartitionColumns) {
              options.setPartitionColumn("s");
            }
            if (withCtas) {
              options.setAsSelect(1, "a");
            }
            String t1 = setupTable(options);
            if (withCtas) {
              testTableReadWriteCreatedAsSelect(t1, Pair.of(1, "a"));
            } else {
              testTableReadWrite(t1);
            }
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
  public void testTableOperations(String scheme, boolean renewCredEnabled) {
    session = createSparkSessionWithCatalogs(renewCredEnabled, SPARK_CATALOG, CATALOG_NAME);

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
