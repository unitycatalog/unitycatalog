package io.unitycatalog.connectors.spark;

import static io.unitycatalog.server.utils.TestUtils.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.*;
import io.unitycatalog.server.base.table.TableOperations;
import io.unitycatalog.server.sdk.tables.SdkTableOperations;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.network.util.JavaUtils;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class TableReadWriteTest extends BaseSparkIntegrationTest {

  private static final String ANOTHER_PARQUET_TABLE = "test_parquet_another";
  private static final String PARQUET_TABLE_PARTITIONED = "test_parquet_partitioned";
  private static final String DELTA_TABLE = "test_delta";
  private static final String PARQUET_TABLE = "test_parquet";
  private static final String ANOTHER_DELTA_TABLE = "test_delta_another";
  private static final String DELTA_TABLE_PARTITIONED = "test_delta_partitioned";

  private final File dataDir = new File(System.getProperty("java.io.tmpdir"), "spark_test");

  private TableOperations tableOperations;

  @Test
  public void testParquetReadWrite() throws IOException, ApiException {
    SparkSession session = createSparkSessionWithCatalogs(SPARK_CATALOG);
    // Spark only allow `spark_catalog` to return built-in file source tables.
    setupExternalParquetTable(PARQUET_TABLE, new ArrayList<>(0));
    testTableReadWrite(SPARK_CATALOG + "." + SCHEMA_NAME + "." + PARQUET_TABLE, session);

    setupExternalParquetTable(PARQUET_TABLE_PARTITIONED, Arrays.asList("s"));
    testTableReadWrite(
        SPARK_CATALOG + "." + SCHEMA_NAME + "." + PARQUET_TABLE_PARTITIONED, session);

    session.stop();
  }

  @Test
  public void testDeltaReadWrite() throws IOException, ApiException {
    // Test both `spark_catalog` and other catalog names.
    SparkSession session = createSparkSessionWithCatalogs(SPARK_CATALOG, CATALOG_NAME);

    setupExternalDeltaTable(SPARK_CATALOG, DELTA_TABLE, new ArrayList<>(0), session);
    testTableReadWrite(SPARK_CATALOG + "." + SCHEMA_NAME + "." + DELTA_TABLE, session);

    setupExternalDeltaTable(SPARK_CATALOG, DELTA_TABLE_PARTITIONED, Arrays.asList("s"), session);
    testTableReadWrite(SPARK_CATALOG + "." + SCHEMA_NAME + "." + DELTA_TABLE_PARTITIONED, session);

    setupExternalDeltaTable(CATALOG_NAME, DELTA_TABLE, new ArrayList<>(0), session);
    testTableReadWrite(CATALOG_NAME + "." + SCHEMA_NAME + "." + DELTA_TABLE, session);

    setupExternalDeltaTable(CATALOG_NAME, DELTA_TABLE_PARTITIONED, Arrays.asList("s"), session);
    testTableReadWrite(CATALOG_NAME + "." + SCHEMA_NAME + "." + DELTA_TABLE_PARTITIONED, session);

    session.stop();
  }

  @Test
  public void testDeltaPathTable() throws IOException {
    // We must replace the `spark_catalog` in order to support Delta path tables.
    SparkSession session = createSparkSessionWithCatalogs(SPARK_CATALOG);

    String path1 = new File(dataDir, "test_delta_path1").getCanonicalPath();
    String tableName1 = String.format("delta.`%s`", path1);
    session.sql(String.format("CREATE TABLE %s(i INT) USING delta", tableName1));
    assertThat(session.sql("SELECT * FROM " + tableName1).collectAsList()).isEmpty();
    session.sql("INSERT INTO " + tableName1 + " SELECT 1");
    assertThat(session.sql("SELECT * FROM " + tableName1).collectAsList())
        .first()
        .extracting(row -> row.get(0))
        .isEqualTo(1);

    // Test CTAS
    String path2 = new File(dataDir, "test_delta_path2").getCanonicalPath();
    String tableName2 = String.format("delta.`%s`", path2);
    session.sql(String.format("CREATE TABLE %s USING delta AS SELECT 1 AS i", tableName2));
    assertThat(session.sql("SELECT * FROM " + tableName2).collectAsList())
        .first()
        .extracting(row -> row.get(0))
        .isEqualTo(1);

    session.stop();
  }

  @ParameterizedTest
  @ValueSource(strings = {"s3", "gs", "abfs"})
  public void testCredentialParquet(String scheme) throws ApiException, IOException {
    SparkSession session = createSparkSessionWithCatalogs(SPARK_CATALOG);

    String loc1 = scheme + "://test-bucket0" + generateTableLocation(SPARK_CATALOG, PARQUET_TABLE);
    setupExternalParquetTable(PARQUET_TABLE, loc1, new ArrayList<>(0));
    String t1 = SPARK_CATALOG + "." + SCHEMA_NAME + "." + PARQUET_TABLE;
    testTableReadWrite(t1, session);

    String loc2 =
        scheme + "://test-bucket1" + generateTableLocation(SPARK_CATALOG, ANOTHER_PARQUET_TABLE);
    setupExternalParquetTable(ANOTHER_PARQUET_TABLE, loc2, new ArrayList<>(0));
    String t2 = SPARK_CATALOG + "." + SCHEMA_NAME + "." + ANOTHER_PARQUET_TABLE;
    testTableReadWrite(t2, session);

    Row row =
        session
            .sql(String.format("SELECT l.i FROM %s l JOIN %s r ON l.i = r.i", t1, t2))
            .collectAsList()
            .get(0);
    assertThat(row.getInt(0)).isEqualTo(1);

    session.stop();
  }

  @Disabled("Ignoring test until Delta 3.2.1 is released.")
  @ParameterizedTest
  @ValueSource(strings = {"s3", "gs", "abfs"})
  public void testCredentialDelta(String scheme) throws ApiException, IOException {
    SparkSession session = createSparkSessionWithCatalogs(SPARK_CATALOG, CATALOG_NAME);

    String loc0 = scheme + "://test-bucket0" + generateTableLocation(SPARK_CATALOG, DELTA_TABLE);
    setupExternalDeltaTable(SPARK_CATALOG, DELTA_TABLE, loc0, new ArrayList<>(0), session);
    String t1 = SPARK_CATALOG + "." + SCHEMA_NAME + "." + DELTA_TABLE;
    testTableReadWrite(t1, session);

    String loc1 = scheme + "://test-bucket1" + generateTableLocation(CATALOG_NAME, DELTA_TABLE);
    setupExternalDeltaTable(CATALOG_NAME, DELTA_TABLE, loc1, new ArrayList<>(0), session);
    String t2 = CATALOG_NAME + "." + SCHEMA_NAME + "." + DELTA_TABLE;
    testTableReadWrite(t2, session);

    Row row =
        session
            .sql(String.format("SELECT l.i FROM %s l JOIN %s r ON l.i = r.i", t1, t2))
            .collectAsList()
            .get(0);
    assertThat(row.getInt(0)).isEqualTo(1);

    session.stop();
  }

  @Disabled("Ignoring test until Delta 3.2.1 is released.")
  @ParameterizedTest
  @ValueSource(strings = {"s3", "gs", "abfs"})
  public void testDeleteDeltaTable(String scheme) throws ApiException, IOException {
    SparkSession session = createSparkSessionWithCatalogs(SPARK_CATALOG);

    String loc1 = scheme + "://test-bucket0" + generateTableLocation(SPARK_CATALOG, DELTA_TABLE);
    setupExternalDeltaTable(SPARK_CATALOG, DELTA_TABLE, loc1, new ArrayList<>(0), session);
    String t1 = SPARK_CATALOG + "." + SCHEMA_NAME + "." + DELTA_TABLE;
    testTableReadWrite(t1, session);

    session.sql(String.format("DELETE FROM %s WHERE i = 1", t1));
    List<Row> rows = session.sql("SELECT * FROM " + t1).collectAsList();
    assertThat(0 == rows.size());

    session.stop();
  }

  @Disabled("Ignoring test until Delta 3.2.1 is released.")
  @ParameterizedTest
  @ValueSource(strings = {"s3", "gs", "abfs"})
  public void testMergeDeltaTable(String scheme) throws ApiException, IOException {
    SparkSession session = createSparkSessionWithCatalogs(SPARK_CATALOG, CATALOG_NAME);

    String loc1 = scheme + "://test-bucket0" + generateTableLocation(SPARK_CATALOG, DELTA_TABLE);
    setupExternalDeltaTable(SPARK_CATALOG, DELTA_TABLE, loc1, new ArrayList<>(0), session);
    String t1 = SPARK_CATALOG + "." + SCHEMA_NAME + "." + DELTA_TABLE;
    session.sql("INSERT INTO " + t1 + " SELECT 1, 'a'");

    String loc2 =
        scheme + "://test-bucket1" + generateTableLocation(CATALOG_NAME, ANOTHER_DELTA_TABLE);
    setupExternalDeltaTable(CATALOG_NAME, ANOTHER_DELTA_TABLE, loc2, new ArrayList<>(0), session);
    String t2 = CATALOG_NAME + "." + SCHEMA_NAME + "." + ANOTHER_DELTA_TABLE;
    session.sql("INSERT INTO " + t2 + " SELECT 2, 'b'");

    session.sql(
        String.format(
            "MERGE INTO %s USING %s ON %s.i = %s.i WHEN NOT MATCHED THEN INSERT *",
            t1, t2, t1, t2));
    List<Row> rows = session.sql("SELECT * FROM " + t1).collectAsList();
    assertThat(2 == rows.size());

    session.stop();
  }

  @Disabled("Ignoring test until Delta 3.2.1 is released.")
  @ParameterizedTest
  @ValueSource(strings = {"s3", "gs", "abfs"})
  public void testUpdateDeltaTable(String scheme) throws ApiException, IOException {
    SparkSession session = createSparkSessionWithCatalogs(SPARK_CATALOG);

    String loc1 = scheme + "://test-bucket0" + generateTableLocation(SPARK_CATALOG, DELTA_TABLE);
    setupExternalDeltaTable(SPARK_CATALOG, DELTA_TABLE, loc1, new ArrayList<>(0), session);
    String t1 = SPARK_CATALOG + "." + SCHEMA_NAME + "." + DELTA_TABLE;
    session.sql("INSERT INTO " + t1 + " SELECT 1, 'a'");

    session.sql(String.format("UPDATE %s SET i = 2 WHERE i = 1", t1));
    List<Row> rows = session.sql("SELECT * FROM " + t1).collectAsList();
    assertThat(1 == rows.size());
    assertThat(2 == rows.get(0).getInt(0));
    session.stop();
  }

  @Test
  public void testShowTables() throws ApiException, IOException {
    SparkSession session = createSparkSessionWithCatalogs(SPARK_CATALOG);
    setupExternalParquetTable(PARQUET_TABLE, new ArrayList<>(0));

    Row[] tables = (Row[]) session.sql("SHOW TABLES in " + SCHEMA_NAME).collect();
    assertThat(tables).hasSize(1);
    assertThat(tables[0].getString(0)).isEqualTo(SCHEMA_NAME);
    assertThat(tables[0].getString(1)).isEqualTo(PARQUET_TABLE);

    assertThatThrownBy(() -> session.sql("SHOW TABLES in a.b.c").collect())
        .isInstanceOf(AnalysisException.class)
        .hasMessageContaining("a.b.c");

    session.stop();
  }

  @Test
  public void testDropTable() throws ApiException, IOException {
    SparkSession session = createSparkSessionWithCatalogs(SPARK_CATALOG);
    setupExternalParquetTable(PARQUET_TABLE, new ArrayList<>(0));
    String fullName = String.join(".", SPARK_CATALOG, SCHEMA_NAME, PARQUET_TABLE);
    assertTrue(session.catalog().tableExists(fullName));
    session.sql("DROP TABLE " + fullName).collect();
    assertFalse(session.catalog().tableExists(fullName));
    assertThatThrownBy(() -> session.sql("DROP TABLE a.b.c.d").collect())
        .isInstanceOf(AnalysisException.class);
    session.stop();
  }

  private void setupExternalParquetTable(String tableName, List<String> partitionColumns)
      throws IOException, ApiException {
    String location = generateTableLocation(SPARK_CATALOG, tableName);
    setupExternalParquetTable(tableName, location, partitionColumns);
  }

  private void setupExternalParquetTable(
      String tableName, String location, List<String> partitionColumns)
      throws IOException, ApiException {
    setupTables(
        SPARK_CATALOG, tableName, DataSourceFormat.PARQUET, location, partitionColumns, false);
  }

  private void setupExternalDeltaTable(
      String catalogName, String tableName, List<String> partitionColumns, SparkSession session)
      throws IOException, ApiException {
    String location = generateTableLocation(catalogName, tableName);
    setupExternalDeltaTable(catalogName, tableName, location, partitionColumns, session);
  }

  @Test
  public void testCreateExternalParquetTable() throws ApiException, IOException {
    SparkSession session = createSparkSessionWithCatalogs(CATALOG_NAME);
    String path = generateTableLocation(CATALOG_NAME, PARQUET_TABLE);
    String fullTableName = CATALOG_NAME + "." + SCHEMA_NAME + "." + PARQUET_TABLE;
    session
        .sql(
            "CREATE TABLE " + fullTableName + "(name STRING) USING PARQUET LOCATION '" + path + "'")
        .collect();
    assertTrue(session.catalog().tableExists(fullTableName));
    TableInfo tableInfo = tableOperations.getTable(fullTableName);
    assertEquals(1, tableInfo.getColumns().size());
    assertEquals("name", tableInfo.getColumns().get(0).getName());
    assertEquals(ColumnTypeName.STRING, tableInfo.getColumns().get(0).getTypeName());
    session.stop();
  }

  @Disabled("Ignoring test until Delta 3.2.1 is released.")
  @Test
  public void testCreateExternalDeltaTable() throws ApiException, IOException {
    SparkSession session = createSparkSessionWithCatalogs(SPARK_CATALOG, CATALOG_NAME);
    String path1 = generateTableLocation(SPARK_CATALOG, DELTA_TABLE);
    String path2 = generateTableLocation(CATALOG_NAME, DELTA_TABLE);
    session.sql(String.format("CREATE TABLE delta.`%s`(name STRING) USING delta", path1));
    session.sql(String.format("CREATE TABLE delta.`%s`(name STRING) USING delta", path2));

    String fullTableName1 = SPARK_CATALOG + "." + SCHEMA_NAME + "." + DELTA_TABLE;
    session.sql(
        "CREATE TABLE " + fullTableName1 + "(name STRING) USING delta LOCATION '" + path1 + "'");
    assertTrue(session.catalog().tableExists(fullTableName1));
    TableInfo tableInfo1 = tableOperations.getTable(fullTableName1);
    // By default, Delta tables do not store schema in the catalog.
    assertTrue(tableInfo1.getColumns().isEmpty());
    assertTrue(session.table(fullTableName1).collectAsList().isEmpty());
    StructType schema1 = session.table(fullTableName1).schema();
    assertEquals("name", schema1.apply(0).name());
    assertEquals(DataTypes.StringType, schema1.apply(0).dataType());

    String fullTableName2 = CATALOG_NAME + "." + SCHEMA_NAME + "." + DELTA_TABLE;
    session.sql(
        "CREATE TABLE " + fullTableName2 + "(name STRING) USING delta LOCATION '" + path2 + "'");
    assertTrue(session.catalog().tableExists(fullTableName2));
    TableInfo tableInfo2 = tableOperations.getTable(fullTableName2);
    // By default, Delta tables do not store schema in the catalog.
    assertTrue(tableInfo2.getColumns().isEmpty());
    assertTrue(session.table(fullTableName2).collectAsList().isEmpty());
    StructType schema2 = session.table(fullTableName2).schema();
    assertEquals("name", schema2.apply(0).name());
    assertEquals(DataTypes.StringType, schema2.apply(0).dataType());

    session.stop();
  }

  @Test
  public void testCreateManagedParquetTable() {
    SparkSession session = createSparkSessionWithCatalogs(CATALOG_NAME);
    String fullTableName = CATALOG_NAME + "." + SCHEMA_NAME + "." + PARQUET_TABLE;
    assertThatThrownBy(() -> {
      session.sql("CREATE TABLE " + fullTableName + "(name STRING) USING parquet");
    }).hasMessageContaining("not support managed table");
    session.close();
  }

  @Disabled("Ignoring test until Delta 3.2.1 is released.")
  @Test
  public void testCreateManagedDeltaTable() {
    SparkSession session = createSparkSessionWithCatalogs(SPARK_CATALOG, CATALOG_NAME);

    String fullTableName1 = SPARK_CATALOG + "." + SCHEMA_NAME + "." + DELTA_TABLE;
    assertThatThrownBy(() -> {
      session.sql("CREATE TABLE " + fullTableName1 + "(name STRING) USING delta");
    }).hasMessageContaining("not support managed table");

    String fullTableName2 = CATALOG_NAME + "." + SCHEMA_NAME + "." + DELTA_TABLE;
    assertThatThrownBy(() -> {
      session.sql("CREATE TABLE " + fullTableName2 + "(name STRING) USING delta");
    }).hasMessageContaining("not support managed table");

    session.close();
  }

  private String generateTableLocation(String catalogName, String tableName) throws IOException {
    return new File(new File(dataDir, catalogName), tableName).getCanonicalPath();
  }

  private void setupExternalDeltaTable(
      String catalogName,
      String tableName,
      String location,
      List<String> partitionColumns,
      SparkSession session)
      throws IOException, ApiException {
    // The Delta path can't be empty, need to initialize before read.
    String partitionClause;
    if (partitionColumns.isEmpty()) {
      partitionClause = "";
    } else {
      partitionClause = String.format(" PARTITIONED BY (%s)", String.join(", ", partitionColumns));
    }

    // Temporarily disable the credential check when setting up the external Delta location which
    // does not involve Unity Catalog at all.
    CredentialTestFileSystem.credentialCheckEnabled = false;
    session.sql(
        String.format("CREATE TABLE delta.`%s`(i INT, s STRING) USING delta", location)
            + partitionClause);
    CredentialTestFileSystem.credentialCheckEnabled = true;

    setupTables(catalogName, tableName, DataSourceFormat.DELTA, location, partitionColumns, false);
  }

  private void testTableReadWrite(String tableFullName, SparkSession session) {
    assertThat(session.sql("SELECT * FROM " + tableFullName).collectAsList()).isEmpty();
    session.sql("INSERT INTO " + tableFullName + " SELECT 1, 'a'");
    Row row = session.sql("SELECT * FROM " + tableFullName).collectAsList().get(0);
    assertThat(row.getInt(0)).isEqualTo(1);
    assertThat(row.getString(1)).isEqualTo("a");
  }

  private void setupTables(
      String catalogName,
      String tableName,
      DataSourceFormat format,
      String location,
      List<String> partitionColumns,
      boolean isManaged)
      throws IOException, ApiException {
    Integer partitionIndex1 = partitionColumns.indexOf("i");
    if (partitionIndex1 == -1) partitionIndex1 = null;
    Integer partitionIndex2 = partitionColumns.indexOf("s");
    if (partitionIndex2 == -1) partitionIndex2 = null;

    ColumnInfo c1 =
        new ColumnInfo()
            .name("i")
            .typeText("INTEGER")
            .typeJson("{\"type\": \"integer\"}")
            .typeName(ColumnTypeName.INT)
            .typePrecision(10)
            .typeScale(0)
            .position(0)
            .partitionIndex(partitionIndex1)
            .comment("Integer column")
            .nullable(true);

    ColumnInfo c2 =
        new ColumnInfo()
            .name("s")
            .typeText("STRING")
            .typeJson("{\"type\": \"string\"}")
            .typeName(ColumnTypeName.STRING)
            .position(1)
            .partitionIndex(partitionIndex2)
            .comment("String column")
            .nullable(true);
    TableType tableType;
    if (isManaged) {
      tableType = TableType.MANAGED;
    } else {
      tableType = TableType.EXTERNAL;
    }
    CreateTable createTableRequest =
        new CreateTable()
            .name(tableName)
            .catalogName(catalogName)
            .schemaName(SCHEMA_NAME)
            .columns(Arrays.asList(c1, c2))
            .comment(COMMENT)
            .tableType(tableType)
            .dataSourceFormat(format);
    if (!isManaged) {
      createTableRequest = createTableRequest.storageLocation(location);
    }
    tableOperations.createTable(createTableRequest);
  }

  @BeforeEach
  @Override
  public void setUp() {
    super.setUp();
    tableOperations = new SdkTableOperations(createApiClient(serverConfig));
  }

  @Override
  public void cleanUp() {
    super.cleanUp();
    try {
      JavaUtils.deleteRecursively(dataDir);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
