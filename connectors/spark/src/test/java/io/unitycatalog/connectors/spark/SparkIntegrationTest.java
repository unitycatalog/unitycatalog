package io.unitycatalog.connectors.spark;

import static io.unitycatalog.server.utils.TestUtils.*;
import static org.junit.jupiter.api.Assertions.*;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.*;
import io.unitycatalog.server.base.BaseCRUDTest;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.catalog.CatalogOperations;
import io.unitycatalog.server.base.schema.SchemaOperations;
import io.unitycatalog.server.base.table.TableOperations;
import io.unitycatalog.server.sdk.catalog.SdkCatalogOperations;
import io.unitycatalog.server.sdk.schema.SdkSchemaOperations;
import io.unitycatalog.server.sdk.tables.SdkTableOperations;
import io.unitycatalog.server.utils.TestUtils;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import org.apache.spark.network.util.JavaUtils;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SparkIntegrationTest extends BaseCRUDTest {

  private static final String SPARK_CATALOG = "spark_catalog";
  private static final String PARQUET_TABLE = "test_parquet";
  private static final String ANOTHER_PARQUET_TABLE = "test_parquet_another";
  private static final String PARQUET_TABLE_PARTITIONED = "test_parquet_partitioned";
  private static final String DELTA_TABLE = "test_delta";
  private static final String DELTA_TABLE_PARTITIONED = "test_delta_partitioned";

  private final File dataDir = new File(System.getProperty("java.io.tmpdir"), "spark_test");

  @Test
  public void testParquetReadWrite() throws IOException, ApiException {
    createCommonResources();
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
    createCommonResources();
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
  public void testDeltaPathTable() throws IOException, ApiException {
    createCommonResources();
    // We must replace the `spark_catalog` in order to support Delta path tables.
    SparkSession session = createSparkSessionWithCatalogs(SPARK_CATALOG);

    String path1 = new File(dataDir, "test_delta_path1").getCanonicalPath();
    String tableName1 = String.format("delta.`%s`", path1);
    session.sql(String.format("CREATE TABLE %s(i INT) USING delta", tableName1));
    assertTrue(session.sql("SELECT * FROM " + tableName1).collectAsList().isEmpty());
    session.sql("INSERT INTO " + tableName1 + " SELECT 1");
    assertEquals(1, session.sql("SELECT * FROM " + tableName1).collectAsList().get(0).getInt(0));

    // Test CTAS
    String path2 = new File(dataDir, "test_delta_path2").getCanonicalPath();
    String tableName2 = String.format("delta.`%s`", path2);
    session.sql(String.format("CREATE TABLE %s USING delta AS SELECT 1 AS i", tableName2));
    assertEquals(1, session.sql("SELECT * FROM " + tableName2).collectAsList().get(0).getInt(0));

    session.stop();
  }

  @Test
  public void testCredentialParquet() throws ApiException, IOException {
    createCommonResources();
    SparkSession session = createSparkSessionWithCatalogs(SPARK_CATALOG);

    String loc0 = "s3://test-bucket0" + generateTableLocation(SPARK_CATALOG, PARQUET_TABLE);
    setupExternalParquetTable(PARQUET_TABLE, loc0, new ArrayList<>(0));
    String t1 = SPARK_CATALOG + "." + SCHEMA_NAME + "." + PARQUET_TABLE;
    testTableReadWrite(t1, session);

    String loc1 = "s3://test-bucket1" + generateTableLocation(SPARK_CATALOG, ANOTHER_PARQUET_TABLE);
    setupExternalParquetTable(ANOTHER_PARQUET_TABLE, loc1, new ArrayList<>(0));
    String t2 = SPARK_CATALOG + "." + SCHEMA_NAME + "." + ANOTHER_PARQUET_TABLE;
    testTableReadWrite(t2, session);

    Row row =
        session
            .sql(String.format("SELECT l.i FROM %s l JOIN %s r ON l.i = r.i", t1, t2))
            .collectAsList()
            .get(0);
    assertEquals(1, row.getInt(0));

    session.stop();
  }

  @Test
  public void testCredentialDelta() throws ApiException, IOException {
    createCommonResources();
    SparkSession session = createSparkSessionWithCatalogs(SPARK_CATALOG, CATALOG_NAME);

    String loc0 = "s3://test-bucket0" + generateTableLocation(SPARK_CATALOG, DELTA_TABLE);
    setupExternalDeltaTable(SPARK_CATALOG, DELTA_TABLE, loc0, new ArrayList<>(0), session);
    String t1 = SPARK_CATALOG + "." + SCHEMA_NAME + "." + DELTA_TABLE;
    testTableReadWrite(t1, session);

    String loc1 = "s3://test-bucket1" + generateTableLocation(CATALOG_NAME, DELTA_TABLE);
    setupExternalDeltaTable(CATALOG_NAME, DELTA_TABLE, loc1, new ArrayList<>(0), session);
    String t2 = CATALOG_NAME + "." + SCHEMA_NAME + "." + DELTA_TABLE;
    testTableReadWrite(t2, session);

    Row row =
      session
        .sql(String.format("SELECT l.i FROM %s l JOIN %s r ON l.i = r.i", t1, t2))
        .collectAsList()
        .get(0);
    assertEquals(1, row.getInt(0));

    session.stop();
  }

  @Test
  public void testDeleteDeltaTable() throws ApiException, IOException {
    createCommonResources();
    SparkSession session = createSparkSessionWithCatalogs(SPARK_CATALOG, CATALOG_NAME);
    Path tempDirectory = Files.createTempDirectory("testCredentialDelta");

    String loc0 = tempDirectory.toString() + generateTableLocation(SPARK_CATALOG, DELTA_TABLE);
    setupExternalDeltaTable(SPARK_CATALOG, DELTA_TABLE, loc0, new ArrayList<>(0), session);
    String t1 = SPARK_CATALOG + "." + SCHEMA_NAME + "." + DELTA_TABLE;
    testTableReadWrite(t1, session);

    List<Row> ret =
      session
        .sql(String.format("DELETE FROM %s WHERE %s.i == 1", t1, t1))
        .collectAsList();
    assertEquals(0, ret.size());

    session.stop();
  }

  @Test
  public void testShowTables() throws ApiException, IOException {
    createCommonResources();
    SparkSession session = createSparkSessionWithCatalogs(SPARK_CATALOG);
    setupExternalParquetTable(PARQUET_TABLE, new ArrayList<>(0));

    Row[] tables = (Row[]) session.sql("SHOW TABLES in " + SCHEMA_NAME).collect();
    assertEquals(tables.length, 1);
    assertEquals(tables[0].getString(0), SCHEMA_NAME);
    assertEquals(tables[0].getString(1), PARQUET_TABLE);

    AnalysisException exception =
        assertThrows(AnalysisException.class, () -> session.sql("SHOW TABLES in a.b.c").collect());
    assertTrue(exception.getMessage().contains("a.b.c"));

    session.stop();
  }

  @Test
  public void testDropTable() throws ApiException, IOException {
    createCommonResources();
    SparkSession session = createSparkSessionWithCatalogs(SPARK_CATALOG);
    setupExternalParquetTable(PARQUET_TABLE, new ArrayList<>(0));
    String fullName = String.join(".", SPARK_CATALOG, SCHEMA_NAME, PARQUET_TABLE);
    assertTrue(session.catalog().tableExists(fullName));
    session.sql("DROP TABLE " + fullName).collect();
    assertFalse(session.catalog().tableExists(fullName));
    AnalysisException exception =
        assertThrows(AnalysisException.class, () -> session.sql("DROP TABLE a.b.c.d").collect());
    session.stop();
  }

  @Test
  public void testSetCurrentDB() throws ApiException {
    createCommonResources();
    SparkSession session = createSparkSessionWithCatalogs(SPARK_CATALOG, TestUtils.CATALOG_NAME);
    session.catalog().setCurrentCatalog(TestUtils.CATALOG_NAME);
    session.catalog().setCurrentDatabase(SCHEMA_NAME);
    session.catalog().setCurrentCatalog(SPARK_CATALOG);
    // TODO: We need to apply a fix on Spark side to use v2 session catalog handle
    // `setCurrentDatabase` when the catalog name is `spark_catalog`.
    // session.catalog().setCurrentDatabase(SCHEMA_NAME);
    session.stop();
  }

  private String generateTableLocation(String catalogName, String tableName) throws IOException {
    return new File(new File(dataDir, catalogName), tableName).getCanonicalPath();
  }

  private void testTableReadWrite(String tableFullName, SparkSession session) {
    assertTrue(session.sql("SELECT * FROM " + tableFullName).collectAsList().isEmpty());
    session.sql("INSERT INTO " + tableFullName + " SELECT 1, 'a'");
    Row row = session.sql("SELECT * FROM " + tableFullName).collectAsList().get(0);
    assertEquals(1, row.getInt(0));
    assertEquals("a", row.getString(1));
  }

  private SchemaOperations schemaOperations;
  private TableOperations tableOperations;

  private void createCommonResources() throws ApiException {
    // Common setup operations such as creating a catalog and schema
    catalogOperations.createCatalog(
        new CreateCatalog().name(TestUtils.CATALOG_NAME).comment(TestUtils.COMMENT));
    schemaOperations.createSchema(new CreateSchema().name(SCHEMA_NAME).catalogName(CATALOG_NAME));
    catalogOperations.createCatalog(
        new CreateCatalog().name(SPARK_CATALOG).comment("Spark catalog"));
    schemaOperations.createSchema(new CreateSchema().name(SCHEMA_NAME).catalogName(SPARK_CATALOG));
  }

  private SparkSession createSparkSessionWithCatalogs(String... catalogs) {
    SparkSession.Builder builder =
        SparkSession.builder()
            .appName("test")
            .master("local[*]")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension");
    for (String catalog : catalogs) {
      String catalogConf = "spark.sql.catalog." + catalog;
      builder =
          builder
              .config(catalogConf, UCSingleCatalog.class.getName())
              .config(catalogConf + ".uri", serverConfig.getServerUrl())
              .config(catalogConf + ".token", serverConfig.getAuthToken());
    }
    // Use fake file system for s3:// so that we can test credentials.
    builder.config("fs.s3.impl", CredentialTestFileSystem.class.getName());
    return builder.getOrCreate();
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
    session.sql(
        String.format("CREATE TABLE delta.`%s`(i INT, s STRING) USING delta", location)
            + partitionClause);

    setupTables(catalogName, tableName, DataSourceFormat.DELTA, location, partitionColumns, false);
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
    schemaOperations = new SdkSchemaOperations(createApiClient(serverConfig));
    tableOperations = new SdkTableOperations(createApiClient(serverConfig));
    cleanUp();
  }

  @Override
  protected CatalogOperations createCatalogOperations(ServerConfig serverConfig) {
    return new SdkCatalogOperations(createApiClient(serverConfig));
  }

  @Override
  public void cleanUp() {
    try {
      catalogOperations.deleteCatalog(SPARK_CATALOG, Optional.of(true));
    } catch (Exception e) {
      // Ignore
    }
    super.cleanUp();
    try {
      JavaUtils.deleteRecursively(dataDir);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
