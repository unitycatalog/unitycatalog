package io.unitycatalog.spark;

import static io.unitycatalog.server.utils.TestUtils.CATALOG_NAME;
import static io.unitycatalog.server.utils.TestUtils.COMMENT;
import static io.unitycatalog.server.utils.TestUtils.SCHEMA_NAME;
import static io.unitycatalog.server.utils.TestUtils.createApiClient;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.ColumnInfo;
import io.unitycatalog.client.model.ColumnTypeName;
import io.unitycatalog.client.model.CreateTable;
import io.unitycatalog.client.model.DataSourceFormat;
import io.unitycatalog.client.model.TableInfo;
import io.unitycatalog.client.model.TableType;
import io.unitycatalog.server.base.table.TableOperations;
import io.unitycatalog.server.sdk.tables.SdkTableOperations;
import io.unitycatalog.spark.utils.OptionsUtil;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class ExternalTableReadWriteTest extends BaseTableReadWriteTest {
  private static final String ANOTHER_PARQUET_TABLE = "test_parquet_another";
  private static final String PARQUET_TABLE_PARTITIONED = "test_parquet_partitioned";
  private static final String DELTA_TABLE = "test_delta";
  private static final String PARQUET_TABLE = "test_parquet";
  private static final String ANOTHER_DELTA_TABLE = "test_delta_another";

  @TempDir private File dataDir;

  // CredentialTestFileSystem provides two test buckets test-bucket0 and test-bucket1.
  // Tests in this class wants to use them alternating. So this variable will be changing between
  // 0 and 1 to construct the bucket name.
  private int bucketIndex = 0;

  private TableOperations tableOperations;

  /**
   * This function provides a set of test parameters that cloud-aware tests should run for this
   * class.
   *
   * @return A stream of Arguments.of(String scheme, boolean renewCredEnabled)
   */
  protected static Stream<Arguments> cloudParameters() {
    return Stream.of(
        Arguments.of("s3", false),
        Arguments.of("s3", true),
        Arguments.of("gs", false),
        Arguments.of("abfs", false),
        Arguments.of("abfs", true));
  }

  @Test
  public void testNoDeltaCatalog() throws IOException, ApiException {
    UCSingleCatalog.LOAD_DELTA_CATALOG().set(false);
    UCSingleCatalog.DELTA_CATALOG_LOADED().set(false);
    SparkSession.Builder builder =
        SparkSession.builder()
            .appName("test")
            .master("local[*]")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension");
    String catalogConf = "spark.sql.catalog.spark_catalog";
    builder =
        builder
            .config(catalogConf, UCSingleCatalog.class.getName())
            .config(catalogConf + "." + OptionsUtil.URI, serverConfig.getServerUrl())
            .config(catalogConf + "." + OptionsUtil.TOKEN, serverConfig.getAuthToken())
            .config(catalogConf + "." + OptionsUtil.WAREHOUSE, CATALOG_NAME);
    session = builder.getOrCreate();
    setupExternalParquetTable(PARQUET_TABLE, new ArrayList<>(0));
    testTableReadWrite("spark_catalog." + SCHEMA_NAME + "." + PARQUET_TABLE);
    assertThat(UCSingleCatalog.DELTA_CATALOG_LOADED().get()).isEqualTo(false);
  }

  @Test
  public void testParquetReadWrite() throws IOException, ApiException {
    session = createSparkSessionWithCatalogs(SPARK_CATALOG);
    // Spark only allow `spark_catalog` to return built-in file source tables.
    setupExternalParquetTable(PARQUET_TABLE, new ArrayList<>(0));
    testTableReadWrite(SPARK_CATALOG + "." + SCHEMA_NAME + "." + PARQUET_TABLE);

    setupExternalParquetTable(PARQUET_TABLE_PARTITIONED, List.of("s"));
    testTableReadWrite(SPARK_CATALOG + "." + SCHEMA_NAME + "." + PARQUET_TABLE_PARTITIONED);
  }

  @Test
  public void testDeltaPathTable() throws IOException {
    // We must replace the `spark_catalog` in order to support Delta path tables.
    session = createSparkSessionWithCatalogs(SPARK_CATALOG);

    String path1 = new File(dataDir, "test_delta_path1").getCanonicalPath();
    String tableName1 = String.format("delta.`%s`", path1);
    sql("CREATE TABLE %s(i INT) USING delta", tableName1);
    assertThat(sql("SELECT * FROM %s", tableName1)).isEmpty();
    sql("INSERT INTO %s SELECT 1", tableName1);
    validateRows(sql("SELECT * FROM %s", tableName1), 1);

    // Test CTAS
    String path2 = new File(dataDir, "test_delta_path2").getCanonicalPath();
    String tableName2 = String.format("delta.`%s`", path2);
    sql("CREATE TABLE %s USING delta AS SELECT 1 AS i", tableName2);
    validateRows(sql("SELECT * FROM %s", tableName2), 1);
  }

  @ParameterizedTest
  @MethodSource("cloudParameters")
  public void testCredentialParquet(String scheme, boolean renewCredEnabled)
      throws ApiException, IOException {
    session = createSparkSessionWithCatalogs(renewCredEnabled, SPARK_CATALOG);

    String loc1 = scheme + "://test-bucket0" + generateTableLocation(SPARK_CATALOG, PARQUET_TABLE);
    setupExternalParquetTable(PARQUET_TABLE, loc1, new ArrayList<>(0));
    String t1 = SPARK_CATALOG + "." + SCHEMA_NAME + "." + PARQUET_TABLE;
    testTableReadWrite(t1);

    String loc2 =
        scheme + "://test-bucket1" + generateTableLocation(SPARK_CATALOG, ANOTHER_PARQUET_TABLE);
    setupExternalParquetTable(ANOTHER_PARQUET_TABLE, loc2, new ArrayList<>(0));
    String t2 = SPARK_CATALOG + "." + SCHEMA_NAME + "." + ANOTHER_PARQUET_TABLE;
    testTableReadWrite(t2);

    validateRows(sql("SELECT l.i FROM %s l JOIN %s r ON l.i = r.i", t1, t2), 1);
  }

  @ParameterizedTest
  @MethodSource("cloudParameters")
  public void testCredentialCreateDeltaTable(String scheme, boolean renewCredEnabled)
      throws IOException {
    session = createSparkSessionWithCatalogs(renewCredEnabled, SPARK_CATALOG, CATALOG_NAME);

    String loc1 = scheme + "://test-bucket0" + generateTableLocation(SPARK_CATALOG, DELTA_TABLE);
    setupDeltaTableLocation(loc1, new ArrayList<>(0));
    String t1 = SPARK_CATALOG + "." + SCHEMA_NAME + "." + DELTA_TABLE;
    sql("CREATE TABLE %s USING delta LOCATION '%s'", t1, loc1);
    testTableReadWrite(t1);

    String loc2 = scheme + "://test-bucket1" + generateTableLocation(CATALOG_NAME, DELTA_TABLE);
    setupDeltaTableLocation(loc2, new ArrayList<>(0));
    String t2 = CATALOG_NAME + "." + SCHEMA_NAME + "." + DELTA_TABLE;
    sql("CREATE TABLE %s USING delta LOCATION '%s'", t2, loc2);
    testTableReadWrite(t2);

    validateRows(sql("SELECT l.i FROM %s l JOIN %s r ON l.i = r.i", t1, t2), 1);

    // Path that does not exist
    String loc3 =
        scheme + "://test-bucket1" + generateTableLocation(CATALOG_NAME, ANOTHER_DELTA_TABLE);
    String t3 = CATALOG_NAME + "." + SCHEMA_NAME + "." + ANOTHER_DELTA_TABLE;
    sql("CREATE TABLE %s(i INT) USING delta LOCATION '%s'", t3, loc3);
    List<Row> rows = session.table(t3).collectAsList();
    assertThat(rows).isEmpty();
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

  @Test
  public void testCreateExternalParquetTable() throws IOException {

    session = createSparkSessionWithCatalogs(SPARK_CATALOG, CATALOG_NAME);
    String[] names = {SPARK_CATALOG, CATALOG_NAME};
    for (String testCatalog : names) {
      String path = generateTableLocation(testCatalog, PARQUET_TABLE);
      String fullTableName = testCatalog + "." + SCHEMA_NAME + "." + PARQUET_TABLE;
      String fullTableName2 = testCatalog + "." + SCHEMA_NAME + "." + ANOTHER_PARQUET_TABLE;

      sql(
          "CREATE TABLE "
              + fullTableName
              + " USING parquet LOCATION '"
              + path
              + "' as SELECT 1, 2, 3");
      assertThat(sql("SELECT * FROM %s", fullTableName)).hasSize(1);
      String path2 = generateTableLocation(testCatalog, ANOTHER_PARQUET_TABLE);
      sql(
          "CREATE TABLE "
              + fullTableName2
              + "(i INT, s STRING) USING PARQUET LOCATION '"
              + path2
              + "'");
      testTableReadWrite(fullTableName2);
    }
  }

  @Test
  public void testCreateExternalDeltaTable() throws ApiException, IOException {
    session = createSparkSessionWithCatalogs(SPARK_CATALOG, CATALOG_NAME);
    String path1 = generateTableLocation(SPARK_CATALOG, DELTA_TABLE);
    String path2 = generateTableLocation(CATALOG_NAME, DELTA_TABLE);
    sql("CREATE TABLE delta.`%s`(name STRING) USING delta", path1);
    sql("CREATE TABLE delta.`%s`(name STRING) USING delta", path2);

    String fullTableName1 = SPARK_CATALOG + "." + SCHEMA_NAME + "." + DELTA_TABLE;
    sql("CREATE TABLE %s(name STRING) USING delta LOCATION '%s'", fullTableName1, path1);
    assertThat(session.catalog().tableExists(fullTableName1)).isTrue();
    TableInfo tableInfo1 = tableOperations.getTable(fullTableName1);
    // By default, Delta tables do not store schema in the catalog.
    assertThat(tableInfo1.getColumns()).isEmpty();
    assertThat(session.table(fullTableName1).collectAsList()).isEmpty();
    StructType schema1 = session.table(fullTableName1).schema();
    assertThat(schema1.apply(0).name()).isEqualTo("name");
    assertThat(schema1.apply(0).dataType()).isEqualTo(DataTypes.StringType);

    String fullTableName2 = CATALOG_NAME + "." + SCHEMA_NAME + "." + DELTA_TABLE;
    sql("CREATE TABLE %s(name STRING) USING delta LOCATION '%s'", fullTableName2, path2);
    assertThat(session.catalog().tableExists(fullTableName2)).isTrue();
    TableInfo tableInfo2 = tableOperations.getTable(fullTableName2);
    // By default, Delta tables do not store schema in the catalog.
    assertThat(tableInfo2.getColumns()).isEmpty();
    assertThat(session.table(fullTableName2).collectAsList()).isEmpty();
    StructType schema2 = session.table(fullTableName2).schema();
    assertThat(schema2.apply(0).name()).isEqualTo("name");
    assertThat(schema2.apply(0).dataType()).isEqualTo(DataTypes.StringType);
  }

  @Test
  public void testCreateExternalTableWithoutLocation() throws IOException {
    session = createSparkSessionWithCatalogs(CATALOG_NAME);

    String fullTableName1 = CATALOG_NAME + "." + SCHEMA_NAME + "." + PARQUET_TABLE;
    assertThatThrownBy(
            () -> {
              sql("CREATE EXTERNAL TABLE %s(name STRING) USING parquet", fullTableName1);
            })
        .hasMessageContaining("Cannot create EXTERNAL TABLE without location");

    String fullTableName2 = CATALOG_NAME + "." + SCHEMA_NAME + "." + DELTA_TABLE;
    assertThatThrownBy(
            () -> {
              sql("CREATE EXTERNAL TABLE %s(name STRING) USING delta", fullTableName2);
            })
        .hasMessageContaining("Cannot create EXTERNAL TABLE without location");
  }

  // TODO: move this into BaseTableReadWriteTest
  @Test
  public void hyphenInTableName() throws ApiException, IOException {
    String catalogName = "test-catalog-name";
    String schemaName = "test-schema-name";
    String tableName = "test-table-name";
    session = createSparkSessionWithCatalogs(SPARK_CATALOG, catalogName);
    sql("CREATE SCHEMA `%s`.`%s`", catalogName, schemaName);
    String fullTableName = String.format("%s.%s.%s", catalogName, schemaName, tableName);
    String location = generateTableLocation(catalogName, tableName);
    sql(
        "CREATE TABLE %s(i INT, s STRING) USING DELTA LOCATION '%s'",
        quoteEntityName(fullTableName), location);

    testTableReadWrite(fullTableName);

    List<Row> tables1 = sql("SHOW TABLES in `%s`.`%s`", catalogName, schemaName);
    assertThat(tables1).hasSize(1);
    assertThat(tables1.get(0).getString(0)).isEqualTo(quoteEntityName(schemaName));
    assertThat(tables1.get(0).getString(1)).isEqualTo(tableName);

    sql("DROP TABLE %s", quoteEntityName(fullTableName));
    List<Row> tables2 = sql("SHOW TABLES in `%s`.`%s`", catalogName, schemaName);
    assertThat(tables2).isEmpty();
  }

  private String generateTableLocation(String catalogName, String tableName) throws IOException {
    return new File(new File(dataDir, catalogName), tableName).getCanonicalPath();
  }

  private void setupDeltaTableLocation(String location, List<String> partitionColumns) {
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
    sql("CREATE TABLE delta.`%s`(i INT, s STRING) USING delta %s", location, partitionClause);
    CredentialTestFileSystem.credentialCheckEnabled = true;
  }

  /**
   * Returns an emulated cloud bucket to be used for creating external table. Alternating between
   * two configured test buckets
   */
  private String testBucket(String scheme) {
    bucketIndex++;
    bucketIndex %= 2;
    return String.format("%s://test-bucket%d", scheme, bucketIndex);
  }

  @Override
  protected String setupDeltaTable(
      String cloudScheme, String catalogName, String tableName, List<String> partitionColumns)
      throws IOException, ApiException {
    String cloudPrefix = cloudScheme.equals("file") ? "" : testBucket(cloudScheme);
    String location = cloudPrefix + generateTableLocation(catalogName, tableName);
    setupDeltaTableLocation(location, partitionColumns);
    setupTables(catalogName, tableName, DataSourceFormat.DELTA, location, partitionColumns, false);
    return String.join(".", catalogName, SCHEMA_NAME, tableName);
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

  @AfterEach
  @Override
  public void cleanUp() {
    UCSingleCatalog.LOAD_DELTA_CATALOG().set(true);
    super.cleanUp();
  }
}
