package io.unitycatalog.connectors.spark;

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
import static io.unitycatalog.server.utils.TestUtils.*;
import org.apache.spark.network.util.JavaUtils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SparkIntegrationTest extends BaseCRUDTest {

    private static final String SPARK_CATALOG = "spark_catalog";
    private static final String PARQUET_TABLE = "test_parquet";
    private static final String PARQUET_TABLE_PARTITIONED = "test_parquet_partitioned";
    private static final String DELTA_TABLE = "test_delta";
    private static final String DELTA_TABLE_PARTITIONED = "test_delta_partitioned";


    final private File dataDir = new File(System.getProperty("java.io.tmpdir"), "spark_test");

    @Test
    public void testParquetReadWrite() throws IOException, ApiException {
        createCommonResources();
        String catalogConf = "spark.sql.catalog." + SPARK_CATALOG;
        SparkSession session = SparkSession.builder().appName("test").master("local[*]")
                .config(catalogConf, UCSingleCatalog.class.getName())
                .config(catalogConf + ".uri", serverConfig.getServerUrl())
                .config(catalogConf + ".token", serverConfig.getAuthToken())
                .getOrCreate();
        // Spark only allow `spark_catalog` to return built-in file source tables.
        setupParquetTable(PARQUET_TABLE, new ArrayList<>(0), session);
        testTableReadWrite(SPARK_CATALOG + "." + SCHEMA_NAME + "." + PARQUET_TABLE, session);

        setupParquetTable(PARQUET_TABLE_PARTITIONED, Arrays.asList("s"), session);
        testTableReadWrite(SPARK_CATALOG + "." + SCHEMA_NAME + "." + PARQUET_TABLE_PARTITIONED, session);

        session.stop();
    }

    @Test
    public void testDeltaReadWrite() throws IOException, ApiException {
        createCommonResources();
        // Test both `spark_catalog` and other catalog names.
        String catalogConf1 = "spark.sql.catalog." + SPARK_CATALOG;
        String catalogConf2 = "spark.sql.catalog." + CATALOG_NAME;
        SparkSession session = SparkSession.builder().appName("test").master("local[*]")
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config(catalogConf1, UCSingleCatalog.class.getName())
                .config(catalogConf1 + ".uri", serverConfig.getServerUrl())
                .config(catalogConf1 + ".token", serverConfig.getAuthToken())
                .config(catalogConf2, UCSingleCatalog.class.getName())
                .config(catalogConf2 + ".uri", serverConfig.getServerUrl())
                .config(catalogConf2 + ".token", serverConfig.getAuthToken())
                .getOrCreate();

        setupDeltaTable(SPARK_CATALOG, DELTA_TABLE, new ArrayList<>(0), session);
        testTableReadWrite(SPARK_CATALOG + "." + SCHEMA_NAME + "." + DELTA_TABLE, session);

        setupDeltaTable(SPARK_CATALOG, DELTA_TABLE_PARTITIONED, Arrays.asList("s"), session);
        testTableReadWrite(SPARK_CATALOG + "." + SCHEMA_NAME + "." + DELTA_TABLE_PARTITIONED, session);

        setupDeltaTable(CATALOG_NAME, DELTA_TABLE, new ArrayList<>(0), session);
        testTableReadWrite(CATALOG_NAME + "." + SCHEMA_NAME + "." + DELTA_TABLE, session);

        setupDeltaTable(CATALOG_NAME, DELTA_TABLE_PARTITIONED, Arrays.asList("s"), session);
        testTableReadWrite(CATALOG_NAME + "." + SCHEMA_NAME + "." + DELTA_TABLE_PARTITIONED, session);

        session.stop();
    }

    @Test
    public void testDeltaPathTable() throws IOException, ApiException {
        createCommonResources();
        // We must replace the `spark_catalog` in order to support Delta path tables.
        String catalogConf = "spark.sql.catalog." + SPARK_CATALOG;
        SparkSession session = SparkSession.builder().appName("test").master("local[*]")
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config(catalogConf, UCSingleCatalog.class.getName())
                .config(catalogConf + ".uri", serverConfig.getServerUrl())
                .config(catalogConf + ".token", serverConfig.getAuthToken())
                .getOrCreate();

        String path1 = new File(dataDir, "test_delta_path1").getCanonicalPath();
        String tableName1 = String.format("delta.`%s`", path1);
        session.sql(String.format("CREATE TABLE %s(i INT) USING delta", tableName1));
        Assert.assertTrue(
                session.sql("SELECT * FROM " + tableName1).collectAsList().isEmpty());
        session.sql("INSERT INTO " + tableName1 + " SELECT 1");
        Assert.assertEquals(1,
                session.sql("SELECT * FROM " + tableName1).collectAsList().get(0).getInt(0));

        // Test CTAS
        String path2 = new File(dataDir, "test_delta_path2").getCanonicalPath();
        String tableName2 = String.format("delta.`%s`", path2);
        session.sql(String.format("CREATE TABLE %s USING delta AS SELECT 1 AS i", tableName2));
        Assert.assertEquals(1,
                session.sql("SELECT * FROM " + tableName2).collectAsList().get(0).getInt(0));

        session.stop();
    }

    private void testTableReadWrite(String tableFullName, SparkSession session) {
        Assert.assertTrue(
                session.sql("SELECT * FROM " + tableFullName).collectAsList().isEmpty());
        session.sql("INSERT INTO " + tableFullName + " SELECT 1, 'a'");
        Row row = session.sql("SELECT * FROM " + tableFullName).collectAsList().get(0);
        Assert.assertEquals(1, row.getInt(0));
        Assert.assertEquals("a", row.getString(1));
    }

    private SchemaOperations schemaOperations;
    private TableOperations tableOperations;

    private void createCommonResources() throws ApiException {
        // Common setup operations such as creating a catalog and schema
        catalogOperations.createCatalog(CATALOG_NAME, "Common catalog for tables");
        schemaOperations.createSchema(new CreateSchema().name(SCHEMA_NAME).catalogName(CATALOG_NAME));
        catalogOperations.createCatalog(SPARK_CATALOG, "Spark catalog");
        schemaOperations.createSchema(new CreateSchema().name(SCHEMA_NAME).catalogName(SPARK_CATALOG));
    }

    private void setupParquetTable(
            String tableName,
            List<String> partitionColumns,
            SparkSession session) throws IOException, ApiException {
        setupTables(
                SPARK_CATALOG,
                SCHEMA_NAME,
                tableName,
                DataSourceFormat.PARQUET,
                partitionColumns,
                session);
    }

    private void setupDeltaTable(
            String catalogName,
            String tableName,
            List<String> partitionColumns,
            SparkSession session) throws IOException, ApiException {
        setupTables(
                catalogName,
                SCHEMA_NAME,
                tableName,
                DataSourceFormat.DELTA,
                partitionColumns,
                session);
    }
    private void setupTables(
            String catalogName,
            String schemaName,
            String tableName,
            DataSourceFormat format,
            List<String> partitionColumns,
            SparkSession session) throws IOException, ApiException {
        Integer partitionIndex1 = partitionColumns.indexOf("i");
        if (partitionIndex1 == -1) partitionIndex1 = null;
        ColumnInfo c1 = new ColumnInfo().name("i").typeText("INTEGER")
                .typeJson("{\"type\": \"integer\"}")
                .typeName(ColumnTypeName.INT).typePrecision(10).typeScale(0).position(0)
                .partitionIndex(partitionIndex1)
                .comment("Integer column")
                .nullable(true);
        Integer partitionIndex2 = partitionColumns.indexOf("s");
        if (partitionIndex2 == -1) partitionIndex2 = null;
        ColumnInfo c2 = new ColumnInfo().name("s").typeText("STRING")
                .typeJson("{\"type\": \"string\"}")
                .typeName(ColumnTypeName.STRING).position(1)
                .partitionIndex(partitionIndex2)
                .comment("String column")
                .nullable(true);
        String location = new File(new File(dataDir, catalogName), tableName).getCanonicalPath();
        if (format == DataSourceFormat.DELTA) {
            // The Delta path can't be empty, need to set up here.
            String partitionClause;
            if (partitionColumns.isEmpty()) {
                partitionClause = "";
            } else {
                partitionClause = String.format(
                        " PARTITIONED BY (%s)",
                        String.join(", ", partitionColumns));
            }
            session.sql(String.format("CREATE TABLE delta.`%s`(i INT, s STRING) USING delta", location) + partitionClause);
        }
        CreateTable createTableRequest = new CreateTable()
                .name(tableName)
                .catalogName(catalogName)
                .schemaName(schemaName)
                .columns(Arrays.asList(c1, c2))
                .comment(COMMENT)
                .storageLocation(location)
                .tableType(TableType.EXTERNAL)
                .dataSourceFormat(format);
        tableOperations.createTable(createTableRequest);
    }

    @Before
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
    protected void cleanUp() {
        deleteCatalog(CATALOG_NAME);
        deleteCatalog(SPARK_CATALOG);
        try {
            JavaUtils.deleteRecursively(dataDir);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        super.cleanUp();
    }

    private void deleteCatalog(String catalogName) {
        String schemaFullName = catalogName + "." + SCHEMA_NAME;
        deleteTable(schemaFullName + "." + PARQUET_TABLE);
        deleteTable(schemaFullName + "." + DELTA_TABLE);
        try {
            schemaOperations.deleteSchema(schemaFullName);
        } catch (Exception e) {
            // Ignore
        }
        try {
            catalogOperations.deleteCatalog(catalogName);
        } catch (Exception e) {
            // Ignore
        }
    }

    private void deleteTable(String tableFullName) {
        try {
            tableOperations.deleteTable(tableFullName);
        } catch (Exception e) {
            // Ignore
        }
    }
}
