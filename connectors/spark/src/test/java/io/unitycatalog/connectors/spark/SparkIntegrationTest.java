package io.unitycatalog.connectors.spark;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.*;
import io.unitycatalog.server.base.BaseCRUDTest;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.catalog.CatalogOperations;
import io.unitycatalog.server.base.schema.SchemaOperations;
import io.unitycatalog.server.base.table.TableOperations;
import io.unitycatalog.server.persist.dao.ColumnInfoDAO;
import io.unitycatalog.server.persist.dao.TableInfoDAO;
import io.unitycatalog.server.persist.utils.HibernateUtils;
import io.unitycatalog.server.sdk.catalog.SdkCatalogOperations;
import io.unitycatalog.server.sdk.schema.SdkSchemaOperations;
import io.unitycatalog.server.sdk.tables.SdkTableOperations;
import io.unitycatalog.server.utils.TestUtils;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.spark.network.util.JavaUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.QueryPlanningTracker;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.apache.spark.sql.execution.*;
import org.apache.spark.sql.execution.command.DataWritingCommandExec;
import org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.*;

import static io.unitycatalog.server.utils.TestUtils.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
        SparkSession session = createSparkSessionWithCatalogs(SPARK_CATALOG);
        // Spark only allow `spark_catalog` to return built-in file source tables.
        setupExternalParquetTable(PARQUET_TABLE, new ArrayList<>(0), session);
        testTableReadWrite(SPARK_CATALOG + "." + SCHEMA_NAME + "." + PARQUET_TABLE, session);

        setupExternalParquetTable(PARQUET_TABLE_PARTITIONED, Arrays.asList("s"), session);
        testTableReadWrite(SPARK_CATALOG + "." + SCHEMA_NAME + "." + PARQUET_TABLE_PARTITIONED, session);

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
        assertThat(session.sql("SELECT * FROM " + tableName1).collectAsList()).isEmpty();
        session.sql("INSERT INTO " + tableName1 + " SELECT 1");
        assertEquals(1,
                session.sql("SELECT * FROM " + tableName1).collectAsList().get(0).getInt(0));

        // Test CTAS
        String path2 = new File(dataDir, "test_delta_path2").getCanonicalPath();
        String tableName2 = String.format("delta.`%s`", path2);
        session.sql(String.format("CREATE TABLE %s USING delta AS SELECT 1 AS i", tableName2));
        assertEquals(1,
                session.sql("SELECT * FROM " + tableName2).collectAsList().get(0).getInt(0));

        session.stop();
    }

    @Test
    public void testManagedParquetTable() throws ApiException, IOException, ParseException {
        createCommonResources();
        SparkSession session = createSparkSessionWithCatalogs(SPARK_CATALOG);
        // Spark only allow `spark_catalog` to return built-in file source tables.
        setupManagedParquetTable(PARQUET_TABLE, new ArrayList<>(0), session);
        String tableFullName = SPARK_CATALOG + "." + SCHEMA_NAME + "." + PARQUET_TABLE;

        Dataset<Row> df = session.sql("SELECT * FROM " + tableFullName);
        FileSourceScanExec scan = (FileSourceScanExec) df.queryExecution().executedPlan().collectLeaves().head();
        assertEquals("test1", scan.relation().options().apply("fs.s3a.access.key"));
        assertEquals("test2", scan.relation().options().apply("fs.s3a.secret.key"));
        assertEquals("test3", scan.relation().options().apply("fs.s3a.session.token"));

        DataWritingCommandExec cmdExec = (DataWritingCommandExec) new QueryExecution(
                session,
                session.sessionState().sqlParser().parsePlan("INSERT INTO " + tableFullName + " SELECT 1, 'a'"),
                new QueryPlanningTracker(scala.Option.apply(null)),
                CommandExecutionMode.NON_ROOT(),
                DoNotCleanup$.MODULE$
        ).executedPlan();
        InsertIntoHadoopFsRelationCommand cmd = (InsertIntoHadoopFsRelationCommand) cmdExec.cmd();
        assertEquals("test1", cmd.options().apply("fs.s3a.access.key"));
        assertEquals("test2", cmd.options().apply("fs.s3a.secret.key"));
        assertEquals("test3", cmd.options().apply("fs.s3a.session.token"));

        session.stop();
    }

    // TODO: enable the Delta Lake test once UC server supports managed table. We have to disable it
    //       for now as we can only test with a fake table location but Delta table needs a real
    //       Delta path.
    // @Test
    public void testManagedDeltaTable() throws ApiException, IOException, ParseException {
        createCommonResources();
        SparkSession session = createSparkSessionWithCatalogs(SPARK_CATALOG, CATALOG_NAME);
        setupManagedDeltaTable(CATALOG_NAME, DELTA_TABLE, new ArrayList<>(0), session);
        String tableFullName = CATALOG_NAME + "." + SCHEMA_NAME + "." + DELTA_TABLE;

        Dataset<Row> df = session.sql("SELECT * FROM " + tableFullName);
        FileSourceScanExec scan = (FileSourceScanExec) df.queryExecution().executedPlan().collectLeaves().head();
        assertEquals("test1", scan.relation().options().apply("fs.s3a.access.key"));
        assertEquals("test2", scan.relation().options().apply("fs.s3a.secret.key"));
        assertEquals("test3", scan.relation().options().apply("fs.s3a.session.token"));

        DataWritingCommandExec cmdExec = (DataWritingCommandExec) new QueryExecution(
                session,
                session.sessionState().sqlParser().parsePlan("INSERT INTO " + tableFullName + " SELECT 1, 'a'"),
                new QueryPlanningTracker(scala.Option.apply(null)),
                CommandExecutionMode.NON_ROOT(),
                DoNotCleanup$.MODULE$
        ).executedPlan();
        InsertIntoHadoopFsRelationCommand cmd = (InsertIntoHadoopFsRelationCommand) cmdExec.cmd();
        assertEquals("test1", cmd.options().apply("fs.s3a.access.key"));
        assertEquals("test2", cmd.options().apply("fs.s3a.secret.key"));
        assertEquals("test3", cmd.options().apply("fs.s3a.session.token"));

        session.stop();
    }

    private void testTableReadWrite(String tableFullName, SparkSession session) {
        assertTrue(
                session.sql("SELECT * FROM " + tableFullName).collectAsList().isEmpty());
        session.sql("INSERT INTO " + tableFullName + " SELECT 1, 'a'");
        Row row = session.sql("SELECT * FROM " + tableFullName).collectAsList().get(0);
        assertEquals(1, row.getInt(0));
        assertEquals("a", row.getString(1));
    }

    private SchemaOperations schemaOperations;
    private TableOperations tableOperations;
    private String schemaId1 = null;
    private String schemaId2 = null;

    private void createCommonResources() throws ApiException {
        // Common setup operations such as creating a catalog and schema
        catalogOperations.createCatalog(new CreateCatalog()
                .name(TestUtils.CATALOG_NAME)
                .comment(TestUtils.COMMENT));
        SchemaInfo schemaInfo1 = schemaOperations.createSchema(
                new CreateSchema().name(SCHEMA_NAME).catalogName(CATALOG_NAME));
        schemaId1 = schemaInfo1.getSchemaId();
        catalogOperations.createCatalog(new CreateCatalog()
                .name(SPARK_CATALOG)
                .comment("Spark catalog"));
        SchemaInfo schemaInfo2 = schemaOperations.createSchema(
                new CreateSchema().name(SCHEMA_NAME).catalogName(SPARK_CATALOG));
        schemaId2 = schemaInfo2.getSchemaId();
    }

    private SparkSession createSparkSessionWithCatalogs(String... catalogs) {
        SparkSession.Builder builder = SparkSession.builder().appName("test").master("local[*]")
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension");
        for (String catalog : catalogs) {
            String catalogConf = "spark.sql.catalog." + catalog;
            builder = builder.config(catalogConf, UCSingleCatalog.class.getName())
                    .config(catalogConf + ".uri", serverConfig.getServerUrl())
                    .config(catalogConf + ".token", serverConfig.getAuthToken());
        }
        // Use fake file system for s3:// so that test can run.
        builder.config("fs.s3.impl", FakeFileSystem.class.getName());
        return builder.getOrCreate();
    }

    private void setupExternalParquetTable(
            String tableName,
            List<String> partitionColumns,
            SparkSession session) throws IOException, ApiException {
        setupParquetTable(tableName, partitionColumns, false, session);
    }

    private void setupManagedParquetTable(
            String tableName,
            List<String> partitionColumns,
            SparkSession session) throws IOException, ApiException {
        setupParquetTable(tableName, partitionColumns, true, session);
    }

    private void setupParquetTable(
            String tableName,
            List<String> partitionColumns,
            boolean isManaged,
            SparkSession session) throws IOException, ApiException {
        setupTables(
                SPARK_CATALOG,
                tableName,
                DataSourceFormat.PARQUET,
                partitionColumns,
                isManaged,
                session);
    }

    private void setupExternalDeltaTable(
            String catalogName,
            String tableName,
            List<String> partitionColumns,
            SparkSession session) throws IOException, ApiException {
        setupDeltaTable(catalogName, tableName, partitionColumns, false, session);
    }

    private void setupManagedDeltaTable(
            String catalogName,
            String tableName,
            List<String> partitionColumns,
            SparkSession session) throws IOException, ApiException {
        setupDeltaTable(catalogName, tableName, partitionColumns, true, session);
    }

    private void setupDeltaTable(
            String catalogName,
            String tableName,
            List<String> partitionColumns,
            boolean isManaged,
            SparkSession session) throws IOException, ApiException {
        setupTables(
                catalogName,
                tableName,
                DataSourceFormat.DELTA,
                partitionColumns,
                isManaged,
                session);
    }
    private void setupTables(
            String catalogName,
            String tableName,
            DataSourceFormat format,
            List<String> partitionColumns,
            boolean isManaged,
            SparkSession session) throws IOException, ApiException {
        Integer partitionIndex1 = partitionColumns.indexOf("i");
        if (partitionIndex1 == -1) partitionIndex1 = null;
        Integer partitionIndex2 = partitionColumns.indexOf("s");
        if (partitionIndex2 == -1) partitionIndex2 = null;

        // TODO: remove this when managed table is officially supported.
        if (isManaged) {
            Session s = HibernateUtils.getSessionFactory().openSession();
            Transaction tx = s.beginTransaction();

            UUID tableId = UUID.randomUUID();
            String schemaId;
            if (catalogName.equals(CATALOG_NAME)) {
                schemaId = schemaId1;
            } else {
                schemaId = schemaId2;
            }

            TableInfoDAO managedTableInfo =
                    TableInfoDAO.builder()
                            .name(tableName)
                            .schemaId(UUID.fromString(schemaId))
                            .comment(TestUtils.COMMENT)
                            .url("s3://test-bucket")
                            .type(TableType.MANAGED.name())
                            .dataSourceFormat(format.name())
                            .id(tableId)
                            .createdAt(new Date())
                            .updatedAt(new Date())
                            .build();

            ColumnInfoDAO columnInfoDAO1 =
                    ColumnInfoDAO.builder()
                            .name("i")
                            .typeText("INTEGER")
                            .typeJson("{\"type\": \"integer\"}")
                            .typeName(ColumnTypeName.INT.name())
                            .typePrecision(10)
                            .typeScale(0)
                            .ordinalPosition((short) 0)
                            .comment("Integer column")
                            .nullable(true)
                            .table(managedTableInfo)
                            .build();

            ColumnInfoDAO columnInfoDAO2 =
                    ColumnInfoDAO.builder()
                            .name("s")
                            .typeText("STRING")
                            .typeJson("{\"type\": \"string\"}")
                            .typeName(ColumnTypeName.STRING.name())
                            .ordinalPosition((short) 1)
                            .comment("String column")
                            .nullable(true)
                            .table(managedTableInfo)
                            .build();

            managedTableInfo.setColumns(Arrays.asList(columnInfoDAO1, columnInfoDAO2));

            s.persist(managedTableInfo);
            s.flush();
            tx.commit();

            return;
        }

        ColumnInfo c1 = new ColumnInfo().name("i").typeText("INTEGER")
                .typeJson("{\"type\": \"integer\"}")
                .typeName(ColumnTypeName.INT).typePrecision(10).typeScale(0).position(0)
                .partitionIndex(partitionIndex1)
                .comment("Integer column")
                .nullable(true);

        ColumnInfo c2 = new ColumnInfo().name("s").typeText("STRING")
                .typeJson("{\"type\": \"string\"}")
                .typeName(ColumnTypeName.STRING).position(1)
                .partitionIndex(partitionIndex2)
                .comment("String column")
                .nullable(true);
        String location = new File(new File(dataDir, catalogName), tableName).getCanonicalPath();
        if (!isManaged && format == DataSourceFormat.DELTA) {
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
        TableType tableType;
        if (isManaged) {
            tableType = TableType.MANAGED;
        } else {
            tableType = TableType.EXTERNAL;
        }
        CreateTable createTableRequest = new CreateTable()
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

class FakeFileSystem extends RawLocalFileSystem {
    @Override
    public URI getUri() {
        return URI.create("s3://test-bucket");
    }
}
