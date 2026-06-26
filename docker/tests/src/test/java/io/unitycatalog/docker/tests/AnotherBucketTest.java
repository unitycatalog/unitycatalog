package io.unitycatalog.docker.tests;

import io.unitycatalog.client.model.ColumnInfo;
import java.util.List;
import io.unitycatalog.client.model.DataSourceFormat;
import io.unitycatalog.client.model.PathOperation;
import io.unitycatalog.docker.tests.support.DockerTestConfig;
import io.unitycatalog.docker.tests.support.UcOperations;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;

/**
 * Data-bucket import/export and cross-tenant isolation — parity with anotherBucketTest.sh.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class AnotherBucketTest extends DockerIntegrationTestBase {

  private static final String TENANT_A = "ANOTHERBUCKET";
  private static final String TENANT_B = "ANOTHERBUCKET_B";
  private static final String DATA_TENANT_B_PATH =
      DockerTestConfig.DATA_BUCKET + "/tenant/" + TENANT_B;
  private static final String CATALOG_A = "catanother";
  private static final String CATALOG_B = "catother";
  private static final String SCHEMA = "s1";
  private static final String USER_A = "useranother@example.com";
  private static final String USER_B = "userother@example.com";
  private static final String CRED_A = "cred_anotherbucket";
  private static final String EL_A = "el_catanother";
  private static final String DATA_EL_A = "el_catanother_data";
  private static final String CRED_B = "cred_catother";
  private static final String EL_B = "el_catother";
  private static final String DATA_EL_B = "el_catother_data";
  private static final String IMPORT_TABLE = "t_parquet_import";
  private static final String VIEW_LANDING = "v_landing";
  private static final int LANDING_ROWS = 120;
  private static final String TABLE_CUSTOMERS = "t_customers";
  private static final String TABLE_EVENTS = "t_events";
  private static final String VIEW_CUSTOMERS = "v_customers";
  private static final String VIEW_EVENTS = "v_events";
  private static final int CUSTOMERS_ROWS = 10;
  private static final int EVENTS_ROWS = 25;

  private final long runTs = System.currentTimeMillis() / 1000;
  private final String landingTable = "t_landing_" + runTs;
  private final String landingS3 =
      DockerTestConfig.DATA_BUCKET + "/tenant/" + TENANT_A + "/landing/import_" + runTs;
  private final String exportBase =
      DockerTestConfig.DATA_BUCKET + "/tenant/" + TENANT_A + "/export/run_" + runTs;
  private final String isolationS3 =
      DockerTestConfig.DATA_BUCKET + "/tenant/" + TENANT_B + "/isolation/probe";
  private final String isolationTable = "t_isolation_probe";

  private String tokenA;
  private String tokenB;
  private UcOperations ucA;
  private UcOperations ucB;

  @BeforeAll
  void preflight() throws Exception {
    deleteTenant(CATALOG_A, EL_A, DATA_EL_A, CRED_A);
    deleteTenant(CATALOG_B, EL_B, DATA_EL_B, CRED_B);
  }

  @AfterAll
  void cleanup() throws Exception {
    deleteTenant(CATALOG_A, EL_A, DATA_EL_A, CRED_A);
    deleteTenant(CATALOG_B, EL_B, DATA_EL_B, CRED_B);
  }

  @Test
  @Order(1)
  void bootstrapCatalogA() throws Exception {
    bootstrapTenant(tenantSpec(TENANT_A, CATALOG_A, USER_A, CRED_A, EL_A, DATA_EL_A));
    tokenA = userToken(USER_A, null);
    ucA = new UcOperations(SERVER, tokenA);
    ucA.assertCatalogVisible(CATALOG_A);
  }

  @Test
  @Order(2)
  void createSchemaA() throws Exception {
    ucA.createSchema(CATALOG_A, SCHEMA, "anotherBucket schema");
    ucA.grantUseSchema(CATALOG_A, SCHEMA, USER_A);
  }

  @Test
  @Order(3)
  void seedAndImportLandingDataViaExternalTable() throws Exception {
    ensureSpark(CATALOG_A, tokenA);
    ucA.createExternalTable(
        CATALOG_A, SCHEMA, landingTable, landingS3, DataSourceFormat.PARQUET, UcOperations.landingColumns());
    runSparkSql(
        SCHEMA,
        "INSERT INTO "
            + fq(CATALOG_A, SCHEMA, landingTable)
            + " SELECT cast(id AS int), concat('row-', cast(id AS string)), rand() FROM range("
            + LANDING_ROWS
            + ")");
    assertRowCount(
        runSparkSql(
            SCHEMA, "SELECT count(*) AS row_count FROM " + fq(CATALOG_A, SCHEMA, landingTable)),
        LANDING_ROWS);
    runSparkSql(
        SCHEMA,
        "CREATE TABLE "
            + fq(CATALOG_A, SCHEMA, IMPORT_TABLE)
            + " USING DELTA AS SELECT id, label, value FROM "
            + fq(CATALOG_A, SCHEMA, landingTable));
    assertRowCount(
        runSparkSql(
            SCHEMA, "SELECT count(*) AS row_count FROM " + fq(CATALOG_A, SCHEMA, IMPORT_TABLE)),
        LANDING_ROWS);
  }

  @Test
  @Order(4)
  void seedAndImportLandingDataViaView() throws Exception {
    runSparkSql(
        SCHEMA,
        "CREATE VIEW "
            + fq(CATALOG_A, SCHEMA, VIEW_LANDING)
            + " AS SELECT id, label, value FROM "
            + fq(CATALOG_A, SCHEMA, landingTable));

    runSparkSql(
        SCHEMA,
        "INSERT INTO "
            + fq(CATALOG_A, SCHEMA, VIEW_LANDING)
            + " SELECT cast(id + "
            + LANDING_ROWS
            + " AS int), concat('row-via-view-', cast(id AS string)), rand() FROM range("
            + LANDING_ROWS
            + ")");
    assertRowCount(
        runSparkSql(
            SCHEMA,
            "SELECT count(*) AS row_count FROM " + fq(CATALOG_A, SCHEMA, VIEW_LANDING)),
        LANDING_ROWS * 2L);
    runSparkSql(
        SCHEMA,
        "INSERT INTO "
            + fq(CATALOG_A, SCHEMA, IMPORT_TABLE)
            + " SELECT id, label, value FROM "
            + fq(CATALOG_A, SCHEMA, VIEW_LANDING)
            + " WHERE id >= "
            + LANDING_ROWS);
    assertRowCount(
        runSparkSql(
            SCHEMA, "SELECT count(*) AS row_count FROM " + fq(CATALOG_A, SCHEMA, IMPORT_TABLE)),
        LANDING_ROWS * 2L);

    runSparkSql(SCHEMA, "DROP TABLE " + fq(CATALOG_A, SCHEMA, landingTable));
    ucA.getTable(CATALOG_A, SCHEMA, IMPORT_TABLE);
  }

  @Test
  @Order(5)
  void createAndLoadManagedTables() throws Exception {
    runSparkSql(
        SCHEMA,
        "CREATE TABLE "
            + fq(CATALOG_A, SCHEMA, TABLE_CUSTOMERS)
            + " (customer_id INT, name STRING) USING DELTA");
    runSparkSql(
        SCHEMA,
        "INSERT INTO "
            + fq(CATALOG_A, SCHEMA, TABLE_CUSTOMERS)
            + " SELECT id, concat('customer-', cast(id AS string)) FROM range("
            + CUSTOMERS_ROWS
            + ")");
    runSparkSql(
        SCHEMA,
        "CREATE TABLE "
            + fq(CATALOG_A, SCHEMA, TABLE_EVENTS)
            + " (event_id INT, event_type STRING) USING DELTA");
    runSparkSql(
        SCHEMA,
        "INSERT INTO "
            + fq(CATALOG_A, SCHEMA, TABLE_EVENTS)
            + " SELECT id, concat('event-', cast(id AS string)) FROM range("
            + EVENTS_ROWS
            + ")");
    assertRowCount(
        runSparkSql(
            SCHEMA, "SELECT count(*) AS row_count FROM " + fq(CATALOG_A, SCHEMA, IMPORT_TABLE)),
        LANDING_ROWS * 2L);
    assertRowCount(
        runSparkSql(
            SCHEMA,
            "SELECT count(*) AS row_count FROM " + fq(CATALOG_A, SCHEMA, TABLE_CUSTOMERS)),
        CUSTOMERS_ROWS);
    assertRowCount(
        runSparkSql(
            SCHEMA, "SELECT count(*) AS row_count FROM " + fq(CATALOG_A, SCHEMA, TABLE_EVENTS)),
        EVENTS_ROWS);
  }

  @Test
  @Order(6)
  void createViewsAndLoadViaViews() throws Exception {
    ucA.createView(
        CATALOG_A,
        SCHEMA,
        VIEW_CUSTOMERS,
        CATALOG_A,
        SCHEMA,
        TABLE_CUSTOMERS,
        UcOperations.customerColumns());
    ucA.createView(
        CATALOG_A,
        SCHEMA,
        VIEW_EVENTS,
        CATALOG_A,
        SCHEMA,
        TABLE_EVENTS,
        UcOperations.eventColumns());
    ucA.getView(CATALOG_A, SCHEMA, VIEW_CUSTOMERS);
    ucA.getView(CATALOG_A, SCHEMA, VIEW_EVENTS);

    runSparkSql(
        SCHEMA,
        "INSERT INTO "
            + fq(CATALOG_A, SCHEMA, VIEW_CUSTOMERS)
            + " SELECT id + "
            + CUSTOMERS_ROWS
            + ", concat('customer-via-view-', cast(id AS string)) FROM range("
            + CUSTOMERS_ROWS
            + ")");
    runSparkSql(
        SCHEMA,
        "INSERT INTO "
            + fq(CATALOG_A, SCHEMA, VIEW_EVENTS)
            + " SELECT id + "
            + EVENTS_ROWS
            + ", concat('event-via-view-', cast(id AS string)) FROM range("
            + EVENTS_ROWS
            + ")");

    assertRowCount(
        runSparkSql(
            SCHEMA,
            "SELECT count(*) AS row_count FROM " + fq(CATALOG_A, SCHEMA, VIEW_CUSTOMERS)),
        CUSTOMERS_ROWS * 2L);
    assertRowCount(
        runSparkSql(
            SCHEMA,
            "SELECT count(*) AS row_count FROM " + fq(CATALOG_A, SCHEMA, VIEW_EVENTS)),
        EVENTS_ROWS * 2L);
    assertRowCount(
        runSparkSql(
            SCHEMA,
            "SELECT count(*) AS row_count FROM " + fq(CATALOG_A, SCHEMA, TABLE_CUSTOMERS)),
        CUSTOMERS_ROWS * 2L);
    assertRowCount(
        runSparkSql(
            SCHEMA, "SELECT count(*) AS row_count FROM " + fq(CATALOG_A, SCHEMA, TABLE_EVENTS)),
        EVENTS_ROWS * 2L);
  }

  @Test
  @Order(7)
  void exportToDataBucket() throws Exception {
    exportTable(IMPORT_TABLE, UcOperations.landingColumns(), LANDING_ROWS * 2L);
    exportTable(TABLE_CUSTOMERS, UcOperations.customerColumns(), CUSTOMERS_ROWS * 2L);
    exportTable(TABLE_EVENTS, UcOperations.eventColumns(), EVENTS_ROWS * 2L);
  }

  @Test
  @Order(8)
  void bootstrapCatalogBAndVerifyIsolation() throws Exception {
    bootstrapTenant(tenantSpec(TENANT_B, CATALOG_B, USER_B, CRED_B, EL_B, DATA_EL_B));
    new UcOperations(SERVER, adminToken)
        .assertExternalLocationsPresent(EL_A, DATA_EL_A, EL_B, DATA_EL_B);
    tokenB = userToken(USER_B, null);
    ucB = new UcOperations(SERVER, tokenB);
    ucB.assertCatalogVisibleOnly(CATALOG_B, CATALOG_A);

    ucB.createSchemaIgnoreConflict(CATALOG_B, SCHEMA, "isolation schema");
    ucB.grantUseSchema(CATALOG_B, SCHEMA, USER_B);

    ucB.createExternalTable(
        CATALOG_B,
        SCHEMA,
        isolationTable,
        isolationS3,
        DataSourceFormat.PARQUET,
        UcOperations.idIntColumn());
    runSparkSqlAs(
        CATALOG_B,
        SCHEMA,
        tokenB,
        "INSERT INTO "
            + fq(CATALOG_B, SCHEMA, isolationTable)
            + " SELECT id FROM range(5)");

    ucA.assertPathCredentialsDenied(DATA_TENANT_B_PATH, PathOperation.PATH_READ);
    ucA.assertPathCredentialsDenied(DATA_TENANT_B_PATH, PathOperation.PATH_READ_WRITE);
    ucA.assertPathCredentialsDenied(isolationS3, PathOperation.PATH_READ);
    ucA.assertPathCredentialsDenied(isolationS3, PathOperation.PATH_READ_WRITE);
    assertSparkSqlFails(
        SCHEMA,
        "CREATE EXTERNAL TABLE "
            + fq(CATALOG_A, SCHEMA, "bad_probe")
            + " (id INT) USING PARQUET LOCATION '"
            + isolationS3
            + "'");
    ucB.assertPathReadAllowed(isolationS3);
  }

  private void exportTable(String table, List<ColumnInfo> columns, long expectedRows)
      throws Exception {
    String exportTable = "t_export_" + table;
    String dest = exportBase + "/" + table;
    ucA.createExternalTable(
        CATALOG_A, SCHEMA, exportTable, dest, DataSourceFormat.PARQUET, columns);
    runSparkSql(
        SCHEMA,
        "INSERT OVERWRITE TABLE "
            + fq(CATALOG_A, SCHEMA, exportTable)
            + " SELECT * FROM "
            + fq(CATALOG_A, SCHEMA, table));
    assertRowCount(
        runSparkSql(
            SCHEMA,
            "SELECT count(*) AS row_count FROM " + fq(CATALOG_A, SCHEMA, exportTable)),
        expectedRows);
  }
}
