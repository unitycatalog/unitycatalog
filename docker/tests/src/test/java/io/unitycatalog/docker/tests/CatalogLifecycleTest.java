package io.unitycatalog.docker.tests;

import io.unitycatalog.client.model.DataSourceFormat;
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
 * Catalog/metadata lifecycle (Delta tables, REST metadata, Spark DML) — parity with catalogTest.sh.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class CatalogLifecycleTest extends DockerIntegrationTestBase {

  private static final String TENANT_ID = "CATTEST";
  private static final String TENANT_PATH = DockerTestConfig.BUCKET + "/tenant/" + TENANT_ID;
  private static final String CATALOG = "cattest";
  private static final String SCHEMA = "s1";
  private static final String USER_EMAIL = "usercattest@example.com";
  private static final String CREDENTIAL = "cred_cattest";
  private static final String EL = "el_cattest";
  private static final String DATA_EL = "el_cattest_data";
  private static final String MANAGED_TABLE = "t_managed_ct";
  private static final String EXTERNAL_TABLE = "t_external";
  private static final String VIEW = "v_alias";

  private String userToken;
  private UcOperations uc;

  @BeforeAll
  void setup() throws Exception {
    deleteTenant(CATALOG, EL, DATA_EL, CREDENTIAL);
    bootstrapTenant(tenantSpec(TENANT_ID, CATALOG, USER_EMAIL, CREDENTIAL, EL, DATA_EL));
    userToken = userToken(USER_EMAIL, null);
    uc = new UcOperations(SERVER, userToken);
  }

  @AfterAll
  void cleanup() throws Exception {
    deleteTenant(CATALOG, EL, DATA_EL, CREDENTIAL);
  }

  @Test
  @Order(1)
  void createSchemaAndGrantUseSchema() throws Exception {
    uc.createSchema(CATALOG, SCHEMA, "catalogTest schema");
    uc.grantUseSchema(CATALOG, SCHEMA, USER_EMAIL);
    uc.getSchema(CATALOG, SCHEMA);
    uc.assertCreateSchemaConflict(CATALOG, SCHEMA);
  }

  @Test
  @Order(2)
  void createTables() throws Exception {
    ensureSpark(CATALOG, userToken);
    runSparkSql(
        SCHEMA,
        "CREATE TABLE "
            + fq(CATALOG, SCHEMA, MANAGED_TABLE)
            + " (id INT, name STRING) USING DELTA");
    uc.createExternalTable(
        CATALOG,
        SCHEMA,
        EXTERNAL_TABLE,
        TENANT_PATH + "/ext/" + EXTERNAL_TABLE,
        DataSourceFormat.DELTA,
        UcOperations.idIntColumn());
    uc.getTable(CATALOG, SCHEMA, MANAGED_TABLE);
    uc.assertTableFormatDelta(CATALOG, SCHEMA, MANAGED_TABLE);
  }

  @Test
  @Order(3)
  void createView() throws Exception {
    uc.createView(
        CATALOG,
        SCHEMA,
        VIEW,
        CATALOG,
        SCHEMA,
        MANAGED_TABLE,
        UcOperations.viewColumns());
    uc.getView(CATALOG, SCHEMA, VIEW);
  }

  @Test
  @Order(4)
  void listMetadata() throws Exception {
    uc.assertListed("schemas", CATALOG, SCHEMA, SCHEMA);
    uc.assertListed("tables", CATALOG, SCHEMA, MANAGED_TABLE);
    uc.assertListed("tables", CATALOG, SCHEMA, EXTERNAL_TABLE);
    uc.assertListed("views", CATALOG, SCHEMA, VIEW);
  }

  @Test
  @Order(5)
  void dataMovement() throws Exception {
    String fq = fq(CATALOG, SCHEMA, MANAGED_TABLE);
    runSparkSql(SCHEMA, "INSERT INTO " + fq + " VALUES (1,'a'),(2,'b')");
    runSparkSql(
        SCHEMA,
        "MERGE INTO "
            + fq
            + " t USING (SELECT 1 AS id, 'a2' AS name) s ON t.id=s.id "
            + "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *");
    uc.createExternalTable(
        CATALOG,
        SCHEMA,
        "t_export_" + MANAGED_TABLE,
        TENANT_PATH + "/export/" + MANAGED_TABLE,
        DataSourceFormat.PARQUET,
        UcOperations.idNameColumns());
    runSparkSql(
        SCHEMA,
        "INSERT OVERWRITE TABLE "
            + fq(CATALOG, SCHEMA, "t_export_" + MANAGED_TABLE)
            + " SELECT * FROM "
            + fq);
    runSparkSql(SCHEMA, "DELETE FROM " + fq + " WHERE id=2");
  }

  @Test
  @Order(6)
  void drops() throws Exception {
    uc.deleteView(CATALOG, SCHEMA, VIEW);
    uc.assertNotListed("views", CATALOG, SCHEMA, VIEW);
    uc.deleteTable(CATALOG, SCHEMA, EXTERNAL_TABLE);
    uc.deleteTable(CATALOG, SCHEMA, MANAGED_TABLE);
    uc.assertNotListed("tables", CATALOG, SCHEMA, MANAGED_TABLE);
    uc.createExternalTable(
        CATALOG,
        SCHEMA,
        "t_cascade",
        TENANT_PATH + "/ext/t_cascade",
        DataSourceFormat.DELTA,
        UcOperations.idIntColumn());
    uc.assertDeleteSchemaRejected(CATALOG, SCHEMA);
    uc.deleteSchema(CATALOG, SCHEMA, true);
    uc.assertNotListed("schemas", CATALOG, SCHEMA, SCHEMA);
  }
}
