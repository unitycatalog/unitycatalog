package io.unitycatalog.server.sdk.access;

import static io.unitycatalog.server.utils.TestUtils.assertPermissionDenied;
import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.api.SchemasApi;
import io.unitycatalog.client.api.TablesApi;
import io.unitycatalog.client.model.CreateSchema;
import io.unitycatalog.client.model.SchemaInfo;
import io.unitycatalog.client.model.SecurableType;
import io.unitycatalog.client.model.UpdateSchema;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.persist.model.Privileges;
import io.unitycatalog.server.utils.TestUtils;
import java.util.List;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;

/**
 * SDK-based access control tests for Schema CRUD operations.
 *
 * <p>This test class verifies:
 *
 * <ul>
 *   <li>Schema creation requires CREATE SCHEMA permission on catalog
 *   <li>Schema listing is filtered based on permissions
 *   <li>Schema get requires USE SCHEMA permission
 *   <li>Schema update requires ownership
 *   <li>Schema delete requires ownership or catalog ownership
 *   <li>Creating schemas with managed storage requires CREATE MANAGED STORAGE permission
 * </ul>
 */
public class SdkSchemaAccessControlCRUDTest extends SdkAccessControlBaseCRUDTest {

  @Test
  @SneakyThrows
  public void testSchemaAccess() {
    createCommonTestUsers();
    setupCommonCatalogAndSchema();

    // Create API clients for different users
    ServerConfig principal1Config = createTestUserServerConfig(PRINCIPAL_1);
    ServerConfig principal2Config = createTestUserServerConfig(PRINCIPAL_2);
    ServerConfig regular1Config = createTestUserServerConfig(REGULAR_1);
    ServerConfig regular2Config = createTestUserServerConfig(REGULAR_2);

    SchemasApi adminSchemasApi = new SchemasApi(adminApiClient);
    SchemasApi principal1SchemasApi = new SchemasApi(TestUtils.createApiClient(principal1Config));
    SchemasApi principal2SchemasApi = new SchemasApi(TestUtils.createApiClient(principal2Config));
    SchemasApi regular1SchemasApi = new SchemasApi(TestUtils.createApiClient(regular1Config));
    SchemasApi regular2SchemasApi = new SchemasApi(TestUtils.createApiClient(regular2Config));

    // give user USE CATALOG on cat_pr1
    grantPermissions(REGULAR_1, SecurableType.CATALOG, "cat_pr1", Privileges.USE_CATALOG);

    // create a schema (admin) -> metastore admin -> denied
    CreateSchema schemaAdm = new CreateSchema().name("sch_adm").catalogName("cat_pr1");
    assertPermissionDenied(() -> adminSchemasApi.createSchema(schemaAdm));

    // create a schema (regular-1) -> denied
    CreateSchema schemaRg1 = new CreateSchema().name("sch_rg1").catalogName("cat_pr1");
    assertPermissionDenied(() -> regular1SchemasApi.createSchema(schemaRg1));

    // give user CREATE SCHEMA on cat_pr1
    grantPermissions(REGULAR_2, SecurableType.CATALOG, "cat_pr1", Privileges.CREATE_SCHEMA);
    grantPermissions(REGULAR_2, SecurableType.CATALOG, "cat_pr1", Privileges.USE_CATALOG);

    // create a schema (regular-2)
    CreateSchema schemaRg2 = new CreateSchema().name("sch_rg2").catalogName("cat_pr1");
    SchemaInfo schemaRg2Info = regular2SchemasApi.createSchema(schemaRg2);
    assertThat(schemaRg2Info).isNotNull();

    // give user USE SCHEMA on sch_rg2
    grantPermissions(REGULAR_2, SecurableType.SCHEMA, "cat_pr1.sch_rg2", Privileges.USE_SCHEMA);

    // list schemas (admin) -> metastore admin -> allowed - list all
    List<SchemaInfo> adminSchemas = adminSchemasApi.listSchemas("cat_pr1", null, null).getSchemas();
    assertThat(adminSchemas).hasSizeGreaterThanOrEqualTo(2);

    // list schemas (principal-1) -> owner (catalog) -> allowed - list all
    List<SchemaInfo> principal1Schemas =
        principal1SchemasApi.listSchemas("cat_pr1", null, null).getSchemas();
    assertThat(principal1Schemas).hasSizeGreaterThanOrEqualTo(2);

    // list schemas (regular-1) -> -> allowed - empty list
    List<SchemaInfo> regular1Schemas =
        regular1SchemasApi.listSchemas("cat_pr1", null, null).getSchemas();
    assertThat(regular1Schemas).isEmpty();

    // list schemas (regular-2) -> -> USE SCHEMA - filtered list
    List<SchemaInfo> regular2Schemas =
        regular2SchemasApi.listSchemas("cat_pr1", null, null).getSchemas();
    assertThat(regular2Schemas).hasSize(1);
    assertThat(regular2Schemas.get(0).getName()).isEqualTo("sch_rg2");

    // get schema (admin) -> metastore admin -> allowed
    SchemaInfo schemaInfo = adminSchemasApi.getSchema("cat_pr1.sch_pr1");
    assertThat(schemaInfo).isNotNull();

    // get schema (principal-1) -> owner -> allowed
    SchemaInfo schemaInfoOwner = principal1SchemasApi.getSchema("cat_pr1.sch_pr1");
    assertThat(schemaInfoOwner).isNotNull();

    // get schema (regular-1) -> -- -> denied
    assertPermissionDenied(() -> regular1SchemasApi.getSchema("cat_pr1.sch_pr1"));

    // give user USE SCHEMA on sch_rg2
    grantPermissions(REGULAR_2, SecurableType.SCHEMA, "cat_pr1.sch_pr1", Privileges.USE_SCHEMA);

    // get schema (regular-1) -> USE SCHEMA -> allowed
    SchemaInfo schemaInfoRegular2 = regular2SchemasApi.getSchema("cat_pr1.sch_pr1");
    assertThat(schemaInfoRegular2).isNotNull();

    // update schema (admin) -> metastore admin -> allowed
    UpdateSchema updateSchemaAdmin = new UpdateSchema().comment("(admin update)");
    SchemaInfo updatedSchemaAdmin =
        adminSchemasApi.updateSchema("cat_pr1.sch_pr1", updateSchemaAdmin);
    assertThat(updatedSchemaAdmin.getComment()).isEqualTo("(admin update)");

    // update schema (principal-1) -> owner -> allowed
    UpdateSchema updateSchemaOwner = new UpdateSchema().comment("(principal update)");
    SchemaInfo updatedSchemaOwner =
        principal1SchemasApi.updateSchema("cat_pr1.sch_pr1", updateSchemaOwner);
    assertThat(updatedSchemaOwner.getComment()).isEqualTo("(principal update)");

    // update schema (regular-1) -> -- -> denied
    UpdateSchema updateSchemaRegular = new UpdateSchema().comment("(regular update)");
    assertPermissionDenied(
        () -> regular1SchemasApi.updateSchema("cat_pr1.sch_pr1", updateSchemaRegular));

    // delete schema (regular-1) -> -- -> denied
    assertPermissionDenied(() -> regular1SchemasApi.deleteSchema("cat_pr1.sch_pr1", null));

    // delete schema (regular-1) -> "schema" owner -> allowed
    principal1SchemasApi.deleteSchema("cat_pr1.sch_pr1", null);

    // delete schema (regular-1) -> "catalog" owner -> allowed
    principal1SchemasApi.deleteSchema("cat_pr1.sch_rg2", null);

    // Test managed storage with external location

    // Try to create a schema at the location before External Location is created would fail
    CreateSchema schemaWithLoc1 =
        new CreateSchema()
            .name("schema_with_location1")
            .catalogName("cat_pr1")
            .storageRoot("file:///tmp/external_location");
    assertPermissionDenied(() -> principal1SchemasApi.createSchema(schemaWithLoc1));

    // Create the External Location
    createExternalLocationWithCredential("admin_cred", "admin_el", "file:///tmp/external_location");

    // Try to create a schema at the location and still fail due to lack of permission
    assertPermissionDenied(() -> principal1SchemasApi.createSchema(schemaWithLoc1));

    // Grant CREATE MANAGED STORAGE permission
    grantPermissions(
        PRINCIPAL_1,
        SecurableType.EXTERNAL_LOCATION,
        "admin_el",
        Privileges.CREATE_MANAGED_STORAGE);

    // Then the schema using external location as managed storage can be created
    SchemaInfo schemaWithLoc1Info = principal1SchemasApi.createSchema(schemaWithLoc1);
    assertThat(schemaWithLoc1Info).isNotNull();
    assertThat(schemaWithLoc1Info.getName()).isEqualTo("schema_with_location1");

    // Create a table, then a schema under the table. It should fail.
    // First grant CREATE EXTERNAL TABLE permission
    grantPermissions(
        PRINCIPAL_1, SecurableType.EXTERNAL_LOCATION, "admin_el", Privileges.CREATE_EXTERNAL_TABLE);

    TablesApi principal1TablesApi =
        new TablesApi(TestUtils.createApiClient(createTestUserServerConfig(PRINCIPAL_1)));
    createExternalTable(
        principal1TablesApi,
        "cat_pr1",
        "schema_with_location1",
        "tbl_pr1",
        "file:///tmp/external_location/ext_table");

    CreateSchema schemaWithLoc2 =
        new CreateSchema()
            .name("schema_with_location2")
            .catalogName("cat_pr1")
            .storageRoot("file:///tmp/external_location/ext_table");
    assertPermissionDenied(() -> principal1SchemasApi.createSchema(schemaWithLoc2));
  }
}
