package io.unitycatalog.server.sdk.access;

import static io.unitycatalog.server.utils.TestUtils.assertHttpApiException;
import static io.unitycatalog.server.utils.TestUtils.assertPermissionDenied;
import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.api.CatalogsApi;
import io.unitycatalog.client.api.SchemasApi;
import io.unitycatalog.client.api.TablesApi;
import io.unitycatalog.client.model.CatalogInfo;
import io.unitycatalog.client.model.CreateCatalog;
import io.unitycatalog.client.model.CreateSchema;
import io.unitycatalog.client.model.SchemaInfo;
import io.unitycatalog.client.model.SecurableType;
import io.unitycatalog.client.model.TableInfo;
import io.unitycatalog.client.model.UpdateCatalog;
import io.unitycatalog.server.auth.decorator.UnityAccessDecorator;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.persist.model.Privileges;
import io.unitycatalog.server.utils.TestUtils;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.Optional;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;

/**
 * SDK-based access control tests for Catalog CRUD operations.
 *
 * <p>This test class verifies:
 *
 * <ul>
 *   <li>Catalog creation requires CREATE CATALOG permission on metastore
 *   <li>Catalog listing is filtered based on permissions
 *   <li>Catalog get requires USE CATALOG permission
 *   <li>Catalog update requires ownership or appropriate permissions
 *   <li>Catalog delete requires ownership or metastore admin
 *   <li>Creating catalogs with managed storage requires CREATE MANAGED STORAGE permission
 * </ul>
 */
public class SdkCatalogAccessControlCRUDTest extends SdkAccessControlBaseCRUDTest {

  @Test
  @SneakyThrows
  public void testCatalogAccess() {
    createCommonTestUsers();

    // Create API clients for different users
    ServerConfig principal1Config = createTestUserServerConfig(PRINCIPAL_1);
    ServerConfig principal2Config = createTestUserServerConfig(PRINCIPAL_2);
    ServerConfig regular1Config = createTestUserServerConfig(REGULAR_1);

    CatalogsApi adminCatalogsApi = new CatalogsApi(adminApiClient);
    CatalogsApi principal1CatalogsApi =
        new CatalogsApi(TestUtils.createApiClient(principal1Config));
    CatalogsApi principal2CatalogsApi =
        new CatalogsApi(TestUtils.createApiClient(principal2Config));
    CatalogsApi regular1CatalogsApi = new CatalogsApi(TestUtils.createApiClient(regular1Config));
    SchemasApi adminSchemasApi = new SchemasApi(adminApiClient);

    // create a catalog -> metastore admin -> allowed
    CreateCatalog adminCatalog1 =
        new CreateCatalog().name("admincatalog1").comment("(created from scratch)");
    CatalogInfo adminCatalog1Info = adminCatalogsApi.createCatalog(adminCatalog1);
    assertThat(adminCatalog1Info).isNotNull();
    assertThat(adminCatalog1Info.getName()).isEqualTo("admincatalog1");

    // create default schema for admincatalog1
    CreateSchema defaultSchema = new CreateSchema().name("default").catalogName("admincatalog1");
    adminSchemasApi.createSchema(defaultSchema);

    // give user CREATE CATALOG
    grantPermissions(
        PRINCIPAL_1, SecurableType.METASTORE, METASTORE_NAME, Privileges.CREATE_CATALOG);

    // create a catalog -> CREATE CATALOG -> allowed
    CreateCatalog catalog1 = new CreateCatalog().name("catalog1").comment("(created from scratch)");
    CatalogInfo catalog1Info = principal1CatalogsApi.createCatalog(catalog1);
    assertThat(catalog1Info).isNotNull();
    assertThat(catalog1Info.getName()).isEqualTo("catalog1");

    // create a catalog -> -- -> denied
    CreateCatalog catalog2 = new CreateCatalog().name("catalog2").comment("(created from scratch)");
    assertPermissionDenied(() -> principal2CatalogsApi.createCatalog(catalog2));

    // list catalogs (admin) -> metastore admin -> allowed - list all
    List<CatalogInfo> adminCatalogs = adminCatalogsApi.listCatalogs(null, null).getCatalogs();
    assertThat(adminCatalogs).hasSizeGreaterThanOrEqualTo(2);
    assertThat(adminCatalogs.stream().map(CatalogInfo::getName))
        .contains("admincatalog1", "catalog1");

    // list catalogs (principal-1) -> owner -> allowed - list owning
    List<CatalogInfo> principal1Catalogs =
        principal1CatalogsApi.listCatalogs(null, null).getCatalogs();
    assertThat(principal1Catalogs).hasSize(1);
    assertThat(principal1Catalogs.get(0).getName()).isEqualTo("catalog1");

    // give user USE CATALOG on catalog1
    grantPermissions(REGULAR_1, SecurableType.CATALOG, "catalog1", Privileges.USE_CATALOG);

    // list catalogs (regular-1) -> USE CATALOG -> allowed - list filtered
    List<CatalogInfo> regular1Catalogs = regular1CatalogsApi.listCatalogs(null, null).getCatalogs();
    assertThat(regular1Catalogs).hasSize(1);
    assertThat(regular1Catalogs.get(0).getName()).isEqualTo("catalog1");

    // get catalog (admin) should be able to get any catalog
    CatalogInfo getCatalog1 = adminCatalogsApi.getCatalog("catalog1");
    assertThat(getCatalog1).isNotNull();
    assertThat(getCatalog1.getName()).isEqualTo("catalog1");

    // get catalog (principal-1) -> denied
    assertPermissionDenied(() -> principal1CatalogsApi.getCatalog("admincatalog1"));

    // Cross-channel probe 1: the same GET, but with a JSON body that tries to override the `name`
    // path parameter. getCatalog's authorization key is the URL path segment, so the body must not
    // change which catalog is authorized; the request stays denied (403) exactly as it did with no
    // body.
    HttpResponse<String> rawWithBody =
        TestUtils.sendRawGet(
            principal1Config,
            "/api/2.1/unity-catalog/catalogs/admincatalog1",
            Optional.of("{\"name\":\"different\"}"));
    assertThat(rawWithBody.statusCode())
        .as("body on a URL-param GET must not bypass URL-driven authz")
        .isEqualTo(403);

    // Cross-channel probe 2: attacker omits the URL path segment entirely and moves the name
    // into the body. This routes to listCatalogs (a different endpoint with per-user filtering),
    // NOT getCatalog. The body's "name" field is not routed through authz, and the response must
    // not leak admincatalog1 to principal-1 who lacks access.
    HttpResponse<String> rawWithBodyNoPath =
        TestUtils.sendRawGet(
            principal1Config,
            "/api/2.1/unity-catalog/catalogs",
            Optional.of("{\"name\":\"admincatalog1\"}"));
    assertThat(rawWithBodyNoPath.statusCode())
        .as("listCatalogs must return 200 for a principal that can see at least their own catalog")
        .isEqualTo(200);
    assertThat(rawWithBodyNoPath.body())
        .as("listCatalogs response must not leak catalogs the caller cannot see")
        .doesNotContain("admincatalog1");

    // Cross-channel probe 3 (write path via PATCH): updateCatalog's authorization key is the URL
    // path segment, not the body. A request body must not flip a path-based denial into a
    // successful mutation. principal-1 does not own admincatalog1, so the PATCH must be denied
    // regardless of the body it carries, and the catalog must be left unmodified.
    HttpResponse<String> rawPatch =
        TestUtils.sendRaw(
            principal1Config,
            "PATCH",
            "/api/2.1/unity-catalog/catalogs/admincatalog1",
            Optional.of("{\"comment\":\"hijacked\",\"new_name\":\"hijacked\"}"));
    assertThat(rawPatch.statusCode())
        .as("mutation on a non-owned catalog must be denied regardless of request body")
        .isEqualTo(403);
    assertThat(adminCatalogsApi.getCatalog("admincatalog1").getComment())
        .as("a denied PATCH must not have modified the catalog")
        .isEqualTo("(created from scratch)");

    // get catalog (regular-1) -> USE CATALOG -> allowed
    CatalogInfo getCatalog1AsRegular1 = regular1CatalogsApi.getCatalog("catalog1");
    assertThat(getCatalog1AsRegular1).isNotNull();

    // get catalog (principal-1) -> denied
    assertPermissionDenied(() -> regular1CatalogsApi.getCatalog("admincatalog1"));

    // update catalog (admin) -> metastore admin -> denied
    UpdateCatalog updateCatalog1Admin = new UpdateCatalog().comment("(admin update)");
    assertPermissionDenied(() -> adminCatalogsApi.updateCatalog("catalog1", updateCatalog1Admin));

    // update catalog (principal-1) -> owner -> allowed
    UpdateCatalog updateCatalog1Owner = new UpdateCatalog().comment("(principal update 1)");
    CatalogInfo updatedCatalog1 =
        principal1CatalogsApi.updateCatalog("catalog1", updateCatalog1Owner);
    assertThat(updatedCatalog1.getComment()).isEqualTo("(principal update 1)");

    // grant USE CATALOG to principal-1 and update again
    grantPermissions(PRINCIPAL_1, SecurableType.CATALOG, "catalog1", Privileges.USE_CATALOG);
    UpdateCatalog updateCatalog1Owner2 = new UpdateCatalog().comment("(principal update 2)");
    CatalogInfo updatedCatalog1Again =
        principal1CatalogsApi.updateCatalog("catalog1", updateCatalog1Owner2);
    assertThat(updatedCatalog1Again.getComment()).isEqualTo("(principal update 2)");

    // update catalog (regular-1) -> use catalog -> denied
    UpdateCatalog updateCatalog1Regular = new UpdateCatalog().comment("(regular update)");
    assertPermissionDenied(
        () -> regular1CatalogsApi.updateCatalog("catalog1", updateCatalog1Regular));

    // create a catalog -> metastore admin -> allowed
    CreateCatalog adminCatalog2 =
        new CreateCatalog().name("admincatalog2").comment("(created from scratch)");
    adminCatalogsApi.createCatalog(adminCatalog2);

    // delete a catalog -> denied
    assertPermissionDenied(() -> principal1CatalogsApi.deleteCatalog("admincatalog2", null));

    // force delete a catalog -> USE CATALOG -> denied
    grantPermissions(REGULAR_1, SecurableType.CATALOG, "admincatalog1", Privileges.USE_CATALOG);
    assertPermissionDenied(() -> regular1CatalogsApi.deleteCatalog("admincatalog1", true));
    assertThat(adminCatalogsApi.getCatalog("admincatalog1").getName()).isEqualTo("admincatalog1");
    assertThat(adminSchemasApi.getSchema("admincatalog1.default").getName()).isEqualTo("default");

    // delete a catalog -> metastore admin -> allowed
    adminCatalogsApi.deleteCatalog("admincatalog2", null);

    // create a catalog -> CREATE CATALOG -> allowed
    CreateCatalog catalog3 = new CreateCatalog().name("catalog3").comment("(created from scratch)");
    principal1CatalogsApi.createCatalog(catalog3);
    adminCatalogsApi.deleteCatalog("catalog3", null);

    // create a catalog -> CREATE CATALOG -> allowed
    CreateCatalog catalog4 = new CreateCatalog().name("catalog4").comment("(created from scratch)");
    principal1CatalogsApi.createCatalog(catalog4);

    // delete a catalog -> catalog owner -> allowed
    principal1CatalogsApi.deleteCatalog("catalog4", null);

    // managed storage with external location

    // Try to create a catalog at the location before External Location is created would fail
    CreateCatalog catalogWithLoc1 =
        new CreateCatalog()
            .name("catalog_with_location1")
            .storageRoot("file:///tmp/external_location");
    assertPermissionDenied(() -> principal1CatalogsApi.createCatalog(catalogWithLoc1));

    // Create the External Location
    createExternalLocationWithCredential("admin_cred", "admin_el", "file:///tmp/external_location");

    // Try to create a catalog at the location and still fail due to lack of permission
    assertPermissionDenied(() -> principal1CatalogsApi.createCatalog(catalogWithLoc1));

    // Grant CREATE MANAGED STORAGE permission
    grantPermissions(
        PRINCIPAL_1,
        SecurableType.EXTERNAL_LOCATION,
        "admin_el",
        Privileges.CREATE_MANAGED_STORAGE);

    // Then the catalog using external location as managed storage can be created
    CatalogInfo catalogWithLoc1Info = principal1CatalogsApi.createCatalog(catalogWithLoc1);
    assertThat(catalogWithLoc1Info).isNotNull();
    assertThat(catalogWithLoc1Info.getName()).isEqualTo("catalog_with_location1");

    // Create a table, then a catalog under the table. It should fail.
    TablesApi adminTablesApi = new TablesApi(adminApiClient);
    createExternalTable(
        adminTablesApi,
        "admincatalog1",
        "default",
        "tbl_pr1",
        "file:///tmp/external_location/ext_table");

    CreateCatalog catalogWithLoc2 =
        new CreateCatalog()
            .name("catalog_with_location2")
            .storageRoot("file:///tmp/external_location/ext_table");
    assertPermissionDenied(() -> principal1CatalogsApi.createCatalog(catalogWithLoc2));
  }

  private static final String CATALOGS_PATH = "/api/2.1/unity-catalog/catalogs";

  /**
   * Body shapes the generated SDK cannot produce, exercised against POST /catalogs -- which reads
   * its authorization key from the CreateCatalog body, so every case here goes through the
   * PAYLOAD-source gate.
   *
   * <p>The cases share one setup because each is a single request with no state of its own. They
   * cover the two failure modes the gate must keep distinct -- a body that cannot bind is a client
   * error (400) raised before authorization, while a bound body that carries no usable key is a
   * denial (403) -- plus the valid shapes that must not be mistaken for either.
   */
  @Test
  @SneakyThrows
  public void rawBodyShapesAtThePayloadAuthorizationGate() {
    // Body-less: fails in binding before authorization runs, so 400 rather than 403.
    assertHttpApiException(
        TestUtils.sendRawEmptyPost(adminConfig, CATALOGS_PATH), ErrorCode.INVALID_ARGUMENT);

    // Non-JSON content-type: also cannot bind, so also 400 before authorization.
    assertHttpApiException(
        TestUtils.sendRawJsonPost(
            adminConfig, CATALOGS_PATH, "{\"name\":\"cat_text\"}", "text/plain"),
        ErrorCode.INVALID_ARGUMENT);

    // JSON literal null: binds to null, so it carries no authorization keys. The gate must fail
    // closed with 403 rather than NPE.
    assertHttpApiException(
        TestUtils.sendRawJsonPost(adminConfig, CATALOGS_PATH, "null", "application/json"),
        ErrorCode.PERMISSION_DENIED,
        UnityAccessDecorator.ERR_AUTH_NOT_EXECUTED);

    // Malformed JSON: parsing fails during binding, so this is a client error before authorization
    // rather than a denial.
    assertHttpApiException(
        TestUtils.sendRawJsonPost(adminConfig, CATALOGS_PATH, "{\"name\":", "application/json"),
        ErrorCode.INVALID_ARGUMENT);

    // Trailing whitespace after the JSON object must not be rejected by the gate.
    HttpResponse<String> trailingNewline =
        TestUtils.sendRawJsonPost(
            adminConfig,
            CATALOGS_PATH,
            "{\"name\":\"cat_trailing_newline\"}\n",
            "application/json");
    assertThat(trailingNewline.statusCode()).isEqualTo(200);
    assertThat(trailingNewline.body()).contains("cat_trailing_newline");

    // A charset-qualified JSON content-type must not be rejected by the gate either.
    HttpResponse<String> charset =
        TestUtils.sendRawJsonPost(
            adminConfig,
            CATALOGS_PATH,
            "{\"name\":\"cat_charset\"}",
            "application/json; charset=utf-8");
    assertThat(charset.statusCode()).isEqualTo(200);
    assertThat(charset.body()).contains("cat_charset");

    // Body split across two chunks, mid-token: the gate authorizes the reassembled body, so this
    // must succeed exactly like the fixed-length case above.
    HttpResponse<String> chunked =
        TestUtils.sendTwoChunkJsonPost(
            adminConfig, CATALOGS_PATH, "{\"name\":\"cat_chunked\"}", "application/json");
    assertThat(chunked.statusCode()).isEqualTo(200);
    assertThat(chunked.body()).contains("cat_chunked");
  }

  @Test
  @SneakyThrows
  public void forceDeleteCatalogClearsChildAuthorizations() {
    createCommonTestUsers();

    CatalogsApi adminCatalogsApi = new CatalogsApi(adminApiClient);
    SchemasApi adminSchemasApi = new SchemasApi(adminApiClient);
    TablesApi adminTablesApi = new TablesApi(adminApiClient);

    CreateCatalog catalog =
        new CreateCatalog().name("force_delete_catalog").comment("force delete auth cleanup test");
    CatalogInfo catalogInfo = adminCatalogsApi.createCatalog(catalog);
    adminSchemasApi.createSchema(
        new CreateSchema().name("default").catalogName("force_delete_catalog"));
    SchemaInfo schemaInfo = adminSchemasApi.getSchema("force_delete_catalog.default");
    TableInfo tableInfo =
        createExternalTable(
            adminTablesApi,
            "force_delete_catalog",
            "default",
            "tbl",
            testDirectoryRoot.resolve("force_delete_tbl").toUri().toString());

    grantPermissions(
        REGULAR_1, SecurableType.CATALOG, "force_delete_catalog", Privileges.USE_CATALOG);
    grantPermissions(
        REGULAR_1, SecurableType.SCHEMA, "force_delete_catalog.default", Privileges.USE_SCHEMA);
    grantPermissions(
        REGULAR_1, SecurableType.TABLE, "force_delete_catalog.default.tbl", Privileges.SELECT);

    assertThat(
            countCasbinRulesReferencing(
                catalogInfo.getId(), schemaInfo.getSchemaId(), tableInfo.getTableId()))
        .isPositive();

    adminCatalogsApi.deleteCatalog("force_delete_catalog", true);

    assertThat(
            countCasbinRulesReferencing(
                catalogInfo.getId(), schemaInfo.getSchemaId(), tableInfo.getTableId()))
        .isZero();
  }
}
