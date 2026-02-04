package io.unitycatalog.server.sdk.access;

import static io.unitycatalog.server.utils.TestUtils.assertPermissionDenied;
import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.api.CatalogsApi;
import io.unitycatalog.client.api.SchemasApi;
import io.unitycatalog.client.api.TablesApi;
import io.unitycatalog.client.model.CatalogInfo;
import io.unitycatalog.client.model.CreateCatalog;
import io.unitycatalog.client.model.CreateSchema;
import io.unitycatalog.client.model.SecurableType;
import io.unitycatalog.client.model.UpdateCatalog;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.persist.model.Privileges;
import io.unitycatalog.server.utils.TestUtils;
import java.util.List;
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

    // delete a catalog -> metastore admin -> allowed
    adminCatalogsApi.deleteCatalog("admincatalog2", null);

    // create a catalog -> CREATE CATALOG -> allowed
    CreateCatalog catalog3 = new CreateCatalog().name("catalog3").comment("(created from scratch)");
    principal1CatalogsApi.createCatalog(catalog3);
    adminCatalogsApi.deleteCatalog("catalog3", null);

    // create a catalog -> CREATE CATALOG -> allowed
    CreateCatalog catalog4 = new CreateCatalog().name("catalog4").comment("(created from scratch)");
    principal1CatalogsApi.createCatalog(catalog4);
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
}
