package io.unitycatalog.server.sdk.access;

import static io.unitycatalog.server.utils.TestUtils.assertHttpApiException;
import static io.unitycatalog.server.utils.TestUtils.assertPermissionDenied;
import static io.unitycatalog.server.utils.TestUtils.sendRawGet;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.unitycatalog.client.api.CatalogsApi;
import io.unitycatalog.client.api.SchemasApi;
import io.unitycatalog.client.api.TablesApi;
import io.unitycatalog.client.model.PermissionsChange;
import io.unitycatalog.client.model.Privilege;
import io.unitycatalog.client.model.SecurableType;
import io.unitycatalog.client.model.UpdateCatalog;
import io.unitycatalog.client.model.UpdatePermissions;
import io.unitycatalog.client.model.UpdateSchema;
import io.unitycatalog.client.model.UpdateTable;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.persist.model.Privileges;
import io.unitycatalog.server.utils.TestUtils;
import java.net.http.HttpResponse;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/** Verifies table existence checks against persisted grants and authenticated HTTP requests. */
public class SdkTableExistsAccessControlTest extends SdkAccessControlBaseCRUDTest {
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String TABLES_PATH = "/api/2.1/unity-catalog/tables/";

  private ServerConfig regularConfig;
  private TablesApi regularTables;

  @BeforeEach
  void createFixture() throws Exception {
    createTestUser(REGULAR_1);
    regularConfig = createTestUserServerConfig(REGULAR_1);
    regularTables = new TablesApi(TestUtils.createApiClient(regularConfig));
    createExternalTable(
        new TablesApi(adminApiClient),
        TestUtils.CATALOG_NAME,
        TestUtils.SCHEMA_NAME,
        TestUtils.TABLE_NAME,
        testDirectoryRoot.resolve("external").toString());
  }

  @ParameterizedTest
  @EnumSource(
      value = SecurableType.class,
      names = {"CATALOG", "SCHEMA", "TABLE"})
  void ownershipHonorsHierarchy(SecurableType ownerType) throws Exception {
    switch (ownerType) {
      case CATALOG ->
          new CatalogsApi(adminApiClient)
              .updateCatalog(TestUtils.CATALOG_NAME, new UpdateCatalog().owner(REGULAR_1));
      case SCHEMA ->
          new SchemasApi(adminApiClient)
              .updateSchema(TestUtils.SCHEMA_FULL_NAME, new UpdateSchema().owner(REGULAR_1));
      case TABLE ->
          new TablesApi(adminApiClient)
              .updateTable(TestUtils.TABLE_FULL_NAME, new UpdateTable().owner(REGULAR_1));
      default -> throw new IllegalArgumentException("Unexpected owner type: " + ownerType);
    }

    if (ownerType != SecurableType.CATALOG) {
      assertDenied();
      if (ownerType == SecurableType.TABLE) {
        grantPermissions(
            REGULAR_1, SecurableType.SCHEMA, TestUtils.SCHEMA_FULL_NAME, Privileges.USE_SCHEMA);
        assertDenied();
        revoke(SecurableType.SCHEMA, TestUtils.SCHEMA_FULL_NAME, Privilege.USE_SCHEMA);
      }
      grantPermissions(
          REGULAR_1, SecurableType.CATALOG, TestUtils.CATALOG_NAME, Privileges.USE_CATALOG);
      if (ownerType == SecurableType.TABLE) {
        assertDenied();
        grantPermissions(
            REGULAR_1, SecurableType.SCHEMA, TestUtils.SCHEMA_FULL_NAME, Privileges.USE_SCHEMA);
      }
    }
    assertExists(regularConfig, TestUtils.TABLE_FULL_NAME, true);
  }

  @Test
  void selectRequiresBothUsageGrantsAndModifyDoesNotGrantVisibility() throws Exception {
    grantPermissions(REGULAR_1, SecurableType.TABLE, TestUtils.TABLE_FULL_NAME, Privileges.SELECT);
    assertDenied();
    grantPermissions(
        REGULAR_1, SecurableType.CATALOG, TestUtils.CATALOG_NAME, Privileges.USE_CATALOG);
    assertDenied();
    grantPermissions(
        REGULAR_1, SecurableType.SCHEMA, TestUtils.SCHEMA_FULL_NAME, Privileges.USE_SCHEMA);
    assertExists(regularConfig, TestUtils.TABLE_FULL_NAME, true);
    revoke(SecurableType.CATALOG, TestUtils.CATALOG_NAME, Privilege.USE_CATALOG);
    assertDenied();
    grantPermissions(
        REGULAR_1, SecurableType.CATALOG, TestUtils.CATALOG_NAME, Privileges.USE_CATALOG);
    revoke(SecurableType.TABLE, TestUtils.TABLE_FULL_NAME, Privilege.SELECT);
    assertDenied();
    grantPermissions(REGULAR_1, SecurableType.TABLE, TestUtils.TABLE_FULL_NAME, Privileges.MODIFY);
    assertDenied();
  }

  @Test
  void catalogBrowseDiscoversTablesAndErrorsKeepTheirStatuses() throws Exception {
    assertExists(adminConfig, TestUtils.TABLE_FULL_NAME, true);
    assertDenied();
    for (String fullName :
        List.of(
            "missing_catalog.missing_schema.missing_table",
            TestUtils.CATALOG_NAME + ".missing_schema.missing_table",
            TestUtils.SCHEMA_FULL_NAME + ".missing_table")) {
      assertExists(regularConfig, fullName, false);
    }
    assertHttpApiException(
        sendRawGet(regularConfig, TABLES_PATH + "catalog..table/exists"),
        ErrorCode.INVALID_ARGUMENT);
    assertHttpApiException(
        sendRawGet(
            new ServerConfig(serverConfig.getServerUrl(), ""),
            TABLES_PATH + TestUtils.TABLE_FULL_NAME + "/exists"),
        ErrorCode.UNAUTHENTICATED);

    grantPermissions(REGULAR_1, SecurableType.CATALOG, TestUtils.CATALOG_NAME, Privileges.BROWSE);
    assertExists(regularConfig, TestUtils.TABLE_FULL_NAME, true);
    revoke(SecurableType.CATALOG, TestUtils.CATALOG_NAME, Privilege.BROWSE);
    assertDenied();
  }

  private void assertDenied() throws Exception {
    assertPermissionDenied(() -> regularTables.tableExists(TestUtils.TABLE_FULL_NAME));
    assertHttpApiException(
        sendRawGet(
            regularConfig, TABLES_PATH + TestUtils.TABLE_FULL_NAME + "/exists?include_browse=true"),
        ErrorCode.PERMISSION_DENIED);
  }

  private void assertExists(ServerConfig config, String fullName, boolean expected)
      throws Exception {
    assertThat(
            new TablesApi(TestUtils.createApiClient(config)).tableExists(fullName).getTableExists())
        .isEqualTo(expected);
    HttpResponse<String> response = sendRawGet(config, TABLES_PATH + fullName + "/exists");
    assertThat(response.statusCode()).isEqualTo(200);
    assertThat(MAPPER.readTree(response.body()))
        .isEqualTo(MAPPER.createObjectNode().put("table_exists", expected));
  }

  private void revoke(SecurableType type, String fullName, Privilege privilege) throws Exception {
    grantsApi.update(
        type,
        fullName,
        new UpdatePermissions()
            .changes(
                List.of(
                    new PermissionsChange()
                        .principal(REGULAR_1)
                        .add(List.of())
                        .remove(List.of(privilege)))));
  }
}
