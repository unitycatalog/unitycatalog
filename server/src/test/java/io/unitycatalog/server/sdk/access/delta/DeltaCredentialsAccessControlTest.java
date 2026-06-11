package io.unitycatalog.server.sdk.access.delta;

import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.delta.api.DeltaTablesApi;
import io.unitycatalog.client.delta.api.DeltaTemporaryCredentialsApi;
import io.unitycatalog.client.delta.model.DeltaCreateStagingTableRequest;
import io.unitycatalog.client.delta.model.DeltaCreateTableRequest;
import io.unitycatalog.client.delta.model.DeltaCredentialOperation;
import io.unitycatalog.client.delta.model.DeltaCredentialsResponse;
import io.unitycatalog.client.delta.model.DeltaStagingTableResponse;
import io.unitycatalog.client.delta.model.DeltaTableType;
import io.unitycatalog.client.model.SecurableType;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.delta.DeltaBaseTableCRUDTestEnv;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.persist.model.Privileges;
import io.unitycatalog.server.sdk.access.SdkAccessControlBaseCRUDTest;
import io.unitycatalog.server.sdk.catalog.SdkCatalogOperations;
import io.unitycatalog.server.utils.TestUtils;
import java.util.UUID;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;

/**
 * Access control tests for the UC Delta API table-credentials endpoint, focused on shallow clones:
 * the base-location entry in the response requires read access on the base table itself, not just
 * on the clone.
 */
public class DeltaCredentialsAccessControlTest extends SdkAccessControlBaseCRUDTest {

  private static final String SCHEMA_FULL_NAME =
      TestUtils.CATALOG_NAME + "." + TestUtils.SCHEMA_NAME;

  @Override
  protected SdkCatalogOperations createCatalogOperations(ServerConfig serverConfig) {
    return new SdkCatalogOperations(TestUtils.createApiClient(serverConfig));
  }

  @Test
  @SneakyThrows
  public void testShallowCloneBaseTableCredentialAccessControl() {
    // Admin creates a base managed table and a shallow clone of it via the Delta API.
    DeltaTablesApi adminTablesApi = new DeltaTablesApi(adminApiClient);
    UUID baseId = createManagedTable(adminTablesApi, "creds_base", null);
    createManagedTable(adminTablesApi, "creds_clone", baseId);

    createCommonTestUsers();
    DeltaTemporaryCredentialsApi principalCredsApi =
        new DeltaTemporaryCredentialsApi(
            TestUtils.createApiClient(createTestUserServerConfig(PRINCIPAL_1)));

    // Privileges on the clone alone pass the endpoint's own policy, but the base-location entry
    // requires read access on the base table -- so the request is denied.
    grantPermissions(
        PRINCIPAL_1, SecurableType.CATALOG, TestUtils.CATALOG_NAME, Privileges.USE_CATALOG);
    grantPermissions(PRINCIPAL_1, SecurableType.SCHEMA, SCHEMA_FULL_NAME, Privileges.USE_SCHEMA);
    grantPermissions(
        PRINCIPAL_1, SecurableType.TABLE, SCHEMA_FULL_NAME + ".creds_clone", Privileges.SELECT);
    TestUtils.assertApiException(
        () ->
            principalCredsApi.getTableCredentials(
                DeltaCredentialOperation.READ,
                TestUtils.CATALOG_NAME,
                TestUtils.SCHEMA_NAME,
                "creds_clone"),
        ErrorCode.PERMISSION_DENIED,
        "read access to its base table");

    // SELECT on the base table unlocks the two-entry response; the base entry is READ-scoped.
    grantPermissions(
        PRINCIPAL_1, SecurableType.TABLE, SCHEMA_FULL_NAME + ".creds_base", Privileges.SELECT);
    DeltaCredentialsResponse resp =
        principalCredsApi.getTableCredentials(
            DeltaCredentialOperation.READ,
            TestUtils.CATALOG_NAME,
            TestUtils.SCHEMA_NAME,
            "creds_clone");
    assertThat(resp.getStorageCredentials()).hasSize(2);
    assertThat(resp.getStorageCredentials().get(1).getOperation())
        .isEqualTo(DeltaCredentialOperation.READ);

    // The base table itself is unaffected: a single-entry response with SELECT on it.
    DeltaCredentialsResponse baseResp =
        principalCredsApi.getTableCredentials(
            DeltaCredentialOperation.READ,
            TestUtils.CATALOG_NAME,
            TestUtils.SCHEMA_NAME,
            "creds_base");
    assertThat(baseResp.getStorageCredentials()).hasSize(1);
  }

  /**
   * Stage and finalize a managed Delta table as the admin user; a non-null {@code baseTableId}
   * makes it a MANAGED_SHALLOW_CLONE of that base. Returns the created table's UUID.
   */
  private UUID createManagedTable(DeltaTablesApi tablesApi, String name, UUID baseTableId)
      throws ApiException {
    DeltaStagingTableResponse staging =
        tablesApi.createStagingTable(
            TestUtils.CATALOG_NAME,
            TestUtils.SCHEMA_NAME,
            new DeltaCreateStagingTableRequest().name(name));
    DeltaCreateTableRequest request =
        new DeltaCreateTableRequest()
            .name(name)
            .location(staging.getLocation())
            .tableType(
                baseTableId == null ? DeltaTableType.MANAGED : DeltaTableType.MANAGED_SHALLOW_CLONE)
            .baseTableId(baseTableId)
            .columns(DeltaBaseTableCRUDTestEnv.simpleSchema())
            .protocol(DeltaBaseTableCRUDTestEnv.managedProtocol())
            .properties(
                DeltaBaseTableCRUDTestEnv.managedContractProperties(
                    staging.getTableId().toString()))
            .lastCommitTimestampMs(1700000000000L);
    return tablesApi
        .createTable(TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, request)
        .getMetadata()
        .getTableUuid();
  }
}
