package io.unitycatalog.server.sdk.access;

import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.api.SchemasApi;
import io.unitycatalog.client.api.TemporaryCredentialsApi;
import io.unitycatalog.client.model.CreateSchema;
import io.unitycatalog.client.model.GenerateTemporaryTableCredential;
import io.unitycatalog.client.model.SecurableType;
import io.unitycatalog.client.model.TableOperation;
import io.unitycatalog.client.model.TemporaryCredentials;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.persist.model.Privileges;
import io.unitycatalog.server.utils.TestUtils;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * Access control tests for staging tables that require authorization to be enabled.
 *
 * <p>Abstract base class. {@link SdkUCStagingTableAccessControlTest} runs the suite using UC REST
 * createStagingTable / createTable; {@link SdkDeltaStagingTableAccessControlTest} runs the same
 * suite against the Delta REST endpoints. The two surfaces share the persist-layer cross-principal
 * check at {@code StagingTableRepository.commitStagingTable}, so both subclasses must observe
 * identical staging-table access-control behavior; running the same {@code @Test} against both
 * ensures they don't drift.
 *
 * <p>This class does not import either {@code io.unitycatalog.client.api.TablesApi} or {@code
 * io.unitycatalog.client.delta.api.TablesApi}; subclasses encapsulate all surface-specific table
 * RPCs behind {@link #createStaging} and {@link #finalizeManagedTable}. Other APIs reachable
 * regardless of subclass (TemporaryCredentialsApi, SchemasApi) are used inline for cross-cutting
 * checks that don't vary by surface.
 */
public abstract class SdkStagingTableAccessControlTest extends SdkAccessControlBaseCRUDTest {

  /** Just-created staging table identifier (id + storage location). */
  protected record StagingHandle(String id, String location) {}

  /** Result of finalizing a managed table from a staging location. */
  protected record FinalizedTable(String tableId, String storageLocation) {}

  /** Surface-specific {@code createStagingTable} RPC. */
  protected abstract StagingHandle createStaging(
      ServerConfig config, String catalog, String schema, String name) throws Exception;

  /** Surface-specific {@code createTable} RPC that finalizes a managed table from a staging. */
  protected abstract FinalizedTable finalizeManagedTable(
      ServerConfig config, StagingHandle staging, String name) throws Exception;

  /**
   * Test that attempting to create a managed table from another user's staging table fails with
   * PERMISSION_DENIED error.
   *
   * <p>This test validates that the ownership check in staging table commit properly prevents users
   * from creating tables from staging locations they don't own.
   */
  @Test
  public void testManagedTableCreationByDifferentUserShouldFail() throws Exception {
    // Create test users in the database first
    String userAEmail = "user_a@example.com";
    String userBEmail = "user_b@example.com";
    createTestUser(userAEmail, "User A");
    createTestUser(userBEmail, "User B");

    // Grant permissions to both users on metastore, catalog, and schema
    for (String email : List.of(userAEmail, userBEmail)) {
      grantPermissions(
          email,
          SecurableType.METASTORE,
          METASTORE_NAME,
          Privileges.USE_CATALOG,
          Privileges.CREATE_CATALOG);
      grantPermissions(
          email, SecurableType.CATALOG, TestUtils.CATALOG_NAME, Privileges.USE_CATALOG);
      grantPermissions(
          email,
          SecurableType.SCHEMA,
          TestUtils.SCHEMA_FULL_NAME,
          Privileges.USE_SCHEMA,
          Privileges.CREATE_TABLE);
    }

    ServerConfig userAConfig = createTestUserServerConfig(userAEmail);
    ServerConfig userBConfig = createTestUserServerConfig(userBEmail);
    TemporaryCredentialsApi userATempCredsApi =
        new TemporaryCredentialsApi(TestUtils.createApiClient(userAConfig));
    TemporaryCredentialsApi userBTempCredsApi =
        new TemporaryCredentialsApi(TestUtils.createApiClient(userBConfig));

    // Step 1: User A creates a staging table (surface-specific).
    StagingHandle staging =
        createStaging(
            userAConfig, TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, TestUtils.TABLE_NAME);
    assertThat(staging.location()).isNotNull();

    // Step 2: Test temporary credentials generation
    // User A (owner) should be able to get temporary credentials
    GenerateTemporaryTableCredential tempCredRequest =
        new GenerateTemporaryTableCredential()
            .tableId(staging.id())
            .operation(TableOperation.READ_WRITE);

    TemporaryCredentials tempCredsA =
        userATempCredsApi.generateTemporaryTableCredentials(tempCredRequest);
    assertThat(tempCredsA).isNotNull();

    // User B (non-owner) should fail to get temporary credentials with PERMISSION_DENIED
    TestUtils.assertPermissionDenied(
        () -> userBTempCredsApi.generateTemporaryTableCredentials(tempCredRequest));

    // Delta REST getStagingTableCredentials follows the same OWNER-only authz.
    io.unitycatalog.client.delta.api.TemporaryCredentialsApi userADeltaCredsApi =
        new io.unitycatalog.client.delta.api.TemporaryCredentialsApi(
            TestUtils.createApiClient(userAConfig));
    io.unitycatalog.client.delta.api.TemporaryCredentialsApi userBDeltaCredsApi =
        new io.unitycatalog.client.delta.api.TemporaryCredentialsApi(
            TestUtils.createApiClient(userBConfig));
    UUID stagingId = UUID.fromString(staging.id());

    // User A (owner) -> allowed
    assertThat(userADeltaCredsApi.getStagingTableCredentials(stagingId)).isNotNull();

    // User B (non-owner) -> denied
    TestUtils.assertPermissionDenied(
        () -> userBDeltaCredsApi.getStagingTableCredentials(stagingId));

    // createStagingTable authz (catalog USE_CATALOG/OWNER + schema USE_SCHEMA+CREATE_TABLE OR
    // schema OWNER). Both surfaces share the same SpEL constant
    // AuthorizeExpressions.CREATE_STAGING_TABLE; this section exercises both arms of the OR
    // through the abstract createStaging so each subclass tests its own RPC.

    // User A covers the `USE_SCHEMA + CREATE_TABLE` arm -- granted at the top of the test, but
    // not schema OWNER.
    assertThat(
            createStaging(
                userAConfig,
                TestUtils.CATALOG_NAME,
                TestUtils.SCHEMA_NAME,
                "staging_allowed_via_use_schema"))
        .isNotNull();

    // User D covers the `schema OWNER` arm. OWNER is not grantable via the API (it's implicit from
    // creating a resource), so User D creates their own schema and then creates a staging table in
    // it -- with no explicit CREATE_TABLE grant, only the schema OWNER path can admit this call.
    String userDEmail = "user_d@example.com";
    String userDSchema = "user_d_schema";
    createTestUser(userDEmail, "User D");
    grantPermissions(
        userDEmail,
        SecurableType.CATALOG,
        TestUtils.CATALOG_NAME,
        Privileges.USE_CATALOG,
        Privileges.CREATE_SCHEMA);
    ServerConfig userDConfig = createTestUserServerConfig(userDEmail);
    new SchemasApi(TestUtils.createApiClient(userDConfig))
        .createSchema(new CreateSchema().name(userDSchema).catalogName(TestUtils.CATALOG_NAME));
    assertThat(
            createStaging(
                userDConfig,
                TestUtils.CATALOG_NAME,
                userDSchema,
                "staging_allowed_via_schema_owner"))
        .isNotNull();

    // User C has USE_CATALOG only (neither arm of the OR satisfied) -- denied.
    String userCEmail = "user_c@example.com";
    createTestUser(userCEmail, "User C");
    grantPermissions(
        userCEmail, SecurableType.CATALOG, TestUtils.CATALOG_NAME, Privileges.USE_CATALOG);
    ServerConfig userCConfig = createTestUserServerConfig(userCEmail);
    TestUtils.assertPermissionDenied(
        () ->
            createStaging(
                userCConfig, TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, "staging_denied"));

    // Steps 3+4: User B's createTable on User A's staging is rejected at commitStagingTable;
    // User A's same request succeeds and produces a regular table that reuses the staging UUID
    // and storage location.
    TestUtils.assertPermissionDenied(
        () -> finalizeManagedTable(userBConfig, staging, TestUtils.TABLE_NAME),
        "User attempts to create table on a staging location without ownership");
    FinalizedTable finalized = finalizeManagedTable(userAConfig, staging, TestUtils.TABLE_NAME);
    assertThat(finalized.storageLocation()).isEqualTo(staging.location());
    assertThat(finalized.tableId()).isEqualTo(staging.id());

    // Step 5: User B calls getStagingTableCredentials with User A's regular-table UUID.
    // This is not the same as the SdkDeltaCredentialsTest case (owner passes regular-table UUID
    // and gets 404 from the reject-regular-table logic); here the caller is a non-owner, so
    // authz (@AuthorizeResourceKey(TABLE)) resolves the regular-table UUID in the graph, finds
    // that User B is not OWNER, and rejects with 403 before the handler runs. The 404 path is
    // therefore unreachable for non-owners -- which is fail-closed and the desired behavior.
    UUID tableUuid = UUID.fromString(finalized.tableId());
    TestUtils.assertPermissionDenied(
        () -> userBDeltaCredsApi.getStagingTableCredentials(tableUuid));
  }
}
