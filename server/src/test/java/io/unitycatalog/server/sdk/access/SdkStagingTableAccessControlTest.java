package io.unitycatalog.server.sdk.access;

import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.api.SchemasApi;
import io.unitycatalog.client.model.CreateSchema;
import io.unitycatalog.client.model.SecurableType;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.persist.model.Privileges;
import io.unitycatalog.server.utils.TestUtils;
import java.util.List;
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
 * <p>All surface-specific RPCs are encapsulated behind {@link #createStaging}, {@link
 * #finalizeManagedTable}, and {@link #fetchTempCreds}; the abstract test body imports neither
 * surface's TablesApi nor TemporaryCredentialsApi. SchemasApi is used inline because it's
 * surface-independent.
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
   * Surface-specific temporary-credentials RPC for the table identified by {@code tableId} (which
   * may be a staging-table UUID or a regular-table UUID). The UC REST and Delta REST surfaces
   * expose different RPCs ({@code generateTemporaryTableCredentials} vs {@code
   * getStagingTableCredentials}) but both gate through the same {@code @AuthorizeResourceKey}-
   * resolved ownership check, so the access-control semantics this test pins must hold on either
   * surface; each subclass implements this against its own RPC.
   */
  protected abstract void fetchTempCreds(ServerConfig config, String tableId) throws Exception;

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

    // Step 1: User A creates a staging table (surface-specific).
    StagingHandle staging =
        createStaging(
            userAConfig, TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, TestUtils.TABLE_NAME);
    assertThat(staging.location()).isNotNull();

    // Step 2: temp-creds access control on the staging table -- exercised against the subclass's
    // own surface (UC REST generateTemporaryTableCredentials / Delta REST
    // getStagingTableCredentials).
    // Owner allowed; non-owner denied.
    fetchTempCreds(userAConfig, staging.id());
    TestUtils.assertPermissionDenied(() -> fetchTempCreds(userBConfig, staging.id()));

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

    // Step 5: User B calls the temp-creds RPC with User A's regular-table UUID. The
    // surface-specific Delta {@code getStagingTableCredentials} handler has a "reject if not a
    // staging table" branch that returns 404 -- but that 404 is unreachable for a non-owner
    // because authz (@AuthorizeResourceKey resolves the UUID in the graph, finds User B is not
    // OWNER) rejects with 403 before the handler runs. UC REST's
    // {@code generateTemporaryTableCredentials} reaches the same 403 via its own ownership check.
    // Either subclass's surface must exhibit the fail-closed 403; we don't pin which one wins for
    // an owner-with-regular-UUID call here.
    TestUtils.assertPermissionDenied(() -> fetchTempCreds(userBConfig, finalized.tableId()));
  }
}
