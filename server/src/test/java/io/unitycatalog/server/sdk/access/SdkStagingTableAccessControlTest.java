package io.unitycatalog.server.sdk.access;

import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.api.SchemasApi;
import io.unitycatalog.client.api.TablesApi;
import io.unitycatalog.client.api.TemporaryCredentialsApi;
import io.unitycatalog.client.delta.model.CreateStagingTableRequest;
import io.unitycatalog.client.model.ColumnInfo;
import io.unitycatalog.client.model.ColumnTypeName;
import io.unitycatalog.client.model.CreateSchema;
import io.unitycatalog.client.model.CreateStagingTable;
import io.unitycatalog.client.model.CreateTable;
import io.unitycatalog.client.model.DataSourceFormat;
import io.unitycatalog.client.model.GenerateTemporaryTableCredential;
import io.unitycatalog.client.model.SecurableType;
import io.unitycatalog.client.model.StagingTableInfo;
import io.unitycatalog.client.model.TableInfo;
import io.unitycatalog.client.model.TableOperation;
import io.unitycatalog.client.model.TableType;
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
 * <p>This test class extends {@link SdkAccessControlBaseCRUDTest} which provides common support
 * functionality for SDK-based access control tests, including user management, permission
 * management, and resource setup.
 */
public class SdkStagingTableAccessControlTest extends SdkAccessControlBaseCRUDTest {

  private final List<ColumnInfo> columns =
      List.of(
          new ColumnInfo()
              .name("test_column")
              .typeText("INTEGER")
              .typeJson("{\"type\": \"integer\"}")
              .typeName(ColumnTypeName.INT)
              .position(0)
              .nullable(true));

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

    // Create API clients for both users
    ServerConfig userAConfig = createTestUserServerConfig(userAEmail);
    ServerConfig userBConfig = createTestUserServerConfig(userBEmail);
    TablesApi userATablesApi = new TablesApi(TestUtils.createApiClient(userAConfig));
    TablesApi userBTablesApi = new TablesApi(TestUtils.createApiClient(userBConfig));
    TemporaryCredentialsApi userATempCredsApi =
        new TemporaryCredentialsApi(TestUtils.createApiClient(userAConfig));
    TemporaryCredentialsApi userBTempCredsApi =
        new TemporaryCredentialsApi(TestUtils.createApiClient(userBConfig));

    // Step 1: User A creates a staging table
    CreateStagingTable createStagingTableRequest =
        new CreateStagingTable()
            .catalogName(TestUtils.CATALOG_NAME)
            .schemaName(TestUtils.SCHEMA_NAME)
            .name(TestUtils.TABLE_NAME);

    StagingTableInfo stagingTableInfo =
        userATablesApi.createStagingTable(createStagingTableRequest);
    assertThat(stagingTableInfo).isNotNull();
    assertThat(stagingTableInfo.getStagingLocation()).isNotNull();

    // Step 2: Test temporary credentials generation
    // User A (owner) should be able to get temporary credentials
    GenerateTemporaryTableCredential tempCredRequest =
        new GenerateTemporaryTableCredential()
            .tableId(stagingTableInfo.getId())
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
    UUID stagingId = UUID.fromString(stagingTableInfo.getId());

    // User A (owner) -> allowed
    assertThat(userADeltaCredsApi.getStagingTableCredentials(stagingId)).isNotNull();

    // User B (non-owner) -> denied
    TestUtils.assertPermissionDenied(
        () -> userBDeltaCredsApi.getStagingTableCredentials(stagingId));

    // Delta REST createStagingTable: same authz rule as UC REST StagingTableService (catalog
    // USE_CATALOG/OWNER + schema USE_SCHEMA+CREATE_TABLE OR schema OWNER). Verify here that both
    // entry points grant equivalent access so they don't silently drift apart.

    // User A covers the `USE_SCHEMA + CREATE_TABLE` arm of the OR -- User A's schema grants
    // are USE_SCHEMA + CREATE_TABLE (set up at the top of the test), but NOT OWNER.
    assertThat(
            deltaTablesApi(userAConfig)
                .createStagingTable(
                    TestUtils.CATALOG_NAME,
                    TestUtils.SCHEMA_NAME,
                    new CreateStagingTableRequest().name("drc_staging_allowed_via_use_schema")))
        .isNotNull();

    // User D covers the `schema OWNER` arm of the OR. OWNER is not grantable via the API (it's
    // implicit from creating a resource), so User D creates their own schema and then creates a
    // staging table in it -- with no explicit CREATE_TABLE grant, only the schema OWNER path can
    // admit this call. Pairing with User A above, both branches of the OR in
    // AuthorizeExpressions.CREATE_STAGING_TABLE are exercised.
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
            deltaTablesApi(userDConfig)
                .createStagingTable(
                    TestUtils.CATALOG_NAME,
                    userDSchema,
                    new CreateStagingTableRequest().name("drc_staging_allowed_via_schema_owner")))
        .isNotNull();

    // User C has USE_CATALOG only (neither arm of the OR satisfied) -- denied.
    String userCEmail = "user_c@example.com";
    createTestUser(userCEmail, "User C");
    grantPermissions(
        userCEmail, SecurableType.CATALOG, TestUtils.CATALOG_NAME, Privileges.USE_CATALOG);
    ServerConfig userCConfig = createTestUserServerConfig(userCEmail);
    TestUtils.assertPermissionDenied(
        () ->
            deltaTablesApi(userCConfig)
                .createStagingTable(
                    TestUtils.CATALOG_NAME,
                    TestUtils.SCHEMA_NAME,
                    new CreateStagingTableRequest().name("drc_staging_denied")));

    // Step 3: User B attempts to create a managed table using User A's staging location
    CreateTable createTableRequest =
        new CreateTable()
            .name(TestUtils.TABLE_NAME)
            .catalogName(TestUtils.CATALOG_NAME)
            .schemaName(TestUtils.SCHEMA_NAME)
            .columns(columns)
            .tableType(TableType.MANAGED)
            .dataSourceFormat(DataSourceFormat.DELTA)
            .storageLocation(stagingTableInfo.getStagingLocation())
            .comment("Table created by different user - should fail");

    // This by user B should fail with PERMISSION_DENIED and a message identifying the check.
    TestUtils.assertPermissionDenied(
        () -> userBTablesApi.createTable(createTableRequest),
        "User attempts to create table on a staging location without ownership");

    // Step 4: The same request by user A should succeed
    TableInfo tableInfo = userATablesApi.createTable(createTableRequest);
    assertThat(tableInfo).isNotNull();
    assertThat(tableInfo.getStorageLocation()).isEqualTo(stagingTableInfo.getStagingLocation());
    assertThat(tableInfo.getTableId()).isEqualTo(stagingTableInfo.getId());

    // Step 5: User B calls getStagingTableCredentials with User A's regular-table UUID.
    // This is not the same as the SdkDeltaCredentialsTest case (owner passes regular-table UUID
    // and gets 404 from the reject-regular-table logic); here the caller is a non-owner, so
    // authz (@AuthorizeResourceKey(TABLE)) resolves the regular-table UUID in the graph, finds
    // that User B is not OWNER, and rejects with 403 before the handler runs. The 404 path is
    // therefore unreachable for non-owners -- which is fail-closed and the desired behavior.
    UUID tableUuid = UUID.fromString(tableInfo.getTableId());
    TestUtils.assertPermissionDenied(
        () -> userBDeltaCredsApi.getStagingTableCredentials(tableUuid));
  }

  // FQCN kept at this single site because io.unitycatalog.client.delta.api.TablesApi has the same
  // simple name as io.unitycatalog.client.api.TablesApi that's imported for createTable above.
  private static io.unitycatalog.client.delta.api.TablesApi deltaTablesApi(ServerConfig config) {
    return new io.unitycatalog.client.delta.api.TablesApi(TestUtils.createApiClient(config));
  }
}
