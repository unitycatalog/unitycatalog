package io.unitycatalog.server.sdk.access;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.TablesApi;
import io.unitycatalog.client.api.TemporaryCredentialsApi;
import io.unitycatalog.client.model.ColumnInfo;
import io.unitycatalog.client.model.ColumnTypeName;
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
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.persist.model.Privileges;
import io.unitycatalog.server.utils.TestUtils;
import java.util.List;
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
    assertThatExceptionOfType(ApiException.class)
        .isThrownBy(() -> userBTempCredsApi.generateTemporaryTableCredentials(tempCredRequest))
        .satisfies(
            ex ->
                assertThat(ex.getCode())
                    .isEqualTo(ErrorCode.PERMISSION_DENIED.getHttpStatus().code()));

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

    // This by user B should fail with PERMISSION_DENIED
    assertThatExceptionOfType(ApiException.class)
        .isThrownBy(() -> userBTablesApi.createTable(createTableRequest))
        .satisfies(
            ex ->
                assertThat(ex.getCode())
                    .isEqualTo(ErrorCode.PERMISSION_DENIED.getHttpStatus().code()))
        .withMessageContaining(
            "User attempts to create table on a staging location without ownership");

    // Step 4: The same request by user A should succeed
    TableInfo tableInfo = userATablesApi.createTable(createTableRequest);
    assertThat(tableInfo).isNotNull();
    assertThat(tableInfo.getStorageLocation()).isEqualTo(stagingTableInfo.getStagingLocation());
    assertThat(tableInfo.getTableId()).isEqualTo(stagingTableInfo.getId());
  }
}
