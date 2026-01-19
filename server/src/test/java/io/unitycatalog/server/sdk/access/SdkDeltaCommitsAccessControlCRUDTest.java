package io.unitycatalog.server.sdk.access;

import static io.unitycatalog.server.utils.TestUtils.assertApiException;
import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.api.DeltaCommitsApi;
import io.unitycatalog.client.api.TablesApi;
import io.unitycatalog.client.model.ColumnInfo;
import io.unitycatalog.client.model.ColumnTypeName;
import io.unitycatalog.client.model.CreateStagingTable;
import io.unitycatalog.client.model.CreateTable;
import io.unitycatalog.client.model.DataSourceFormat;
import io.unitycatalog.client.model.DeltaCommit;
import io.unitycatalog.client.model.DeltaCommitInfo;
import io.unitycatalog.client.model.DeltaGetCommits;
import io.unitycatalog.client.model.DeltaGetCommitsResponse;
import io.unitycatalog.client.model.SecurableType;
import io.unitycatalog.client.model.StagingTableInfo;
import io.unitycatalog.client.model.TableInfo;
import io.unitycatalog.client.model.TableType;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.persist.model.Privileges;
import io.unitycatalog.server.utils.TestUtils;
import java.util.List;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Access control tests for DeltaCommits API that verify authorization rules.
 *
 * <p>Tests verify:
 *
 * <ul>
 *   <li>Clients without write permission cannot post commits or backfills
 *   <li>Clients without read permission cannot get commits
 *   <li>Clients without valid authentication cannot access the API
 *   <li>Correct HTTP error codes (401 Unauthorized, 403 Forbidden) are returned
 * </ul>
 */
public class SdkDeltaCommitsAccessControlCRUDTest extends SdkAccessControlBaseCRUDTest {

  private static final String READ_USER_EMAIL = "reader@example.com";
  private static final String WRITE_USER_EMAIL = "writer@example.com";
  private static final String NO_ACCESS_USER_EMAIL = "noaccess@example.com";

  private final List<ColumnInfo> COLUMNS =
      List.of(
          new ColumnInfo()
              .name("test_col")
              .typeText("INTEGER")
              .typeJson("{\"type\": \"integer\"}")
              .typeName(ColumnTypeName.INT)
              .position(0)
              .nullable(true));
  private TableInfo tableInfo;
  private String tableFullName;

  @BeforeEach
  @Override
  public void setUp() {
    super.setUp();
    // Create a managed table as admin
    tableInfo = createManagedTable();
    tableFullName =
        TestUtils.CATALOG_NAME + "." + TestUtils.SCHEMA_NAME + "." + TestUtils.TABLE_NAME;
  }

  /**
   * Helper method to create a managed table for testing commits.
   *
   * <p>When authorization is enabled, managed tables must be created from a staging table location.
   *
   * @return The created TableInfo
   * @throws Exception if table creation fails
   */
  @SneakyThrows
  private TableInfo createManagedTable() {
    TablesApi adminTablesApi = new TablesApi(TestUtils.createApiClient(adminConfig));

    // First, create a staging table
    CreateStagingTable createStagingTable =
        new CreateStagingTable()
            .catalogName(TestUtils.CATALOG_NAME)
            .schemaName(TestUtils.SCHEMA_NAME)
            .name(TestUtils.TABLE_NAME);

    StagingTableInfo stagingTableInfo = adminTablesApi.createStagingTable(createStagingTable);

    // Then, create the managed table using the staging location
    CreateTable createTable =
        new CreateTable()
            .name(TestUtils.TABLE_NAME)
            .catalogName(TestUtils.CATALOG_NAME)
            .schemaName(TestUtils.SCHEMA_NAME)
            .columns(COLUMNS)
            .properties(TestUtils.PROPERTIES)
            .comment(TestUtils.COMMENT)
            .storageLocation(stagingTableInfo.getStagingLocation())
            .tableType(TableType.MANAGED)
            .dataSourceFormat(DataSourceFormat.DELTA);

    return adminTablesApi.createTable(createTable);
  }

  /**
   * Helper method to create a commit object for testing.
   *
   * @param version The commit version
   * @return A Commit object
   */
  private DeltaCommit createCommitObject(Long version) {
    return new DeltaCommit()
        .tableId(tableInfo.getTableId())
        .tableUri(tableInfo.getStorageLocation())
        .commitInfo(
            new DeltaCommitInfo()
                .version(version)
                .fileName("file" + version)
                .fileSize(100L)
                .timestamp(1700000000L + version)
                .fileModificationTimestamp(1700000000L + version));
  }

  /**
   * Helper method to create a backfill-only commit object.
   *
   * @param latestBackfilledVersion The latest backfilled version
   * @return A Commit object for backfilling
   */
  private DeltaCommit createBackfillOnlyCommitObject(Long latestBackfilledVersion) {
    return new DeltaCommit()
        .tableId(tableInfo.getTableId())
        .tableUri(tableInfo.getStorageLocation())
        .latestBackfilledVersion(latestBackfilledVersion);
  }

  @SneakyThrows
  private DeltaGetCommitsResponse getCommits(DeltaCommitsApi api) {
    return api.getCommits(
        new DeltaGetCommits()
            .tableId(tableInfo.getTableId())
            .tableUri(tableInfo.getStorageLocation())
            .startVersion(0L));
  }

  @Test
  public void testCommitAndGetCommitsPermission() throws Exception {
    // Create test users
    createTestUser(READ_USER_EMAIL, "Read User");
    createTestUser(WRITE_USER_EMAIL, "Write User");
    createTestUser(NO_ACCESS_USER_EMAIL, "No Access User");

    // Grant catalog and schema permissions to both users
    for (String email : List.of(READ_USER_EMAIL, WRITE_USER_EMAIL)) {
      grantPermissions(
          email, SecurableType.CATALOG, TestUtils.CATALOG_NAME, Privileges.USE_CATALOG);
      grantPermissions(
          email, SecurableType.SCHEMA, TestUtils.SCHEMA_FULL_NAME, Privileges.USE_SCHEMA);
    }

    // Grant SELECT to read user (no MODIFY)
    grantPermissions(READ_USER_EMAIL, SecurableType.TABLE, tableFullName, Privileges.SELECT);

    // Grant MODIFY to write user
    grantPermissions(WRITE_USER_EMAIL, SecurableType.TABLE, tableFullName, Privileges.MODIFY);

    // Create API clients for read user, write user, and a 3rd user without any access.
    ServerConfig readUserConfig = createTestUserServerConfig(READ_USER_EMAIL);
    ServerConfig writeUserConfig = createTestUserServerConfig(WRITE_USER_EMAIL);
    ServerConfig noAccessUserConfig = createTestUserServerConfig(NO_ACCESS_USER_EMAIL);
    DeltaCommitsApi readUserCommitsApi =
        new DeltaCommitsApi(TestUtils.createApiClient(readUserConfig));
    DeltaCommitsApi writeUserCommitsApi =
        new DeltaCommitsApi(TestUtils.createApiClient(writeUserConfig));
    DeltaCommitsApi noAccessUserCommitsApi =
        new DeltaCommitsApi(TestUtils.createApiClient(noAccessUserConfig));
    DeltaCommitsApi unauthCommitsApi = new DeltaCommitsApi(TestUtils.createApiClient(serverConfig));

    // Attempt to post a commit as read user should fail with 403 Forbidden
    DeltaCommit commit1 = createCommitObject(1L);
    assertApiException(
        () -> readUserCommitsApi.commit(commit1), ErrorCode.PERMISSION_DENIED, "denied");
    // Write user posts a commit successfully
    writeUserCommitsApi.commit(commit1);

    // No-access user attempts to get commits should fail with 403 Forbidden
    assertApiException(
        () -> getCommits(noAccessUserCommitsApi), ErrorCode.PERMISSION_DENIED, "denied");

    // read user can get commits
    DeltaGetCommitsResponse commits = getCommits(readUserCommitsApi);
    assertThat(commits.getCommits()).isNotNull();
    assertThat(commits.getCommits().size()).isEqualTo(1);
    assertThat(commits.getLatestTableVersion()).isEqualTo(1L);

    // Read user attempts to backfill should fail with 403 Forbidden
    DeltaCommit backfillCommit = createBackfillOnlyCommitObject(1L);
    assertApiException(
        () -> readUserCommitsApi.commit(backfillCommit), ErrorCode.PERMISSION_DENIED, "denied");

    // Write user can backfill
    writeUserCommitsApi.commit(backfillCommit);

    // Unauthorized client should fail with 401 Unauthorized
    DeltaCommit commit2 = createCommitObject(2L);
    assertApiException(
        () -> unauthCommitsApi.commit(commit2), ErrorCode.UNAUTHENTICATED, "authorization");
    assertApiException(
        () -> getCommits(unauthCommitsApi), ErrorCode.UNAUTHENTICATED, "authorization");
  }
}
