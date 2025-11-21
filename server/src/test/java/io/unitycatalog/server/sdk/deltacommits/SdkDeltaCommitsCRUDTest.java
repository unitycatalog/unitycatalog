package io.unitycatalog.server.sdk.deltacommits;

import static io.unitycatalog.server.utils.TestUtils.assertApiException;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.DeltaCommitsApi;
import io.unitycatalog.client.model.ColumnInfo;
import io.unitycatalog.client.model.ColumnInfos;
import io.unitycatalog.client.model.ColumnTypeName;
import io.unitycatalog.client.model.DeltaCommit;
import io.unitycatalog.client.model.DeltaCommitInfo;
import io.unitycatalog.client.model.DeltaCommitMetadataProperties;
import io.unitycatalog.client.model.DeltaGetCommits;
import io.unitycatalog.client.model.DeltaGetCommitsResponse;
import io.unitycatalog.client.model.DeltaMetadata;
import io.unitycatalog.client.model.TableInfo;
import io.unitycatalog.client.model.TableType;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.catalog.CatalogOperations;
import io.unitycatalog.server.base.schema.SchemaOperations;
import io.unitycatalog.server.base.table.BaseTableCRUDTestEnv;
import io.unitycatalog.server.base.table.TableOperations;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.persist.dao.DeltaCommitDAO;
import io.unitycatalog.server.sdk.catalog.SdkCatalogOperations;
import io.unitycatalog.server.sdk.schema.SdkSchemaOperations;
import io.unitycatalog.server.sdk.tables.SdkTableOperations;
import io.unitycatalog.server.utils.TestUtils;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;
import org.hibernate.Session;
import org.hibernate.query.Query;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SdkDeltaCommitsCRUDTest extends BaseTableCRUDTestEnv {

  private DeltaCommitsApi deltaCommitsApi;
  private TableInfo tableInfo;
  private static final String ZERO_UUID = "00000000-0000-0000-0000-000000000000";

  @Override
  protected CatalogOperations createCatalogOperations(ServerConfig config) {
    return new SdkCatalogOperations(TestUtils.createApiClient(config));
  }

  @Override
  protected SchemaOperations createSchemaOperations(ServerConfig config) {
    return new SdkSchemaOperations(TestUtils.createApiClient(config));
  }

  @Override
  protected TableOperations createTableOperations(ServerConfig config) {
    return new SdkTableOperations(TestUtils.createApiClient(config));
  }

  @BeforeEach
  @Override
  public void setUp() {
    super.setUp();
    deltaCommitsApi = new DeltaCommitsApi(TestUtils.createApiClient(serverConfig));
    tableInfo =
        createTestingTable(
            TestUtils.TABLE_NAME, TableType.MANAGED, Optional.empty(), tableOperations);
  }

  private DeltaMetadata deltaMetadataWithTableId(String tableId) {
    Map<String, String> propertiesWithTableId = Map.of("ucTableId", tableId);
    return new DeltaMetadata()
        .properties(new DeltaCommitMetadataProperties().properties(propertiesWithTableId));
  }

  private DeltaCommit createCommitObject(String tableId, Long version, String tableUri) {
    String fileName = version == null ? "file_null" : "file" + version;
    long timestamp = version == null ? 1700000000L : 1700000000L + version;
    return new DeltaCommit()
        .tableId(tableId)
        .tableUri(tableUri)
        .commitInfo(
            new DeltaCommitInfo()
                .version(version)
                .fileName(fileName)
                .fileSize(100L)
                .timestamp(timestamp)
                .fileModificationTimestamp(timestamp))
        .metadata(deltaMetadataWithTableId(tableId));
  }

  private DeltaCommit createBackfillOnlyCommitObject(long latestBackfilledVersion) {
    return new DeltaCommit()
        .tableId(tableInfo.getTableId())
        .tableUri(tableInfo.getStorageLocation())
        .latestBackfilledVersion(latestBackfilledVersion);
  }

  private List<DeltaCommitDAO> getCommitDAOs(UUID tableId) {
    try (Session session = hibernateConfigurator.getSessionFactory().openSession()) {
      String sql =
          "SELECT * FROM uc_delta_commits WHERE table_id = :tableId ORDER BY commit_version DESC";
      Query<DeltaCommitDAO> query = session.createNativeQuery(sql, DeltaCommitDAO.class);
      query.setParameter("tableId", tableId);
      return query.getResultList();
    }
  }

  private DeltaGetCommitsResponse getAllCommits(
      String tableId, String tableUri, Long startVersion, Optional<Long> endVersion)
      throws ApiException {
    return deltaCommitsApi.getCommits(
        new DeltaGetCommits()
            .tableId(tableId)
            .tableUri(tableUri)
            .startVersion(startVersion)
            .endVersion(endVersion.orElse(null)));
  }

  /** Get all the commits of a table and verify that the response has the latest version as
   * {@code expectedLatestTableVersion} and it contains all the {@code expectedCommits} in the
   * exact order.
   *
   * @param expectedLatestTableVersion the expected latestTableVersion in {@code response}
   * @param expectedCommits Can be either a list of {@code DeltaCommit}, or a list of Integer/Long
   *                        representing the version numbers, or empty if it's not expected to
   *                        return any commits.
   */
  private void verifyDeltaCommits(long expectedLatestTableVersion, Object... expectedCommits)
      throws ApiException {
    DeltaGetCommitsResponse response =
        getAllCommits(tableInfo.getTableId(), tableInfo.getStorageLocation(), 0L, Optional.empty());
    verifyDeltaGetCommitsResponse(response, expectedLatestTableVersion, expectedCommits);
  }

  /** Verify that the {@code response} has the latest version as {@code expectedLatestTableVersion}
   * and it contains all the {@code expectedCommits} in the exact order.
   *
   * @param response returned from {@code DeltaGetCommits} rpc
   * @param expectedLatestTableVersion the expected latestTableVersion in {@code response}
   * @param expectedCommits Can be either a list of {@code DeltaCommit}, or a list of Integer/Long
   *                        representing the version numbers, or empty if it's not expected to
   *                        return any commits.
   */
  private void verifyDeltaGetCommitsResponse(
      DeltaGetCommitsResponse response,
      long expectedLatestTableVersion,
      Object... expectedCommits) {
    assertEquals(expectedLatestTableVersion, response.getLatestTableVersion());
    assertEquals(expectedCommits.length, response.getCommits().size());
    for (int i = 0; i < expectedCommits.length; i++) {
      Object expectedCommit = expectedCommits[i];
      if (expectedCommit instanceof DeltaCommit c) {
        assertThat(response.getCommits().get(i)).isEqualTo(c.getCommitInfo());
      } else if (expectedCommit instanceof Long v) {
        assertThat(response.getCommits().get(i).getVersion()).isEqualTo(v);
      } else if (expectedCommit instanceof Integer v) {
        assertThat(response.getCommits().get(i).getVersion()).isEqualTo(v.longValue());
      } else {
        throw new RuntimeException("Unexpected commit type: " + expectedCommit.getClass());
      }
    }
  }

  @Test
  public void testBasicCoordinatedCommitsCRUD() throws ApiException {
    // Get commits on a table with no commits
    verifyDeltaCommits(
        /* expectedLatestTableVersion= */ -1);

    DeltaCommit commit1 =
        createCommitObject(tableInfo.getTableId(), 1L, tableInfo.getStorageLocation());
    deltaCommitsApi.commit(commit1);

    // Commit the same version again, and it would fail
    assertApiException(
        () -> deltaCommitsApi.commit(commit1),
        ErrorCode.ALREADY_EXISTS,
        "Commit version already accepted.");

    // Commit the same version again with a different filename: the same failure
    DeltaCommit commit1_other_filename =
        createCommitObject(tableInfo.getTableId(), 1L, tableInfo.getStorageLocation());
    commit1_other_filename.getCommitInfo().setFileName("some_other_filename_" + UUID.randomUUID());
    assertApiException(
        () -> deltaCommitsApi.commit(commit1_other_filename),
        ErrorCode.ALREADY_EXISTS,
        "Commit version already accepted.");

    DeltaCommit commit3 =
        createCommitObject(tableInfo.getTableId(), 3L, tableInfo.getStorageLocation());

    // Must commit N+1 on top of N. Committing 3 on top of 1 would fail.
    assertApiException(
        () -> deltaCommitsApi.commit(commit3),
        ErrorCode.INVALID_ARGUMENT,
        "Commit version must be the next version after the latest commit");

    // Commit without metadata is fine
    DeltaCommit commit2 =
        createCommitObject(tableInfo.getTableId(), 2L, tableInfo.getStorageLocation())
            .metadata(null);
    deltaCommitsApi.commit(commit2);
    // Then we can commit 3
    deltaCommitsApi.commit(commit3);

    // Commit the old version 1 again, and it would fail
    assertApiException(
        () -> deltaCommitsApi.commit(commit1),
        ErrorCode.ALREADY_EXISTS,
        "Commit version already accepted.");

    // Get commits with wrong table id
    assertApiException(
        () -> getAllCommits(ZERO_UUID, tableInfo.getStorageLocation(), 0L, Optional.empty()),
        ErrorCode.NOT_FOUND,
        "Table not found");

    // Try to get the commits with different start and end version range
    DeltaGetCommitsResponse response =
        getAllCommits(tableInfo.getTableId(), tableInfo.getStorageLocation(), 0L, Optional.empty());
    verifyDeltaGetCommitsResponse(
        response,
        /* expectedLatestTableVersion= */ 3,
        /* expectedCommits= */ commit3, commit2, commit1);

    response =
        getAllCommits(tableInfo.getTableId(), tableInfo.getStorageLocation(), 2L, Optional.empty());
    verifyDeltaGetCommitsResponse(
        response, /* expectedLatestTableVersion= */ 3, /* expectedCommits= */ commit3, commit2);

    response =
        getAllCommits(tableInfo.getTableId(), tableInfo.getStorageLocation(), 2L, Optional.of(2L));
    verifyDeltaGetCommitsResponse(
        response, /* expectedLatestTableVersion= */ 3, /* expectedCommits= */ commit2);

    // Add a new commit (version 4) and backfill up to version 1 in the same request
    DeltaCommit commit4 =
        createCommitObject(tableInfo.getTableId(), 4L, tableInfo.getStorageLocation())
            .latestBackfilledVersion(1L);
    deltaCommitsApi.commit(commit4);
    // Verify we now have 3 commits (versions 2, 3, 4)
    verifyDeltaCommits(
        /* expectedLatestTableVersion= */ 4, /* expectedCommits= */ commit4, commit3, commit2);

    // It's OK to backfill again and nothing changes
    deltaCommitsApi.commit(createBackfillOnlyCommitObject(1L));
    verifyDeltaCommits(
        /* expectedLatestTableVersion= */ 4, /* expectedCommits= */ commit4, commit3, commit2);

    deltaCommitsApi.commit(createBackfillOnlyCommitObject(4L));
    verifyDeltaCommits(
        /* expectedLatestTableVersion= */ 4);

    // Commit one more version before deleting the table
    DeltaCommit commit5 =
        createCommitObject(tableInfo.getTableId(), 5L, tableInfo.getStorageLocation());
    deltaCommitsApi.commit(commit5);
    verifyDeltaCommits(
        /* expectedLatestTableVersion= */ 5, /* expectedCommits= */ commit5);

    // Verify commits are cleaned up upon table deletion
    tableOperations.deleteTable(TestUtils.TABLE_FULL_NAME);
    // Verify the uc_delta_commits table is cleaned up. This has to be done by checking the DAOs
    // because the getCommit api call would fail with NOT_FOUND once the table is gone.
    assertApiException(
        () ->
            getAllCommits(
                tableInfo.getTableId(), tableInfo.getStorageLocation(), 0L, Optional.empty()),
        ErrorCode.NOT_FOUND,
        "Table not found");
    List<DeltaCommitDAO> commitDAOs = getCommitDAOs(UUID.fromString(tableInfo.getTableId()));
    assertThat(commitDAOs.size()).isEqualTo(0);
  }

  private void checkCommitInvalidParameter(
      Long version, Consumer<DeltaCommit> modify, String containsErrorMessage) {
    DeltaCommit commit =
        createCommitObject(tableInfo.getTableId(), version, tableInfo.getStorageLocation());
    modify.accept(commit);
    assertApiException(
        () -> deltaCommitsApi.commit(commit), ErrorCode.INVALID_ARGUMENT, containsErrorMessage);
  }

  private void checkGetCommitInvalidParameter(
      Long startVersion, Optional<Long> endVersion, String containsErrorMessage) {
    assertApiException(
        () ->
            getAllCommits(
                tableInfo.getTableId(), tableInfo.getStorageLocation(), startVersion, endVersion),
        ErrorCode.INVALID_ARGUMENT,
        containsErrorMessage);
  }

  @Test
  public void testInvalidParameters() throws ApiException {
    checkCommitInvalidParameter(1L, c -> c.setTableId(null), "table_id");
    checkCommitInvalidParameter(1L, c -> c.setTableId(""), "table_id");
    // With a valid table ID but table doesn't exist
    DeltaCommit commitWithWrongId =
        createCommitObject(ZERO_UUID, 1L, tableInfo.getStorageLocation());
    assertApiException(
        () -> deltaCommitsApi.commit(commitWithWrongId), ErrorCode.NOT_FOUND, "Table not found");

    checkCommitInvalidParameter(1L, c -> c.setTableUri(null), "table_uri");
    checkCommitInvalidParameter(1L, c -> c.setTableUri(""), "table_uri");
    checkCommitInvalidParameter(-1L, c -> {}, "version");
    checkCommitInvalidParameter(null, c -> {}, "version");
    checkCommitInvalidParameter(0L, c -> {}, "version");
    checkCommitInvalidParameter(
        1L,
        c -> c.commitInfo(null).latestBackfilledVersion(null),
        "Either commit_info or latest_backfilled_version must be defined");
    checkCommitInvalidParameter(1L, c -> c.getCommitInfo().setFileName(null), "file_name");
    checkCommitInvalidParameter(1L, c -> c.getCommitInfo().setFileName(""), "file_name");
    checkCommitInvalidParameter(1L, c -> c.getCommitInfo().setFileSize(null), "file_size");
    checkCommitInvalidParameter(1L, c -> c.getCommitInfo().setFileSize(-100L), "file_size");
    checkCommitInvalidParameter(1L, c -> c.getCommitInfo().setFileSize(0L), "file_size");
    checkCommitInvalidParameter(1L, c -> c.getCommitInfo().setTimestamp(null), "timestamp");
    checkCommitInvalidParameter(1L, c -> c.getCommitInfo().setTimestamp(-1L), "timestamp");
    checkCommitInvalidParameter(1L, c -> c.getCommitInfo().setTimestamp(0L), "timestamp");
    checkCommitInvalidParameter(
        1L,
        c -> c.getCommitInfo().setFileModificationTimestamp(null),
        "file_modification_timestamp");
    checkCommitInvalidParameter(
        1L, c -> c.getCommitInfo().setFileModificationTimestamp(0L), "file_modification_timestamp");
    checkCommitInvalidParameter(
        1L,
        c -> c.getCommitInfo().setFileModificationTimestamp(-1L),
        "file_modification_timestamp");

    // Commit with properties but without ucTableId
    checkCommitInvalidParameter(
        1L,
        c ->
            c.getMetadata()
                .setProperties(
                    new DeltaCommitMetadataProperties()
                        .properties(Map.of("randomProperty", "randomValue"))),
        "commit does not contain ucTableId in the properties.");

    // Commit with metadata but without ucTableId in properties
    Map<String, String> properties1 = Map.of("customProperty", "customValue");
    DeltaMetadata metadata1 =
        new DeltaMetadata().properties(new DeltaCommitMetadataProperties().properties(properties1));
    checkCommitInvalidParameter(1L, c -> c.setMetadata(metadata1), "ucTableId");
    // Commit with metadata but with wrong ucTableId
    checkCommitInvalidParameter(
        1L,
        c -> c.setMetadata(deltaMetadataWithTableId(ZERO_UUID)),
        "does not match the properties ucTableId");

    // Commit version 1 successfully
    DeltaCommit commit1 =
        createCommitObject(tableInfo.getTableId(), 1L, tableInfo.getStorageLocation());
    deltaCommitsApi.commit(commit1);

    // Try to commit version 3 when version 2 hasn't been committed yet
    checkCommitInvalidParameter(
        3L,
        c -> {},
        "Commit version must be the next version after the latest commit 1, but got 3");

    checkCommitInvalidParameter(2L, c -> c.setTableUri(""), "table_uri");
    checkCommitInvalidParameter(
        2L,
        c -> c.setTableUri("s3://wrong-bucket/wrong-path"),
        "Table URI in commit s3://wrong-bucket/wrong-path does not match the table path");

    // Commit version 2 successfully even when the table URI isn't perfect standard
    DeltaCommit commit2 =
        createCommitObject(tableInfo.getTableId(), 2L, tableInfo.getStorageLocation());
    assertThat(commit2.getTableUri()).contains("file:///");
    commit2.setTableUri(commit2.getTableUri().replace("file:///", "file:/"));
    deltaCommitsApi.commit(commit2);

    // Commit to external table is not supported
    String tablePath = "/tmp/" + UUID.randomUUID();
    TableInfo tableInfoExternal =
        createTestingTable(
            TestUtils.TABLE_NAME + "_external",
            TableType.EXTERNAL,
            Optional.of(tablePath),
            tableOperations);
    checkCommitInvalidParameter(
        1L,
        c ->
            c.tableId(tableInfoExternal.getTableId())
                .metadata(deltaMetadataWithTableId(tableInfoExternal.getTableId())),
        "Only managed tables are supported for Delta commits");

    // Commit to non-Delta table is not supported either. But we can not create a managed table
    // with non-Delta format at all.

    checkGetCommitInvalidParameter(null, Optional.empty(), "start_version");
    checkGetCommitInvalidParameter(-1L, Optional.empty(), "start_version");
    checkGetCommitInvalidParameter(
        5L, Optional.of(3L), "end_version must be >=start_version if set");
    assertApiException(
        () -> getAllCommits(null, tableInfo.getStorageLocation(), 0L, Optional.empty()),
        ErrorCode.INVALID_ARGUMENT,
        "table_id");
  }

  @Test
  public void testBackfillCommitAtDifferentVersions() throws ApiException {
    // Backfill when there's no commit
    assertApiException(
        () -> deltaCommitsApi.commit(createBackfillOnlyCommitObject(1L)),
        ErrorCode.INVALID_ARGUMENT,
        "Field can not be null: commit_info in onboarding commit");

    // Create 5 commits
    for (long i = 1; i <= 5; i++) {
      DeltaCommit commit =
          createCommitObject(tableInfo.getTableId(), i, tableInfo.getStorageLocation());
      deltaCommitsApi.commit(commit);
    }

    // Verify all 5 commits exist
    verifyDeltaCommits(
        /* expectedLatestTableVersion= */ 5, /* expectedCommits= */ 5, 4, 3, 2, 1);

    // Backfill with metadata should fail
    DeltaCommit backfillCommit2WithMetadata =
        createBackfillOnlyCommitObject(2L)
            .metadata(deltaMetadataWithTableId(tableInfo.getTableId()));
    assertApiException(
        () -> deltaCommitsApi.commit(backfillCommit2WithMetadata),
        ErrorCode.INVALID_ARGUMENT,
        "metadata shouldn't be set for backfill only commit");

    // Backfill up to version 2 (should keep versions 3, 4, 5)
    deltaCommitsApi.commit(createBackfillOnlyCommitObject(2L));

    // Verify versions 3, 4, 5 are present
    verifyDeltaCommits(
        /* expectedLatestTableVersion= */ 5, /* expectedCommits= */ 5, 4, 3);

    // Try to backfill beyond the latest version
    assertApiException(
        () -> deltaCommitsApi.commit(createBackfillOnlyCommitObject(9L)),
        ErrorCode.INVALID_ARGUMENT,
        "Should not backfill version 9 while the last version committed is 5");

    // Try to backfill beyond the latest version while commiting new version
    DeltaCommit commit6WithBackfill =
        createCommitObject(tableInfo.getTableId(), 6L, tableInfo.getStorageLocation())
            .latestBackfilledVersion(9L);
    assertApiException(
        () -> deltaCommitsApi.commit(commit6WithBackfill),
        ErrorCode.INVALID_ARGUMENT,
        "Latest backfilled version 9 cannot be greater than the last commit version = 5");
    commit6WithBackfill.setLatestBackfilledVersion(6L);
    assertApiException(
        () -> deltaCommitsApi.commit(commit6WithBackfill),
        ErrorCode.INVALID_ARGUMENT,
        "Latest backfilled version 6 cannot be greater than the last commit version = 5");

    // Backfill up to version 4 (should keep only version 5)
    deltaCommitsApi.commit(createBackfillOnlyCommitObject(4L));
    verifyDeltaCommits(
        /* expectedLatestTableVersion= */ 5, /* expectedCommits= */ 5);

    // Backfill up to version 5 (the latest)
    deltaCommitsApi.commit(createBackfillOnlyCommitObject(5L));
    // The commit should be marked as backfilled, so no commits should be returned
    verifyDeltaCommits(
        /* expectedLatestTableVersion= */ 5);

    // Commit v6. Now we have [v5(backfilled), v6] in DB. Get commits only returns v6.
    DeltaCommit commit6 =
        createCommitObject(tableInfo.getTableId(), 6L, tableInfo.getStorageLocation());
    deltaCommitsApi.commit(commit6);
    verifyDeltaCommits(
        /* expectedLatestTableVersion= */ 6, /* expectedCommits= */ 6);
  }

  @Test
  public void testCommitLimit() throws ApiException {
    // MAX_NUM_COMMITS_PER_TABLE is 10, so we'll try to add 10 commits
    // First add 10 commits - should succeed
    for (long i = 1; i <= 10; i++) {
      DeltaCommit commit =
          createCommitObject(tableInfo.getTableId(), i, tableInfo.getStorageLocation());
      deltaCommitsApi.commit(commit);
    }

    // Verify we now have 10 commits (1~10)
    verifyDeltaCommits(
        /* expectedLatestTableVersion= */ 10, /* expectedCommits= */ 10, 9, 8, 7, 6, 5, 4, 3, 2, 1);

    // Then try to add the 11th commit - should fail
    DeltaCommit commit11 =
        createCommitObject(tableInfo.getTableId(), 11L, tableInfo.getStorageLocation());
    assertApiException(
        () -> deltaCommitsApi.commit(commit11),
        ErrorCode.RESOURCE_EXHAUSTED,
        "Max number of commits per table reached");

    // Backfill the first 6 commits
    deltaCommitsApi.commit(createBackfillOnlyCommitObject(6L));

    // Verify we now have 4 commits (7~10)
    verifyDeltaCommits(
        /* expectedLatestTableVersion= */ 10, /* expectedCommits= */ 10, 9, 8, 7);

    // Now we should be able to add more commits
    for (long i = 11; i <= 14; i++) {
      DeltaCommit commit =
          createCommitObject(tableInfo.getTableId(), i, tableInfo.getStorageLocation());
      deltaCommitsApi.commit(commit);
    }

    // Verify we now have 8 commits (7~14)
    verifyDeltaCommits(
        /* expectedLatestTableVersion= */ 14, /* expectedCommits= */ 14, 13, 12, 11, 10, 9, 8, 7);

    // Backfill all commits again
    deltaCommitsApi.commit(createBackfillOnlyCommitObject(14L));

    // Verify there's no unbackfilled commits
    verifyDeltaCommits(
        /* expectedLatestTableVersion= */ 14);

    // Now we should be able to add another 10 commits
    for (long i = 15; i <= 24; i++) {
      DeltaCommit commit =
          createCommitObject(tableInfo.getTableId(), i, tableInfo.getStorageLocation());
      deltaCommitsApi.commit(commit);
    }
  }

  @Test
  public void testPagination() throws ApiException {
    // Create 10 commits
    for (long i = 1; i <= 10; i++) {
      deltaCommitsApi.commit(
          createCommitObject(tableInfo.getTableId(), i, tableInfo.getStorageLocation()));
    }

    // Get commits with pagination: start_version=0, end_version=3
    DeltaGetCommitsResponse response =
        getAllCommits(tableInfo.getTableId(), tableInfo.getStorageLocation(), 0L, Optional.of(3L));
    verifyDeltaGetCommitsResponse(
        response, /* expectedLatestTableVersion= */ 10, /* expectedCommits= */ 3, 2, 1);

    // Get next page: start_version=4, end_version=7
    response =
        getAllCommits(tableInfo.getTableId(), tableInfo.getStorageLocation(), 4L, Optional.of(7L));
    verifyDeltaGetCommitsResponse(
        response, /* expectedLatestTableVersion= */ 10, /* expectedCommits= */ 7, 6, 5, 4);

    // Get last page: start_version=8, end_version=null
    response =
        getAllCommits(tableInfo.getTableId(), tableInfo.getStorageLocation(), 8L, Optional.empty());
    verifyDeltaGetCommitsResponse(
        response, /* expectedLatestTableVersion= */ 10, /* expectedCommits= */ 10, 9, 8);

    // Get commits starting from a version that doesn't exist yet
    response =
        getAllCommits(
            tableInfo.getTableId(), tableInfo.getStorageLocation(), 11L, Optional.empty());
    verifyDeltaGetCommitsResponse(
        response, /* expectedLatestTableVersion= */ 10);

    // Get commits with start_version = end_version
    response =
        getAllCommits(tableInfo.getTableId(), tableInfo.getStorageLocation(), 3L, Optional.of(3L));
    verifyDeltaGetCommitsResponse(
        response, /* expectedLatestTableVersion= */ 10, /* expectedCommits= */ 3);
  }

  @Test
  public void testCommitWithMetadata() throws ApiException {
    // Commit with metadata properties
    Map<String, String> properties1 = new HashMap<>();
    properties1.put("ucTableId", tableInfo.getTableId());
    properties1.put("customProperty", "customValue");
    properties1.put("delta.feature.test", "enabled");

    DeltaCommitMetadataProperties metadataProperties =
        new DeltaCommitMetadataProperties().properties(properties1);
    DeltaMetadata metadata = new DeltaMetadata().properties(metadataProperties);

    DeltaCommit commit =
        createCommitObject(tableInfo.getTableId(), 1L, tableInfo.getStorageLocation())
            .metadata(metadata);
    deltaCommitsApi.commit(commit);

    // Retrieve the table and verify properties were updated
    TableInfo updatedTable1 = tableOperations.getTable(TestUtils.TABLE_FULL_NAME);
    assertNotNull(updatedTable1.getProperties());
    assertEquals(tableInfo.getTableId(), updatedTable1.getProperties().get("ucTableId"));
    assertEquals("customValue", updatedTable1.getProperties().get("customProperty"));
    assertEquals("enabled", updatedTable1.getProperties().get("delta.feature.test"));

    // Commit again with only a description change
    commit =
        createCommitObject(tableInfo.getTableId(), 2L, tableInfo.getStorageLocation())
            .metadata(new DeltaMetadata().description("Updated table description via commit"));
    deltaCommitsApi.commit(commit);

    // Retrieve the table and verify that only description was updated.
    TableInfo updatedTable2 = tableOperations.getTable(TestUtils.TABLE_FULL_NAME);
    assertEquals("Updated table description via commit", updatedTable2.getComment());
    assertEquals(updatedTable1.getProperties(), updatedTable2.getProperties());
    assertEquals(updatedTable1.getColumns(), updatedTable2.getColumns());

    ColumnInfo newColumn1 =
        new ColumnInfo()
            .name("new_column_1")
            .typeName(ColumnTypeName.STRING)
            .typeText("string")
            .typeJson("{\"type\":\"string\"}")
            .position(0)
            .nullable(true);
    ColumnInfo newColumn2 =
        new ColumnInfo()
            .name("new_column_2")
            .typeName(ColumnTypeName.INT)
            .typeText("int")
            .typeJson("{\"type\":\"integer\"}")
            .position(1)
            .nullable(false);
    ColumnInfos columnInfos =
        new ColumnInfos().addColumnsItem(newColumn1).addColumnsItem(newColumn2);
    commit =
        createCommitObject(tableInfo.getTableId(), 3L, tableInfo.getStorageLocation())
            .metadata(new DeltaMetadata().schema(columnInfos));
    deltaCommitsApi.commit(commit);

    // Retrieve the table and verify schema was updated
    TableInfo updatedTable3 = tableOperations.getTable(TestUtils.TABLE_FULL_NAME);
    assertNotNull(updatedTable3.getColumns());
    assertEquals(2, updatedTable3.getColumns().size());
    assertEquals("new_column_1", updatedTable3.getColumns().get(0).getName());
    assertEquals("new_column_2", updatedTable3.getColumns().get(1).getName());
    assertEquals(updatedTable2.getProperties(), updatedTable3.getProperties());
    assertEquals(updatedTable2.getComment(), updatedTable3.getComment());

    // Commit with all metadata fields
    Map<String, String> properties2 = new HashMap<>();
    properties2.put("ucTableId", tableInfo.getTableId());
    properties2.put("customProperty", "customValueNew");
    metadataProperties = new DeltaCommitMetadataProperties().properties(properties2);
    columnInfos =
        new ColumnInfos()
            .addColumnsItem(
                new ColumnInfo()
                    .name("test_column_new")
                    .typeName(ColumnTypeName.STRING)
                    .typeText("string")
                    .typeJson("{\"type\":\"string\"}")
                    .position(0)
                    .nullable(true)
                    .comment("Test column comment new"));
    metadata =
        new DeltaMetadata()
            .properties(metadataProperties)
            .description("Complete metadata test table")
            .schema(columnInfos);
    commit =
        createCommitObject(tableInfo.getTableId(), 4L, tableInfo.getStorageLocation())
            .metadata(metadata);
    deltaCommitsApi.commit(commit);

    // Retrieve the table and verify all metadata was updated
    TableInfo updatedTable4 = tableOperations.getTable(TestUtils.TABLE_FULL_NAME);
    assertEquals("Complete metadata test table", updatedTable4.getComment());
    assertNotNull(updatedTable4.getProperties());
    assertFalse(updatedTable4.getProperties().containsKey("delta.feature.test")); // removed
    assertEquals("customValueNew", updatedTable4.getProperties().get("customProperty"));
    assertNotNull(updatedTable4.getColumns());
    assertEquals(1, updatedTable4.getColumns().size());
    assertEquals("test_column_new", updatedTable4.getColumns().get(0).getName());
    assertEquals("Test column comment new", updatedTable4.getColumns().get(0).getComment());

    // Commit with empty metadata (no properties, description, or schema)
    checkCommitInvalidParameter(
        5L,
        c -> c.setMetadata(new DeltaMetadata()),
        "At least one of description, properties, or schema must be set in commit.metadata");
  }
}
