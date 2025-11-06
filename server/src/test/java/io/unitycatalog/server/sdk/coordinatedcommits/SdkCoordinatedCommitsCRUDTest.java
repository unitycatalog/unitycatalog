package io.unitycatalog.server.sdk.coordinatedcommits;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.CoordinatedCommitsApi;
import io.unitycatalog.client.model.ColumnInfo;
import io.unitycatalog.client.model.ColumnInfos;
import io.unitycatalog.client.model.ColumnTypeName;
import io.unitycatalog.client.model.Commit;
import io.unitycatalog.client.model.CommitInfo;
import io.unitycatalog.client.model.CommitMetadataProperties;
import io.unitycatalog.client.model.Metadata;
import io.unitycatalog.client.model.TableInfo;
import io.unitycatalog.client.model.TableType;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.catalog.CatalogOperations;
import io.unitycatalog.server.base.schema.SchemaOperations;
import io.unitycatalog.server.base.table.BaseTableCRUDTestEnv;
import io.unitycatalog.server.base.table.TableOperations;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.persist.dao.CommitDAO;
import io.unitycatalog.server.sdk.catalog.SdkCatalogOperations;
import io.unitycatalog.server.sdk.schema.SdkSchemaOperations;
import io.unitycatalog.server.sdk.tables.SdkTableOperations;
import io.unitycatalog.server.utils.TestUtils;
import java.util.ArrayList;
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

public class SdkCoordinatedCommitsCRUDTest extends BaseTableCRUDTestEnv {

  private CoordinatedCommitsApi coordinatedCommitsApi;
  private TableInfo tableInfo;

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
    coordinatedCommitsApi = new CoordinatedCommitsApi(TestUtils.createApiClient(serverConfig));
    tableInfo =
        createTestingTable(
            TestUtils.TABLE_NAME, TableType.MANAGED, Optional.empty(), tableOperations);
  }

  private Commit createCommitObject(String tableId, Long version, String tableUri) {
    return new Commit()
        .tableId(tableId)
        .tableUri(tableUri)
        .commitInfo(
            new CommitInfo()
                .version(version)
                .fileName("file" + version)
                .fileSize(100L)
                .timestamp(1700000000L + version)
                .fileModificationTimestamp(1700000000L + version))
        .metadata(
            new Metadata()
                .properties(
                    new CommitMetadataProperties().properties(Map.of("ucTableId", tableId))));
  }

  private Commit createBackfillOnlyCommitObject(
      String tableId, Long latestBackfilledVersion, String tableUri) {
    return new Commit()
        .tableId(tableId)
        .tableUri(tableUri)
        .latestBackfilledVersion(latestBackfilledVersion);
  }

  private List<CommitDAO> getCommitDAOs(UUID tableId) {
    try (Session session = hibernateConfigurator.getSessionFactory().openSession()) {
      String sql =
          "SELECT * FROM uc_commits WHERE table_id = :tableId ORDER BY commit_version DESC";
      Query<CommitDAO> query = session.createNativeQuery(sql, CommitDAO.class);
      query.setParameter("tableId", tableId);
      return query.getResultList();
    }
  }

  private void checkCommitDAO(CommitDAO dao, Commit commit) {
    CommitInfo commitInfo = commit.getCommitInfo();
    assertThat(commitInfo).isNotNull();
    assertThat(dao).isNotNull();
    assertThat(dao.getCommitVersion()).isEqualTo(commitInfo.getVersion());
    assertThat(dao.getCommitFilename()).isEqualTo(commitInfo.getFileName());
    assertThat(dao.getCommitFilesize()).isEqualTo(commitInfo.getFileSize());
    assertThat(dao.getCommitFileModificationTimestamp().getTime())
        .isEqualTo(commitInfo.getFileModificationTimestamp());
  }

  @Test
  public void testBasicCoordinatedCommitsCRUD() throws ApiException {
    // It is a table with no commits
    assertThat(getCommitDAOs(UUID.fromString(tableInfo.getTableId())).size()).isEqualTo(0);

    Commit commit1 = createCommitObject(tableInfo.getTableId(), 1L, tableInfo.getStorageLocation());
    coordinatedCommitsApi.commit(commit1);

    // Commit the same version again, and it would fail
    ApiException ex = assertThrows(ApiException.class, () -> coordinatedCommitsApi.commit(commit1));
    assertThat(ex.getCode()).isEqualTo(ErrorCode.ALREADY_EXISTS.getHttpStatus().code());
    assertThat(ex.getMessage()).contains("Commit version already accepted.");

    // Commit the same version again with a different filename: the same failure
    Commit commit1_other_filename =
        createCommitObject(tableInfo.getTableId(), 1L, tableInfo.getStorageLocation());
    commit1_other_filename.getCommitInfo().setFileName("some_other_filename_" + UUID.randomUUID());
    ex =
        assertThrows(
            ApiException.class, () -> coordinatedCommitsApi.commit(commit1_other_filename));
    assertThat(ex.getCode()).isEqualTo(ErrorCode.ALREADY_EXISTS.getHttpStatus().code());
    assertThat(ex.getMessage()).contains("Commit version already accepted.");

    Commit commit3 = createCommitObject(tableInfo.getTableId(), 3L, tableInfo.getStorageLocation());

    // Must commit N+1 on top of N. Committing 3 on top of 1 would fail.
    ex = assertThrows(ApiException.class, () -> coordinatedCommitsApi.commit(commit3));
    assertThat(ex.getCode()).isEqualTo(ErrorCode.INVALID_ARGUMENT.getHttpStatus().code());
    assertThat(ex.getMessage())
        .contains("Commit version must be the next version after the latest commit");

    Commit commit2 = createCommitObject(tableInfo.getTableId(), 2L, tableInfo.getStorageLocation());
    coordinatedCommitsApi.commit(commit2);
    coordinatedCommitsApi.commit(commit3);

    // Commit the old version 1 again, and it would fail
    ex = assertThrows(ApiException.class, () -> coordinatedCommitsApi.commit(commit1));
    assertThat(ex.getCode()).isEqualTo(ErrorCode.ALREADY_EXISTS.getHttpStatus().code());
    assertThat(ex.getMessage()).contains("Commit version already accepted.");

    List<CommitDAO> commitDAOs = getCommitDAOs(UUID.fromString(tableInfo.getTableId()));
    assertThat(commitDAOs.size()).isEqualTo(3);
    checkCommitDAO(commitDAOs.get(0), commit3);
    checkCommitDAO(commitDAOs.get(1), commit2);
    checkCommitDAO(commitDAOs.get(2), commit1);

    // Add a new commit (version 4) and backfill up to version 1 in the same request
    Commit commit4 = createCommitObject(tableInfo.getTableId(), 4L, tableInfo.getStorageLocation());
    commit4.setLatestBackfilledVersion(1L);
    coordinatedCommitsApi.commit(commit4);
    // Verify we now have 3 commits (versions 2, 3, 4)
    commitDAOs = getCommitDAOs(UUID.fromString(tableInfo.getTableId()));
    assertThat(commitDAOs.size()).isEqualTo(3);
    checkCommitDAO(commitDAOs.get(0), commit4);
    checkCommitDAO(commitDAOs.get(1), commit3);
    checkCommitDAO(commitDAOs.get(2), commit2);

    // It's OK to backfill again and nothing changes
    Commit backfillOnlyCommit1 =
        createBackfillOnlyCommitObject(tableInfo.getTableId(), 1L, tableInfo.getStorageLocation());
    coordinatedCommitsApi.commit(backfillOnlyCommit1);
    commitDAOs = getCommitDAOs(UUID.fromString(tableInfo.getTableId()));
    assertThat(commitDAOs.size()).isEqualTo(3);

    Commit backfillOnlyCommit2 =
        createBackfillOnlyCommitObject(tableInfo.getTableId(), 4L, tableInfo.getStorageLocation());
    coordinatedCommitsApi.commit(backfillOnlyCommit2);
    commitDAOs = getCommitDAOs(UUID.fromString(tableInfo.getTableId()));
    assertThat(commitDAOs.size()).isEqualTo(1);
    checkCommitDAO(commitDAOs.get(0), commit4);
    assertThat(commitDAOs.get(0).getIsBackfilledLatestCommit()).isTrue();

    // Verify commits are cleaned up upon table deletion
    Commit commit5 = createCommitObject(tableInfo.getTableId(), 5L, tableInfo.getStorageLocation());
    coordinatedCommitsApi.commit(commit5);
    commitDAOs = getCommitDAOs(UUID.fromString(tableInfo.getTableId()));
    assertThat(commitDAOs.size()).isEqualTo(2);
    // Delete the table
    tableOperations.deleteTable(TestUtils.TABLE_FULL_NAME);
    // Verify the uc_commits table is cleaned up
    commitDAOs = getCommitDAOs(UUID.fromString(tableInfo.getTableId()));
    assertThat(commitDAOs.size()).isEqualTo(0);
  }

  private void checkCommitInvalidParameter(
      long version, Consumer<Commit> modify, String containsErrorMessage) {
    Commit commit =
        createCommitObject(tableInfo.getTableId(), version, tableInfo.getStorageLocation());
    modify.accept(commit);
    ApiException ex = assertThrows(ApiException.class, () -> coordinatedCommitsApi.commit(commit));
    assertThat(ex.getCode()).isEqualTo(ErrorCode.INVALID_ARGUMENT.getHttpStatus().code());
    assertThat(ex.getMessage()).contains(containsErrorMessage);
  }

  @Test
  public void testInvalidParameters() throws ApiException {
    checkCommitInvalidParameter(1L, c -> c.setTableId(null), "table_id");
    checkCommitInvalidParameter(1L, c -> c.setTableId(""), "table_id");
    checkCommitInvalidParameter(1L, c -> c.setTableUri(null), "table_uri");
    checkCommitInvalidParameter(1L, c -> c.setTableUri(""), "table_uri");
    checkCommitInvalidParameter(-1L, c -> {}, "version");
    checkCommitInvalidParameter(0L, c -> {}, "version");
    checkCommitInvalidParameter(
        1L,
        c -> c.commitInfo(null).latestBackfilledVersion(null),
        "Either commit_info or latest_backfilled_version must be defined");
    checkCommitInvalidParameter(1L, c -> c.getCommitInfo().setFileName(null), "file_name");
    checkCommitInvalidParameter(1L, c -> c.getCommitInfo().setFileName(""), "file_name");
    checkCommitInvalidParameter(1L, c -> c.getCommitInfo().setFileSize(-100L), "file_size");
    checkCommitInvalidParameter(1L, c -> c.getCommitInfo().setFileSize(0L), "file_size");
    checkCommitInvalidParameter(1L, c -> c.getCommitInfo().setTimestamp(-1L), "timestamp");
    checkCommitInvalidParameter(1L, c -> c.getCommitInfo().setTimestamp(0L), "timestamp");

    // Create a commit with metadata but without ucTableId in properties
    Map<String, String> properties1 = Map.of("customProperty", "customValue");
    Metadata metadata1 =
        new Metadata().properties(new CommitMetadataProperties().properties(properties1));
    checkCommitInvalidParameter(1L, c -> c.setMetadata(metadata1), "ucTableId");
    // Create a commit with metadata but with wrong ucTableId
    Map<String, String> properties2 = Map.of("ucTableId", "00000000-0000-0000-0000-000000000000");
    Metadata metadata2 =
        new Metadata().properties(new CommitMetadataProperties().properties(properties2));
    checkCommitInvalidParameter(
        1L, c -> c.setMetadata(metadata2), "does not match the properties ucTableId");

    // Commit version 1 successfully
    Commit commit1 = createCommitObject(tableInfo.getTableId(), 1L, tableInfo.getStorageLocation());
    coordinatedCommitsApi.commit(commit1);

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
    Commit commit2 = createCommitObject(tableInfo.getTableId(), 2L, tableInfo.getStorageLocation());
    assertThat(commit2.getTableUri()).contains("file:///");
    commit2.setTableUri(commit2.getTableUri().replace("file:///", "file:/"));
    coordinatedCommitsApi.commit(commit2);
  }

  @Test
  public void testBackfillCommitAtDifferentVersions() throws ApiException {
    TableInfo tableInfo =
        createTestingTable(
            "test_backfill_versions", TableType.MANAGED, Optional.empty(), tableOperations);

    // Create 5 commits
    ArrayList<Commit> commits = new ArrayList<>();
    for (long i = 1; i <= 5; i++) {
      Commit commit = createCommitObject(tableInfo.getTableId(), i, tableInfo.getStorageLocation());
      commits.add(commit);
      coordinatedCommitsApi.commit(commit);
    }

    // Verify all 5 commits exist
    List<CommitDAO> commitDAOs = getCommitDAOs(UUID.fromString(tableInfo.getTableId()));
    assertThat(commitDAOs.size()).isEqualTo(5);

    // Backfill up to version 2 (should keep versions 3, 4, 5)
    Commit backfillCommit =
        createBackfillOnlyCommitObject(tableInfo.getTableId(), 2L, tableInfo.getStorageLocation());
    coordinatedCommitsApi.commit(backfillCommit);

    commitDAOs = getCommitDAOs(UUID.fromString(tableInfo.getTableId()));
    assertThat(commitDAOs.size()).isEqualTo(3);
    checkCommitDAO(commitDAOs.get(0), commits.get(4));
    checkCommitDAO(commitDAOs.get(1), commits.get(3));
    checkCommitDAO(commitDAOs.get(2), commits.get(2));

    // Try to backfill beyond the latest version
    Commit backfillCommit2 =
        createBackfillOnlyCommitObject(tableInfo.getTableId(), 9L, tableInfo.getStorageLocation());
    ApiException ex =
        assertThrows(ApiException.class, () -> coordinatedCommitsApi.commit(backfillCommit2));
    assertThat(ex.getMessage())
        .contains("Should not backfill version 9 while the last version committed is 5");

    // Backfill up to version 4 (should keep only version 5)
    backfillCommit =
        createBackfillOnlyCommitObject(tableInfo.getTableId(), 4L, tableInfo.getStorageLocation());
    coordinatedCommitsApi.commit(backfillCommit);

    commitDAOs = getCommitDAOs(UUID.fromString(tableInfo.getTableId()));
    assertThat(commitDAOs.size()).isEqualTo(1);
    checkCommitDAO(commitDAOs.get(0), commits.get(4));

    // Backfill up to version 5 (the latest)
    backfillCommit =
        createBackfillOnlyCommitObject(tableInfo.getTableId(), 5L, tableInfo.getStorageLocation());
    coordinatedCommitsApi.commit(backfillCommit);

    commitDAOs = getCommitDAOs(UUID.fromString(tableInfo.getTableId()));
    assertThat(commitDAOs.size()).isEqualTo(1);
    checkCommitDAO(commitDAOs.get(0), commits.get(4));
    assertThat(commitDAOs.get(0).getIsBackfilledLatestCommit()).isTrue();
  }

  @Test
  public void testCommitLimit() throws ApiException {
    TableInfo tableInfo =
        createTestingTable(
            "test_commit_limit", TableType.MANAGED, Optional.empty(), tableOperations);

    // MAX_NUM_COMMITS_PER_TABLE is 50, so we'll try to add 51 commits
    // First add 50 commits - should succeed
    for (long i = 1; i <= 50; i++) {
      Commit commit = createCommitObject(tableInfo.getTableId(), i, tableInfo.getStorageLocation());
      coordinatedCommitsApi.commit(commit);
    }

    // Try to add the 51st commit - should fail
    Commit commit51 =
        createCommitObject(tableInfo.getTableId(), 51L, tableInfo.getStorageLocation());
    ApiException ex =
        assertThrows(ApiException.class, () -> coordinatedCommitsApi.commit(commit51));
    assertThat(ex.getCode()).isEqualTo(ErrorCode.RESOURCE_EXHAUSTED.getHttpStatus().code());
    assertThat(ex.getMessage()).contains("Max number of commits per table reached");

    // Backfill the first 30 commits
    Commit backfillCommit =
        createBackfillOnlyCommitObject(tableInfo.getTableId(), 30L, tableInfo.getStorageLocation());
    coordinatedCommitsApi.commit(backfillCommit);

    // Verify we now have 20 commits (31-50)
    List<CommitDAO> commitDAOs = getCommitDAOs(UUID.fromString(tableInfo.getTableId()));
    assertThat(commitDAOs.size()).isEqualTo(20);

    // Now we should be able to add more commits
    for (long i = 51; i <= 60; i++) {
      Commit commit = createCommitObject(tableInfo.getTableId(), i, tableInfo.getStorageLocation());
      coordinatedCommitsApi.commit(commit);
    }

    // Verify we now have 30 commits (31-60)
    commitDAOs = getCommitDAOs(UUID.fromString(tableInfo.getTableId()));
    assertThat(commitDAOs.size()).isEqualTo(30);
  }

  @Test
  public void testCommitWithMetadata() throws ApiException {
    TableInfo tableInfo =
        createTestingTable("test_metadata", TableType.MANAGED, Optional.empty(), tableOperations);
    String fullTableName =
        tableInfo.getCatalogName() + "." + tableInfo.getSchemaName() + "." + tableInfo.getName();

    // Create a commit with metadata properties
    Map<String, String> properties = new HashMap<>();
    properties.put("ucTableId", tableInfo.getTableId());
    properties.put("customProperty", "customValue");
    properties.put("delta.feature.test", "enabled");

    CommitMetadataProperties metadataProperties =
        new CommitMetadataProperties().properties(properties);
    Metadata metadata = new Metadata().properties(metadataProperties);

    Commit commit =
        createCommitObject(tableInfo.getTableId(), 1L, tableInfo.getStorageLocation())
            .metadata(metadata);
    coordinatedCommitsApi.commit(commit);

    // Retrieve the table and verify properties were updated
    TableInfo updatedTable = tableOperations.getTable(fullTableName);
    assertNotNull(updatedTable.getProperties());
    assertEquals("customValue", updatedTable.getProperties().get("customProperty"));
    assertEquals("enabled", updatedTable.getProperties().get("delta.feature.test"));

    commit = createCommitObject(tableInfo.getTableId(), 2L, tableInfo.getStorageLocation());
    commit.getMetadata().setDescription("Updated table description via commit");
    coordinatedCommitsApi.commit(commit);

    // Retrieve the table and verify description was updated
    updatedTable = tableOperations.getTable(fullTableName);
    assertEquals("Updated table description via commit", updatedTable.getComment());

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
    commit = createCommitObject(tableInfo.getTableId(), 3L, tableInfo.getStorageLocation());
    commit.getMetadata().setSchema(columnInfos);
    coordinatedCommitsApi.commit(commit);

    // Retrieve the table and verify schema was updated
    updatedTable = tableOperations.getTable(fullTableName);
    assertNotNull(updatedTable.getColumns());
    assertEquals(2, updatedTable.getColumns().size());
    assertEquals("new_column_1", updatedTable.getColumns().get(0).getName());
    assertEquals("new_column_2", updatedTable.getColumns().get(1).getName());

    // Create a commit with all metadata fields
    properties = new HashMap<>();
    properties.put("ucTableId", tableInfo.getTableId());
    properties.put("customProperty", "customValueNew");
    metadataProperties = new CommitMetadataProperties().properties(properties);
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
        new Metadata()
            .properties(metadataProperties)
            .description("Complete metadata test table")
            .schema(columnInfos);
    commit =
        createCommitObject(tableInfo.getTableId(), 4L, tableInfo.getStorageLocation())
            .metadata(metadata);
    coordinatedCommitsApi.commit(commit);

    // Retrieve the table and verify all metadata was updated
    updatedTable = tableOperations.getTable(fullTableName);
    assertEquals("Complete metadata test table", updatedTable.getComment());
    assertNotNull(updatedTable.getProperties());
    assertFalse(updatedTable.getProperties().containsKey("delta.feature.test")); // removed
    assertEquals("customValueNew", updatedTable.getProperties().get("customProperty"));
    assertNotNull(updatedTable.getColumns());
    assertEquals(1, updatedTable.getColumns().size());
    assertEquals("test_column_new", updatedTable.getColumns().get(0).getName());
    assertEquals("Test column comment new", updatedTable.getColumns().get(0).getComment());

    // Create a commit with empty metadata (no properties, description, or schema)
    checkCommitInvalidParameter(
        5L,
        c -> c.setMetadata(new Metadata()),
        "At least one of description, properties, or schema must be set in commit.metadata");
  }
}
