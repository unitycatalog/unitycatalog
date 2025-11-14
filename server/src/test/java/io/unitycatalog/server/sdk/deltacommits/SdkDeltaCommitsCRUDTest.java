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

  private DeltaCommit createBackfillOnlyCommitObject(
      String tableId, Long latestBackfilledVersion, String tableUri) {
    return new DeltaCommit()
        .tableId(tableId)
        .tableUri(tableUri)
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

  private void checkCommitDAO(DeltaCommitDAO dao, DeltaCommit commit) {
    DeltaCommitInfo commitInfo = commit.getCommitInfo();
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

    List<DeltaCommitDAO> commitDAOs = getCommitDAOs(UUID.fromString(tableInfo.getTableId()));
    assertThat(commitDAOs.size()).isEqualTo(3);
    checkCommitDAO(commitDAOs.get(0), commit3);
    checkCommitDAO(commitDAOs.get(1), commit2);
    checkCommitDAO(commitDAOs.get(2), commit1);

    // Add a new commit (version 4) and backfill up to version 1 in the same request
    DeltaCommit commit4 =
        createCommitObject(tableInfo.getTableId(), 4L, tableInfo.getStorageLocation())
            .latestBackfilledVersion(1L);
    deltaCommitsApi.commit(commit4);
    // Verify we now have 3 commits (versions 2, 3, 4)
    commitDAOs = getCommitDAOs(UUID.fromString(tableInfo.getTableId()));
    assertThat(commitDAOs.size()).isEqualTo(3);
    checkCommitDAO(commitDAOs.get(0), commit4);
    checkCommitDAO(commitDAOs.get(1), commit3);
    checkCommitDAO(commitDAOs.get(2), commit2);

    // It's OK to backfill again and nothing changes
    DeltaCommit backfillOnlyCommit1 =
        createBackfillOnlyCommitObject(tableInfo.getTableId(), 1L, tableInfo.getStorageLocation());
    deltaCommitsApi.commit(backfillOnlyCommit1);
    commitDAOs = getCommitDAOs(UUID.fromString(tableInfo.getTableId()));
    assertThat(commitDAOs.size()).isEqualTo(3);

    DeltaCommit backfillOnlyCommit2 =
        createBackfillOnlyCommitObject(tableInfo.getTableId(), 4L, tableInfo.getStorageLocation());
    deltaCommitsApi.commit(backfillOnlyCommit2);
    commitDAOs = getCommitDAOs(UUID.fromString(tableInfo.getTableId()));
    assertThat(commitDAOs.size()).isEqualTo(1);
    checkCommitDAO(commitDAOs.get(0), commit4);
    assertThat(commitDAOs.get(0).isBackfilledLatestCommit()).isTrue();

    // Verify commits are cleaned up upon table deletion
    DeltaCommit commit5 =
        createCommitObject(tableInfo.getTableId(), 5L, tableInfo.getStorageLocation());
    deltaCommitsApi.commit(commit5);
    commitDAOs = getCommitDAOs(UUID.fromString(tableInfo.getTableId()));
    assertThat(commitDAOs.size()).isEqualTo(2);
    // Delete the table
    tableOperations.deleteTable(TestUtils.TABLE_FULL_NAME);
    // Verify the uc_delta_commits table is cleaned up
    commitDAOs = getCommitDAOs(UUID.fromString(tableInfo.getTableId()));
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

    // Commit with metadata but without properties
    DeltaMetadata metadata2 = new DeltaMetadata().description("description");
    checkCommitInvalidParameter(
        1L, c -> c.setMetadata(metadata2), "commit does not contain ucTableId in the properties.");

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
  }

  @Test
  public void testBackfillCommitAtDifferentVersions() throws ApiException {
    TableInfo tableInfo =
        createTestingTable(
            "test_backfill_versions", TableType.MANAGED, Optional.empty(), tableOperations);

    // Backfill when there's no commit
    assertApiException(
        () ->
            deltaCommitsApi.commit(
                createBackfillOnlyCommitObject(
                    tableInfo.getTableId(), 1L, tableInfo.getStorageLocation())),
        ErrorCode.INVALID_ARGUMENT,
        "Field can not be null: commit_info in onboarding commit");

    // Create 5 commits
    ArrayList<DeltaCommit> commits = new ArrayList<>();
    for (long i = 1; i <= 5; i++) {
      DeltaCommit commit =
          createCommitObject(tableInfo.getTableId(), i, tableInfo.getStorageLocation());
      commits.add(commit);
      deltaCommitsApi.commit(commit);
    }

    // Verify all 5 commits exist
    List<DeltaCommitDAO> commitDAOs = getCommitDAOs(UUID.fromString(tableInfo.getTableId()));
    assertThat(commitDAOs.size()).isEqualTo(5);

    // Backfill with metadata should fail
    DeltaCommit backfillCommit2WithMetadata =
        createBackfillOnlyCommitObject(tableInfo.getTableId(), 2L, tableInfo.getStorageLocation())
            .metadata(commits.get(0).getMetadata());
    assertApiException(
        () -> deltaCommitsApi.commit(backfillCommit2WithMetadata),
        ErrorCode.INVALID_ARGUMENT,
        "metadata shouldn't be set for backfill only commit");

    // Backfill up to version 2 (should keep versions 3, 4, 5)
    deltaCommitsApi.commit(
        createBackfillOnlyCommitObject(tableInfo.getTableId(), 2L, tableInfo.getStorageLocation()));

    commitDAOs = getCommitDAOs(UUID.fromString(tableInfo.getTableId()));
    assertThat(commitDAOs.size()).isEqualTo(3);
    checkCommitDAO(commitDAOs.get(0), commits.get(4));
    checkCommitDAO(commitDAOs.get(1), commits.get(3));
    checkCommitDAO(commitDAOs.get(2), commits.get(2));

    // Try to backfill beyond the latest version
    assertApiException(
        () ->
            deltaCommitsApi.commit(
                createBackfillOnlyCommitObject(
                    tableInfo.getTableId(), 9L, tableInfo.getStorageLocation())),
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
    deltaCommitsApi.commit(
        createBackfillOnlyCommitObject(tableInfo.getTableId(), 4L, tableInfo.getStorageLocation()));

    commitDAOs = getCommitDAOs(UUID.fromString(tableInfo.getTableId()));
    assertThat(commitDAOs.size()).isEqualTo(1);
    checkCommitDAO(commitDAOs.get(0), commits.get(4));

    // Backfill up to version 5 (the latest)
    deltaCommitsApi.commit(
        createBackfillOnlyCommitObject(tableInfo.getTableId(), 5L, tableInfo.getStorageLocation()));

    commitDAOs = getCommitDAOs(UUID.fromString(tableInfo.getTableId()));
    assertThat(commitDAOs.size()).isEqualTo(1);
    checkCommitDAO(commitDAOs.get(0), commits.get(4));
    assertThat(commitDAOs.get(0).isBackfilledLatestCommit()).isTrue();

    // Commit v6. Now we have [v5(backfilled), v6] in DB.
    DeltaCommit commit6 =
        createCommitObject(tableInfo.getTableId(), 6L, tableInfo.getStorageLocation());
    commits.add(commit6);
    deltaCommitsApi.commit(commit6);
    commitDAOs = getCommitDAOs(UUID.fromString(tableInfo.getTableId()));
    assertThat(commitDAOs.size()).isEqualTo(2);
    checkCommitDAO(commitDAOs.get(0), commits.get(5));
    assertThat(commitDAOs.get(0).isBackfilledLatestCommit()).isFalse();
    checkCommitDAO(commitDAOs.get(1), commits.get(4));
    assertThat(commitDAOs.get(1).isBackfilledLatestCommit()).isTrue();
  }

  @Test
  public void testCommitLimit() throws ApiException {
    TableInfo tableInfo =
        createTestingTable(
            "test_commit_limit", TableType.MANAGED, Optional.empty(), tableOperations);

    // MAX_NUM_COMMITS_PER_TABLE is 10, so we'll try to add 10 commits
    // First add 10 commits - should succeed
    for (long i = 1; i <= 10; i++) {
      DeltaCommit commit =
          createCommitObject(tableInfo.getTableId(), i, tableInfo.getStorageLocation());
      deltaCommitsApi.commit(commit);
    }

    // Then try to add the 11th commit - should fail
    DeltaCommit commit11 =
        createCommitObject(tableInfo.getTableId(), 11L, tableInfo.getStorageLocation());
    assertApiException(
        () -> deltaCommitsApi.commit(commit11),
        ErrorCode.RESOURCE_EXHAUSTED,
        "Max number of commits per table reached");

    // Backfill the first 6 commits
    DeltaCommit backfillCommit =
        createBackfillOnlyCommitObject(tableInfo.getTableId(), 6L, tableInfo.getStorageLocation());
    deltaCommitsApi.commit(backfillCommit);

    // Verify we now have 4 commits (7~10)
    List<DeltaCommitDAO> commitDAOs = getCommitDAOs(UUID.fromString(tableInfo.getTableId()));
    assertThat(commitDAOs.size()).isEqualTo(4);

    // Now we should be able to add more commits
    for (long i = 11; i <= 14; i++) {
      DeltaCommit commit =
          createCommitObject(tableInfo.getTableId(), i, tableInfo.getStorageLocation());
      deltaCommitsApi.commit(commit);
    }

    // Verify we now have 8 commits (7~14)
    commitDAOs = getCommitDAOs(UUID.fromString(tableInfo.getTableId()));
    assertThat(commitDAOs.size()).isEqualTo(8);

    // Backfill all commits again
    DeltaCommit backfillCommit2 =
        createBackfillOnlyCommitObject(tableInfo.getTableId(), 14L, tableInfo.getStorageLocation());
    deltaCommitsApi.commit(backfillCommit2);

    // Verify we now have 1 commit marked as backfilled
    commitDAOs = getCommitDAOs(UUID.fromString(tableInfo.getTableId()));
    assertThat(commitDAOs.size()).isEqualTo(1);
    assertThat(commitDAOs.get(0).isBackfilledLatestCommit()).isTrue();

    // Now we should be able to add another 10 commits
    for (long i = 15; i <= 24; i++) {
      DeltaCommit commit =
          createCommitObject(tableInfo.getTableId(), i, tableInfo.getStorageLocation());
      deltaCommitsApi.commit(commit);
    }
  }

  @Test
  public void testCommitWithMetadata() throws ApiException {
    TableInfo tableInfo =
        createTestingTable("test_metadata", TableType.MANAGED, Optional.empty(), tableOperations);
    String fullTableName =
        tableInfo.getCatalogName() + "." + tableInfo.getSchemaName() + "." + tableInfo.getName();

    // Commit with metadata properties
    Map<String, String> properties = new HashMap<>();
    properties.put("ucTableId", tableInfo.getTableId());
    properties.put("customProperty", "customValue");
    properties.put("delta.feature.test", "enabled");

    DeltaCommitMetadataProperties metadataProperties =
        new DeltaCommitMetadataProperties().properties(properties);
    DeltaMetadata metadata = new DeltaMetadata().properties(metadataProperties);

    DeltaCommit commit =
        createCommitObject(tableInfo.getTableId(), 1L, tableInfo.getStorageLocation())
            .metadata(metadata);
    deltaCommitsApi.commit(commit);

    // Retrieve the table and verify properties were updated
    TableInfo updatedTable = tableOperations.getTable(fullTableName);
    assertNotNull(updatedTable.getProperties());
    assertEquals("customValue", updatedTable.getProperties().get("customProperty"));
    assertEquals("enabled", updatedTable.getProperties().get("delta.feature.test"));

    commit = createCommitObject(tableInfo.getTableId(), 2L, tableInfo.getStorageLocation());
    commit.getMetadata().setDescription("Updated table description via commit");
    deltaCommitsApi.commit(commit);

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
    deltaCommitsApi.commit(commit);

    // Retrieve the table and verify schema was updated
    updatedTable = tableOperations.getTable(fullTableName);
    assertNotNull(updatedTable.getColumns());
    assertEquals(2, updatedTable.getColumns().size());
    assertEquals("new_column_1", updatedTable.getColumns().get(0).getName());
    assertEquals("new_column_2", updatedTable.getColumns().get(1).getName());

    // Commit with all metadata fields
    properties = new HashMap<>();
    properties.put("ucTableId", tableInfo.getTableId());
    properties.put("customProperty", "customValueNew");
    metadataProperties = new DeltaCommitMetadataProperties().properties(properties);
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
    updatedTable = tableOperations.getTable(fullTableName);
    assertEquals("Complete metadata test table", updatedTable.getComment());
    assertNotNull(updatedTable.getProperties());
    assertFalse(updatedTable.getProperties().containsKey("delta.feature.test")); // removed
    assertEquals("customValueNew", updatedTable.getProperties().get("customProperty"));
    assertNotNull(updatedTable.getColumns());
    assertEquals(1, updatedTable.getColumns().size());
    assertEquals("test_column_new", updatedTable.getColumns().get(0).getName());
    assertEquals("Test column comment new", updatedTable.getColumns().get(0).getComment());

    // Commit with empty metadata (no properties, description, or schema)
    checkCommitInvalidParameter(
        5L,
        c -> c.setMetadata(new DeltaMetadata()),
        "At least one of description, properties, or schema must be set in commit.metadata");
  }
}
