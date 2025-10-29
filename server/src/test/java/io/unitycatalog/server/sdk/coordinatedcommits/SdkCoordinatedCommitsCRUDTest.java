package io.unitycatalog.server.sdk.coordinatedcommits;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.*;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.CoordinatedCommitsApi;
import io.unitycatalog.client.model.*;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.catalog.CatalogOperations;
import io.unitycatalog.server.base.schema.SchemaOperations;
import io.unitycatalog.server.base.table.BaseCRUDTestForTable;
import io.unitycatalog.server.base.table.TableOperations;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.persist.dao.CommitDAO;
import io.unitycatalog.server.sdk.catalog.SdkCatalogOperations;
import io.unitycatalog.server.sdk.schema.SdkSchemaOperations;
import io.unitycatalog.server.sdk.tables.SdkTableOperations;
import io.unitycatalog.server.utils.TestUtils;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.hibernate.Session;
import org.hibernate.query.Query;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SdkCoordinatedCommitsCRUDTest extends BaseCRUDTestForTable {

  private CoordinatedCommitsApi coordinatedCommitsApi;

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
  }

  private Commit createCommitObject(
      String tableId, Long version, Boolean isDisownCommit, String tableUri) {
    return new Commit()
        .tableId(tableId)
        .tableUri(tableUri)
        .commitInfo(
            new CommitInfo()
                .version(version)
                .fileName("file" + version)
                .fileSize(100L)
                .timestamp(1700000000L + version)
                .fileModificationTimestamp(1700000000L + version)
                .isDisownCommit(isDisownCommit))
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

  @Test
  public void testBasicCoordinatedCommitsCRUD() throws IOException, ApiException {
    // Create a table for testing
    TableInfo tableInfo =
        createTestingTable(TestUtils.TABLE_NAME, TableType.MANAGED, null, tableOperations);
    String fullTableName =
        tableInfo.getCatalogName() + "." + tableInfo.getSchemaName() + "." + tableInfo.getName();

    // Get commits on a table with no commits
    GetCommitsResponse response =
        coordinatedCommitsApi.getCommits(
            tableInfo.getTableId(), tableInfo.getStorageLocation(), 0L, null);
    assertEquals(-1, response.getLatestTableVersion());
    assertTrue(response.getCommits() == null || response.getCommits().isEmpty());

    Commit commit1 =
        createCommitObject(tableInfo.getTableId(), 1L, false, tableInfo.getStorageLocation());
    coordinatedCommitsApi.commit(commit1);
    ApiException ex = assertThrows(ApiException.class, () -> coordinatedCommitsApi.commit(commit1));
    assertThat(ex.getCode()).isEqualTo(ErrorCode.ALREADY_EXISTS.getHttpStatus().code());
    assertThat(ex.getMessage()).contains("Commit version already accepted.");

    Commit commit2 =
        createCommitObject(tableInfo.getTableId(), 2L, false, tableInfo.getStorageLocation());
    coordinatedCommitsApi.commit(commit2);

    Commit commit3 =
        createCommitObject(tableInfo.getTableId(), 3L, false, tableInfo.getStorageLocation());
    coordinatedCommitsApi.commit(commit3);

    response =
        coordinatedCommitsApi.getCommits(
            tableInfo.getTableId(), tableInfo.getStorageLocation(), 0L, null);
    assertEquals(3, response.getCommits().size());
    assertEquals(3, response.getLatestTableVersion());
    assertTrue(response.getCommits().contains(commit1.getCommitInfo()));
    assertTrue(response.getCommits().contains(commit2.getCommitInfo()));
    assertTrue(response.getCommits().contains(commit3.getCommitInfo()));

    response =
        coordinatedCommitsApi.getCommits(
            tableInfo.getTableId(), tableInfo.getStorageLocation(), 2L, null);
    assertEquals(2, response.getCommits().size());
    assertEquals(3, response.getLatestTableVersion());
    assertFalse(response.getCommits().contains(commit1.getCommitInfo()));
    assertTrue(response.getCommits().contains(commit2.getCommitInfo()));
    assertTrue(response.getCommits().contains(commit3.getCommitInfo()));

    response =
        coordinatedCommitsApi.getCommits(
            tableInfo.getTableId(), tableInfo.getStorageLocation(), 2L, 2L);
    assertEquals(1, response.getCommits().size());
    assertEquals(3, response.getLatestTableVersion());
    assertFalse(response.getCommits().contains(commit1.getCommitInfo()));
    assertTrue(response.getCommits().contains(commit2.getCommitInfo()));
    assertFalse(response.getCommits().contains(commit3.getCommitInfo()));

    // Add a new commit (version 4) and backfill up to version 1 in the same request
    Commit commit4 =
        createCommitObject(tableInfo.getTableId(), 4L, false, tableInfo.getStorageLocation());
    commit4.setLatestBackfilledVersion(1L);
    coordinatedCommitsApi.commit(commit4);
    // Verify we now have 3 commits (versions 2, 3, 4)
    response =
        coordinatedCommitsApi.getCommits(
            tableInfo.getTableId(), tableInfo.getStorageLocation(), 0L, null);
    assertEquals(3, response.getCommits().size());
    assertEquals(4L, response.getLatestTableVersion());
    assertEquals(4L, response.getCommits().get(0).getVersion());
    assertEquals(3L, response.getCommits().get(1).getVersion());
    assertEquals(2L, response.getCommits().get(2).getVersion());

    // It's OK to backfill again and nothing changes
    Commit backfillOnlyCommit1 =
        createBackfillOnlyCommitObject(tableInfo.getTableId(), 1L, tableInfo.getStorageLocation());
    coordinatedCommitsApi.commit(backfillOnlyCommit1);
    response =
        coordinatedCommitsApi.getCommits(
            tableInfo.getTableId(), tableInfo.getStorageLocation(), 0L, null);
    assertEquals(3, response.getCommits().size());
    assertEquals(4L, response.getLatestTableVersion());

    Commit backfillOnlyCommit2 =
        createBackfillOnlyCommitObject(tableInfo.getTableId(), 4L, tableInfo.getStorageLocation());
    coordinatedCommitsApi.commit(backfillOnlyCommit2);
    response =
        coordinatedCommitsApi.getCommits(
            tableInfo.getTableId(), tableInfo.getStorageLocation(), 0L, null);
    assertEquals(0, response.getCommits().size());
    assertEquals(4L, response.getLatestTableVersion());

    // Verify commits are cleaned up upon table deletion
    Commit commit5 =
        createCommitObject(tableInfo.getTableId(), 5L, false, tableInfo.getStorageLocation());
    coordinatedCommitsApi.commit(commit5);
    response =
        coordinatedCommitsApi.getCommits(
            tableInfo.getTableId(), tableInfo.getStorageLocation(), 0L, null);
    assertEquals(2, response.getCommits().size());
    // Delete the table
    tableOperations.deleteTable(fullTableName);
    // Verify the uc_commits table is cleaned up
    try (Session session = hibernateConfigurator.getSessionFactory().openSession()) {
      String sql = "SELECT * FROM uc_commits LIMIT 1";
      Query<CommitDAO> query = session.createNativeQuery(sql, CommitDAO.class);
      List<CommitDAO> result = query.getResultList();
      assertEquals(0, result.size());
    }
  }

  @Test
  public void testInvalidParameters() throws IOException, ApiException {
    int INVALID_ARGUMENT = ErrorCode.INVALID_ARGUMENT.getHttpStatus().code();
    int UNIMPLEMENTED = ErrorCode.UNIMPLEMENTED.getHttpStatus().code();
    TableInfo tableInfo =
        createTestingTable("test_invalid_null_table_id", TableType.MANAGED, null, tableOperations);
    Commit commit1 =
        createCommitObject(tableInfo.getTableId(), 1L, false, tableInfo.getStorageLocation())
            .tableId(null);
    ApiException ex = assertThrows(ApiException.class, () -> coordinatedCommitsApi.commit(commit1));
    assertThat(ex.getCode()).isEqualTo(INVALID_ARGUMENT);
    assertThat(ex.getMessage()).contains("table_id");

    Commit commit2 =
        createCommitObject(tableInfo.getTableId(), 1L, false, tableInfo.getStorageLocation())
            .tableId("");
    ex = assertThrows(ApiException.class, () -> coordinatedCommitsApi.commit(commit2));
    assertThat(ex.getCode()).isEqualTo(INVALID_ARGUMENT);
    assertThat(ex.getMessage()).contains("table_id");

    Commit commit3 = createCommitObject(tableInfo.getTableId(), 1L, false, null);
    ex = assertThrows(ApiException.class, () -> coordinatedCommitsApi.commit(commit3));
    assertThat(ex.getCode()).isEqualTo(INVALID_ARGUMENT);
    assertThat(ex.getMessage()).contains("table_uri");

    Commit commit4 = createCommitObject(tableInfo.getTableId(), 1L, false, "");
    ex = assertThrows(ApiException.class, () -> coordinatedCommitsApi.commit(commit4));
    assertThat(ex.getCode()).isEqualTo(INVALID_ARGUMENT);
    assertThat(ex.getMessage()).contains("table_uri");

    Commit commit5 =
        createCommitObject(tableInfo.getTableId(), -1L, false, tableInfo.getStorageLocation());
    ex = assertThrows(ApiException.class, () -> coordinatedCommitsApi.commit(commit5));
    assertThat(ex.getCode()).isEqualTo(INVALID_ARGUMENT);
    assertThat(ex.getMessage()).contains("version");

    Commit commit6 =
        createCommitObject(tableInfo.getTableId(), 0L, false, tableInfo.getStorageLocation());
    ex = assertThrows(ApiException.class, () -> coordinatedCommitsApi.commit(commit6));
    assertThat(ex.getCode()).isEqualTo(INVALID_ARGUMENT);
    assertThat(ex.getMessage()).contains("version");

    Commit commit7 =
        createCommitObject(tableInfo.getTableId(), 1L, false, tableInfo.getStorageLocation());
    coordinatedCommitsApi.commit(commit7);

    // Try to commit version 3 when version 2 hasn't been committed yet
    Commit commit8 =
        createCommitObject(tableInfo.getTableId(), 3L, false, tableInfo.getStorageLocation());
    ex = assertThrows(ApiException.class, () -> coordinatedCommitsApi.commit(commit8));
    assertThat(ex.getCode()).isEqualTo(INVALID_ARGUMENT);
    assertTrue(
        ex.getMessage()
            .contains(
                "Commit version must be the next version after the latest commit 1, but got 3"));

    Commit commit9 =
        createCommitObject(tableInfo.getTableId(), 2L, false, "s3://wrong-bucket/wrong-path");
    ex = assertThrows(ApiException.class, () -> coordinatedCommitsApi.commit(commit9));
    assertThat(ex.getCode()).isEqualTo(INVALID_ARGUMENT);
    assertThat(ex.getMessage()).contains("Table URI in commit does not match the table path");

    Commit commit10 =
        createCommitObject(tableInfo.getTableId(), 2L, true, tableInfo.getStorageLocation());
    ex = assertThrows(ApiException.class, () -> coordinatedCommitsApi.commit(commit10));
    assertThat(ex.getCode()).isEqualTo(UNIMPLEMENTED);
    assertThat(ex.getMessage()).contains("Disown commits are not supported!");

    Commit commit11 =
        new Commit().tableId(tableInfo.getTableId()).tableUri(tableInfo.getStorageLocation());
    ex = assertThrows(ApiException.class, () -> coordinatedCommitsApi.commit(commit11));
    assertThat(ex.getMessage())
        .contains("Either commit_info or latest_backfilled_version must be defined");

    // Create a commit with metadata but without ucTableId in properties
    Map<String, String> properties = Map.of("customProperty", "customValue");
    Metadata metadata =
        new Metadata().properties(new CommitMetadataProperties().properties(properties));
    Commit commit12 =
        createCommitObject(tableInfo.getTableId(), 2L, false, tableInfo.getStorageLocation())
            .metadata(metadata);
    ex = assertThrows(ApiException.class, () -> coordinatedCommitsApi.commit(commit12));
    assertThat(ex.getCode()).isEqualTo(INVALID_ARGUMENT);
    assertThat(ex.getMessage()).contains("ucTableId");

    // Create a commit with metadata but with wrong ucTableId
    properties = Map.of("ucTableId", "00000000-0000-0000-0000-000000000000");
    metadata = new Metadata().properties(new CommitMetadataProperties().properties(properties));
    Commit commit13 =
        createCommitObject(tableInfo.getTableId(), 2L, false, tableInfo.getStorageLocation())
            .metadata(metadata);
    ex = assertThrows(ApiException.class, () -> coordinatedCommitsApi.commit(commit13));
    assertThat(ex.getCode()).isEqualTo(INVALID_ARGUMENT);
    assertThat(ex.getMessage()).contains("does not match the properties ucTableId");

    // Invalid empty filename
    Commit commit14 =
        createCommitObject(tableInfo.getTableId(), 2L, false, tableInfo.getStorageLocation());
    commit14.getCommitInfo().setFileName("");
    ex = assertThrows(ApiException.class, () -> coordinatedCommitsApi.commit(commit14));
    assertThat(ex.getCode()).isEqualTo(INVALID_ARGUMENT);
    assertThat(ex.getMessage()).contains("file_name");

    // Invalid negative file size
    Commit commit15 =
        createCommitObject(tableInfo.getTableId(), 2L, false, tableInfo.getStorageLocation());
    commit15.getCommitInfo().setFileSize(-100L);
    ex = assertThrows(ApiException.class, () -> coordinatedCommitsApi.commit(commit15));
    assertThat(ex.getCode()).isEqualTo(INVALID_ARGUMENT);
    assertThat(ex.getMessage()).contains("file_size");

    // Invalid negative timestamp
    Commit commit16 =
        createCommitObject(tableInfo.getTableId(), 2L, false, tableInfo.getStorageLocation());
    commit16.getCommitInfo().setTimestamp(-1L);
    ex = assertThrows(ApiException.class, () -> coordinatedCommitsApi.commit(commit16));
    assertThat(ex.getCode()).isEqualTo(INVALID_ARGUMENT);
    assertThat(ex.getMessage()).contains("timestamp");

    ex =
        assertThrows(
            ApiException.class,
            () ->
                coordinatedCommitsApi.getCommits(
                    tableInfo.getTableId(), tableInfo.getStorageLocation(), -1L, null));
    assertThat(ex.getMessage()).contains("start_version");

    ex =
        assertThrows(
            ApiException.class,
            () ->
                coordinatedCommitsApi.getCommits(
                    tableInfo.getTableId(), tableInfo.getStorageLocation(), 5L, 3L));
    assertThat(ex.getMessage()).contains("end_version must be >=start_version if set");
  }

  @Test
  public void testBackfillCommitAtDifferentVersions() throws IOException, ApiException {
    TableInfo tableInfo =
        createTestingTable("test_backfill_versions", TableType.MANAGED, null, tableOperations);

    // Create 5 commits
    for (long i = 1; i <= 5; i++) {
      Commit commit =
          createCommitObject(tableInfo.getTableId(), i, false, tableInfo.getStorageLocation());
      coordinatedCommitsApi.commit(commit);
    }

    // Verify all 5 commits exist
    GetCommitsResponse response =
        coordinatedCommitsApi.getCommits(
            tableInfo.getTableId(), tableInfo.getStorageLocation(), 0L, null);
    assertEquals(5, response.getCommits().size());
    assertEquals(5, response.getLatestTableVersion());

    // Backfill up to version 2 (should keep versions 3, 4, 5)
    Commit backfillCommit =
        createBackfillOnlyCommitObject(tableInfo.getTableId(), 2L, tableInfo.getStorageLocation());
    coordinatedCommitsApi.commit(backfillCommit);

    response =
        coordinatedCommitsApi.getCommits(
            tableInfo.getTableId(), tableInfo.getStorageLocation(), 0L, null);
    assertEquals(3, response.getCommits().size());
    assertEquals(5, response.getLatestTableVersion());
    // Verify versions 3, 4, 5 are present
    assertThat(response.getCommits().get(0).getVersion()).isEqualTo(5L);
    assertThat(response.getCommits().get(1).getVersion()).isEqualTo(4L);
    assertThat(response.getCommits().get(2).getVersion()).isEqualTo(3L);

    // Try to backfill beyond the latest version
    Commit backfillCommit2 =
        createBackfillOnlyCommitObject(tableInfo.getTableId(), 9L, tableInfo.getStorageLocation());
    ApiException ex =
        assertThrows(ApiException.class, () -> coordinatedCommitsApi.commit(backfillCommit2));
    assertThat(ex.getMessage())
        .contains("Should not backfill version 9 while the last version commited is 5");

    // Backfill up to version 4 (should keep only version 5)
    backfillCommit =
        createBackfillOnlyCommitObject(tableInfo.getTableId(), 4L, tableInfo.getStorageLocation());
    coordinatedCommitsApi.commit(backfillCommit);

    response =
        coordinatedCommitsApi.getCommits(
            tableInfo.getTableId(), tableInfo.getStorageLocation(), 0L, null);
    assertEquals(1, response.getCommits().size());
    assertEquals(5, response.getLatestTableVersion());
    assertEquals(5, response.getCommits().get(0).getVersion());

    // Backfill up to version 5 (the latest)
    backfillCommit =
        createBackfillOnlyCommitObject(tableInfo.getTableId(), 5L, tableInfo.getStorageLocation());
    coordinatedCommitsApi.commit(backfillCommit);

    response =
        coordinatedCommitsApi.getCommits(
            tableInfo.getTableId(), tableInfo.getStorageLocation(), 0L, null);
    // When the latest commit is backfilled, it should be marked as such but still returned
    assertEquals(5, response.getLatestTableVersion());
    // The commit should be marked as backfilled, so no commits should be returned
    assertTrue(response.getCommits() == null || response.getCommits().isEmpty());
  }

  @Test
  public void testCommitLimit() throws IOException, ApiException {
    TableInfo tableInfo =
        createTestingTable("test_commit_limit", TableType.MANAGED, null, tableOperations);

    // MAX_NUM_COMMITS_PER_TABLE is 50, so we'll try to add 51 commits
    // First add 50 commits - should succeed
    for (long i = 1; i <= 50; i++) {
      Commit commit =
          createCommitObject(tableInfo.getTableId(), i, false, tableInfo.getStorageLocation());
      coordinatedCommitsApi.commit(commit);
    }

    // Try to add the 51st commit - should fail
    Commit commit51 =
        createCommitObject(tableInfo.getTableId(), 51L, false, tableInfo.getStorageLocation());
    ApiException ex =
        assertThrows(ApiException.class, () -> coordinatedCommitsApi.commit(commit51));
    assertThat(ex.getCode()).isEqualTo(ErrorCode.RESOURCE_EXHAUSTED.getHttpStatus().code());
    assertThat(ex.getMessage()).contains("Max number of commits per table reached");

    // Backfill the first 30 commits
    Commit backfillCommit =
        createBackfillOnlyCommitObject(tableInfo.getTableId(), 30L, tableInfo.getStorageLocation());
    coordinatedCommitsApi.commit(backfillCommit);

    // Verify we now have 20 commits (31-50)
    GetCommitsResponse response =
        coordinatedCommitsApi.getCommits(
            tableInfo.getTableId(), tableInfo.getStorageLocation(), 0L, null);
    assertEquals(20, response.getCommits().size());
    assertEquals(50, response.getCommits().get(0).getVersion());
    assertEquals(31, response.getCommits().get(response.getCommits().size() - 1).getVersion());

    // Now we should be able to add more commits
    for (long i = 51; i <= 60; i++) {
      Commit commit =
          createCommitObject(tableInfo.getTableId(), i, false, tableInfo.getStorageLocation());
      coordinatedCommitsApi.commit(commit);
    }

    // Verify we now have 30 commits (31-60)
    response =
        coordinatedCommitsApi.getCommits(
            tableInfo.getTableId(), tableInfo.getStorageLocation(), 0L, null);
    assertEquals(30, response.getCommits().size());
    assertEquals(60, response.getLatestTableVersion());
  }

  @Test
  public void testPagination() throws IOException, ApiException {
    TableInfo tableInfo =
        createTestingTable("test_pagination", TableType.MANAGED, null, tableOperations);

    // Create 10 commits
    for (long i = 1; i <= 10; i++) {
      Commit commit =
          createCommitObject(tableInfo.getTableId(), i, false, tableInfo.getStorageLocation());
      coordinatedCommitsApi.commit(commit);
    }

    // Get commits with pagination: start_version=0, end_version=3
    GetCommitsResponse response =
        coordinatedCommitsApi.getCommits(
            tableInfo.getTableId(), tableInfo.getStorageLocation(), 0L, 3L);
    assertEquals(
        3,
        response
            .getCommits()
            .size()); // versions 0, 1, 2, 3 (but version 0 doesn't exist, so 1, 2, 3)
    assertEquals(10, response.getLatestTableVersion());

    // Get next page: start_version=4, end_version=7
    response =
        coordinatedCommitsApi.getCommits(
            tableInfo.getTableId(), tableInfo.getStorageLocation(), 4L, 7L);
    assertEquals(4, response.getCommits().size());
    assertEquals(10, response.getLatestTableVersion());

    // Get last page: start_version=8, end_version=null
    response =
        coordinatedCommitsApi.getCommits(
            tableInfo.getTableId(), tableInfo.getStorageLocation(), 8L, null);
    assertEquals(3, response.getCommits().size()); // versions 8, 9, 10
    assertEquals(10, response.getLatestTableVersion());

    // Get commits starting from a version that doesn't exist yet
    response =
        coordinatedCommitsApi.getCommits(
            tableInfo.getTableId(), tableInfo.getStorageLocation(), 11L, null);
    assertTrue(response.getCommits() == null || response.getCommits().isEmpty());
    assertEquals(10, response.getLatestTableVersion());

    // Get commits with start_version = end_version
    response =
        coordinatedCommitsApi.getCommits(
            tableInfo.getTableId(), tableInfo.getStorageLocation(), 3L, 3L);
    assertEquals(1, response.getCommits().size());
    assertEquals(3, response.getCommits().get(0).getVersion());
  }

  @Test
  public void testCommitWithMetadata() throws IOException, ApiException {
    TableInfo tableInfo =
        createTestingTable("test_metadata", TableType.MANAGED, null, tableOperations);
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
        createCommitObject(tableInfo.getTableId(), 1L, false, tableInfo.getStorageLocation())
            .metadata(metadata);
    coordinatedCommitsApi.commit(commit);

    // Retrieve the table and verify properties were updated
    TableInfo updatedTable = tableOperations.getTable(fullTableName);
    assertNotNull(updatedTable.getProperties());
    assertEquals("customValue", updatedTable.getProperties().get("customProperty"));
    assertEquals("enabled", updatedTable.getProperties().get("delta.feature.test"));

    commit = createCommitObject(tableInfo.getTableId(), 2L, false, tableInfo.getStorageLocation());
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
    commit = createCommitObject(tableInfo.getTableId(), 3L, false, tableInfo.getStorageLocation());
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
        createCommitObject(tableInfo.getTableId(), 4L, false, tableInfo.getStorageLocation())
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
    Commit commit5 =
        createCommitObject(tableInfo.getTableId(), 5L, false, tableInfo.getStorageLocation())
            .metadata(new Metadata());

    ApiException ex = assertThrows(ApiException.class, () -> coordinatedCommitsApi.commit(commit5));
    assertThat(ex.getMessage())
        .contains(
            "At least one of description, properties, or schema must be set in commit.metadata");
  }
}
