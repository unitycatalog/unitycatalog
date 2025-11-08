package io.unitycatalog.server.sdk.coordinatedcommits;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.CoordinatedCommitsApi;
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
    checkCommitInvalidParameter(1L, c -> c.setTableUri(""), "table_uri");
    checkCommitInvalidParameter(
        1L,
        c -> c.setTableUri("s3://wrong-bucket/wrong-path"),
        "Table URI in commit does not match the table path");
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
  }
}
