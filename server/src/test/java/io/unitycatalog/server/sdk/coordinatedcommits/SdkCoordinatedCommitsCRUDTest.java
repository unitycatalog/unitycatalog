package io.unitycatalog.server.sdk.coordinatedcommits;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

  @Test
  public void testInvalidParameters() throws ApiException {
    int INVALID_ARGUMENT = ErrorCode.INVALID_ARGUMENT.getHttpStatus().code();

    Commit commit1 =
        createCommitObject(tableInfo.getTableId(), 1L, tableInfo.getStorageLocation())
            .tableId(null);
    ApiException ex = assertThrows(ApiException.class, () -> coordinatedCommitsApi.commit(commit1));
    assertThat(ex.getCode()).isEqualTo(INVALID_ARGUMENT);
    assertThat(ex.getMessage()).contains("table_id");

    Commit commit2 =
        createCommitObject(tableInfo.getTableId(), 1L, tableInfo.getStorageLocation()).tableId("");
    ex = assertThrows(ApiException.class, () -> coordinatedCommitsApi.commit(commit2));
    assertThat(ex.getCode()).isEqualTo(INVALID_ARGUMENT);
    assertThat(ex.getMessage()).contains("table_id");

    Commit commit3 = createCommitObject(tableInfo.getTableId(), 1L, null);
    ex = assertThrows(ApiException.class, () -> coordinatedCommitsApi.commit(commit3));
    assertThat(ex.getCode()).isEqualTo(INVALID_ARGUMENT);
    assertThat(ex.getMessage()).contains("table_uri");

    Commit commit4 = createCommitObject(tableInfo.getTableId(), 1L, "");
    ex = assertThrows(ApiException.class, () -> coordinatedCommitsApi.commit(commit4));
    assertThat(ex.getCode()).isEqualTo(INVALID_ARGUMENT);
    assertThat(ex.getMessage()).contains("table_uri");

    Commit commit5 =
        createCommitObject(tableInfo.getTableId(), -1L, tableInfo.getStorageLocation());
    ex = assertThrows(ApiException.class, () -> coordinatedCommitsApi.commit(commit5));
    assertThat(ex.getCode()).isEqualTo(INVALID_ARGUMENT);
    assertThat(ex.getMessage()).contains("version");

    Commit commit6 = createCommitObject(tableInfo.getTableId(), 0L, tableInfo.getStorageLocation());
    ex = assertThrows(ApiException.class, () -> coordinatedCommitsApi.commit(commit6));
    assertThat(ex.getCode()).isEqualTo(INVALID_ARGUMENT);
    assertThat(ex.getMessage()).contains("version");

    Commit commit7 = createCommitObject(tableInfo.getTableId(), 1L, tableInfo.getStorageLocation());
    coordinatedCommitsApi.commit(commit7);

    // Try to commit version 3 when version 2 hasn't been committed yet
    Commit commit8 = createCommitObject(tableInfo.getTableId(), 3L, tableInfo.getStorageLocation());
    ex = assertThrows(ApiException.class, () -> coordinatedCommitsApi.commit(commit8));
    assertThat(ex.getCode()).isEqualTo(INVALID_ARGUMENT);
    assertTrue(
        ex.getMessage()
            .contains(
                "Commit version must be the next version after the latest commit 1, but got 3"));

    Commit commit9 = createCommitObject(tableInfo.getTableId(), 2L, "s3://wrong-bucket/wrong-path");
    ex = assertThrows(ApiException.class, () -> coordinatedCommitsApi.commit(commit9));
    assertThat(ex.getCode()).isEqualTo(INVALID_ARGUMENT);
    assertThat(ex.getMessage()).contains("Table URI in commit does not match the table path");

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
        createCommitObject(tableInfo.getTableId(), 2L, tableInfo.getStorageLocation())
            .metadata(metadata);
    ex = assertThrows(ApiException.class, () -> coordinatedCommitsApi.commit(commit12));
    assertThat(ex.getCode()).isEqualTo(INVALID_ARGUMENT);
    assertThat(ex.getMessage()).contains("ucTableId");

    // Create a commit with metadata but with wrong ucTableId
    properties = Map.of("ucTableId", "00000000-0000-0000-0000-000000000000");
    metadata = new Metadata().properties(new CommitMetadataProperties().properties(properties));
    Commit commit13 =
        createCommitObject(tableInfo.getTableId(), 2L, tableInfo.getStorageLocation())
            .metadata(metadata);
    ex = assertThrows(ApiException.class, () -> coordinatedCommitsApi.commit(commit13));
    assertThat(ex.getCode()).isEqualTo(INVALID_ARGUMENT);
    assertThat(ex.getMessage()).contains("does not match the properties ucTableId");

    // Invalid empty filename
    Commit commit14 =
        createCommitObject(tableInfo.getTableId(), 2L, tableInfo.getStorageLocation());
    commit14.getCommitInfo().setFileName("");
    ex = assertThrows(ApiException.class, () -> coordinatedCommitsApi.commit(commit14));
    assertThat(ex.getCode()).isEqualTo(INVALID_ARGUMENT);
    assertThat(ex.getMessage()).contains("file_name");

    // Invalid negative file size
    Commit commit15 =
        createCommitObject(tableInfo.getTableId(), 2L, tableInfo.getStorageLocation());
    commit15.getCommitInfo().setFileSize(-100L);
    ex = assertThrows(ApiException.class, () -> coordinatedCommitsApi.commit(commit15));
    assertThat(ex.getCode()).isEqualTo(INVALID_ARGUMENT);
    assertThat(ex.getMessage()).contains("file_size");

    // Invalid negative timestamp
    Commit commit16 =
        createCommitObject(tableInfo.getTableId(), 2L, tableInfo.getStorageLocation());
    commit16.getCommitInfo().setTimestamp(-1L);
    ex = assertThrows(ApiException.class, () -> coordinatedCommitsApi.commit(commit16));
    assertThat(ex.getCode()).isEqualTo(INVALID_ARGUMENT);
    assertThat(ex.getMessage()).contains("timestamp");
  }
}
