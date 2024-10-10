package io.unitycatalog.server.base.coordinatedcommits;

import static org.junit.jupiter.api.Assertions.*;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.*;
import io.unitycatalog.server.base.BaseCRUDTest;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.schema.SchemaOperations;
import io.unitycatalog.server.base.table.TableOperations;
import io.unitycatalog.server.utils.TestUtils;
import java.io.IOException;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public abstract class BaseCoordinatedCommitsCRUDTest extends BaseCRUDTest {
  protected CoordinatedCommitsOperations coordinatedCommitsOperations;
  protected SchemaOperations schemaOperations;
  protected TableOperations tableOperations;

  protected abstract CoordinatedCommitsOperations createCoordinatedCommitsOperations(
      ServerConfig config);

  protected abstract SchemaOperations createSchemaOperations(ServerConfig serverConfig);

  protected abstract TableOperations createTableOperations(ServerConfig serverConfig);

  @BeforeEach
  @Override
  public void setUp() {
    super.setUp();
    schemaOperations = createSchemaOperations(serverConfig);
    tableOperations = createTableOperations(serverConfig);
    coordinatedCommitsOperations = createCoordinatedCommitsOperations(serverConfig);
  }

  public TableInfo createTestingTable(String tableName, String storageLocation)
      throws IOException, ApiException {
    ColumnInfo columnInfo1 =
        new ColumnInfo()
            .name("as_int")
            .typeText("INTEGER")
            .typeJson("{\"type\": \"integer\"}")
            .typeName(ColumnTypeName.INT)
            .typePrecision(10)
            .typeScale(0)
            .position(0)
            .comment("Integer column")
            .nullable(true);

    ColumnInfo columnInfo2 =
        new ColumnInfo()
            .name("as_string")
            .typeText("VARCHAR(255)")
            .typeJson("{\"type\": \"string\", \"length\": \"255\"}")
            .typeName(ColumnTypeName.STRING)
            .position(1)
            .comment("String column")
            .nullable(true);

    CreateTable createTableRequest =
        new CreateTable()
            .name(tableName)
            .catalogName(TestUtils.CATALOG_NAME)
            .schemaName(TestUtils.SCHEMA_NAME)
            .columns(List.of(columnInfo1, columnInfo2))
            .properties(TestUtils.PROPERTIES)
            .comment(TestUtils.COMMENT)
            .storageLocation(storageLocation)
            .tableType(TableType.EXTERNAL)
            .dataSourceFormat(DataSourceFormat.DELTA);

    return tableOperations.createTable(createTableRequest);
  }

  public Commit createCommitObject(
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
                .isDisownCommit(isDisownCommit));
  }

  public Commit createBackfillOnlyCommitObject(
      String tableId, Long latestBackfilledVersion, String tableUri) {
    return new Commit()
        .tableId(tableId)
        .tableUri(tableUri)
        .latestBackfilledVersion(latestBackfilledVersion);
  }

  @Test
  public void testCoordinatedCommitsCRUD() throws IOException, ApiException {
    // Create a table for testing
    catalogOperations.createCatalog(new CreateCatalog().name(TestUtils.CATALOG_NAME));
    schemaOperations.createSchema(
        new CreateSchema().name(TestUtils.SCHEMA_NAME).catalogName(TestUtils.CATALOG_NAME));
    TableInfo tableInfo = createTestingTable(TestUtils.TABLE_NAME, TestUtils.STORAGE_LOCATION);

    Commit commit1 =
        createCommitObject(tableInfo.getTableId(), 1L, false, tableInfo.getStorageLocation());
    Commit commit2 =
        createCommitObject(tableInfo.getTableId(), 2L, false, tableInfo.getStorageLocation());
    Commit commit3 =
        createCommitObject(tableInfo.getTableId(), 3L, false, tableInfo.getStorageLocation());
    Commit backfillOnlyCommit1 =
        createBackfillOnlyCommitObject(tableInfo.getTableId(), 2L, tableInfo.getStorageLocation());
    coordinatedCommitsOperations.commit(commit1);
    // Committing the same commit again should throw an exception
    // TODO: replace with actual exception
    assertThrows(ApiException.class, () -> coordinatedCommitsOperations.commit(commit1));
    coordinatedCommitsOperations.commit(commit2);
    coordinatedCommitsOperations.commit(commit3);

    GetCommits getCommits =
        new GetCommits()
            .tableId(tableInfo.getTableId())
            .tableUri(tableInfo.getStorageLocation())
            .startVersion(0L);
    GetCommitsResponse response = coordinatedCommitsOperations.getCommits(getCommits);
    assertEquals(3, response.getCommits().size());
    assertEquals(3, response.getLatestTableVersion());
    assertTrue(response.getCommits().contains(commit1.getCommitInfo()));
    assertTrue(response.getCommits().contains(commit2.getCommitInfo()));
    assertTrue(response.getCommits().contains(commit3.getCommitInfo()));

    getCommits.startVersion(2L).endVersion(null);
    response = coordinatedCommitsOperations.getCommits(getCommits);
    assertEquals(2, response.getCommits().size());
    assertEquals(3, response.getLatestTableVersion());
    assertFalse(response.getCommits().contains(commit1.getCommitInfo()));
    assertTrue(response.getCommits().contains(commit2.getCommitInfo()));
    assertTrue(response.getCommits().contains(commit3.getCommitInfo()));

    getCommits.startVersion(2L).endVersion(2L);
    response = coordinatedCommitsOperations.getCommits(getCommits);
    assertEquals(1, response.getCommits().size());
    assertEquals(3, response.getLatestTableVersion());
    assertFalse(response.getCommits().contains(commit1.getCommitInfo()));
    assertTrue(response.getCommits().contains(commit2.getCommitInfo()));
    assertFalse(response.getCommits().contains(commit3.getCommitInfo()));

    coordinatedCommitsOperations.commit(backfillOnlyCommit1);
    getCommits.startVersion(0L).endVersion(null);
    response = coordinatedCommitsOperations.getCommits(getCommits);
    assertEquals(1, response.getCommits().size());
    assertTrue(response.getCommits().contains(commit3.getCommitInfo()));
  }
}
