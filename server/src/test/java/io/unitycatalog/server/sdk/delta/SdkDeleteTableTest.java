package io.unitycatalog.server.sdk.delta;

import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.ApiResponse;
import io.unitycatalog.client.delta.api.DeltaTablesApi;
import io.unitycatalog.client.delta.model.DeltaErrorType;
import io.unitycatalog.client.model.CreateCatalog;
import io.unitycatalog.client.model.CreateSchema;
import io.unitycatalog.client.model.DataSourceFormat;
import io.unitycatalog.client.model.TableInfo;
import io.unitycatalog.client.model.TableType;
import io.unitycatalog.server.base.BaseServerTest;
import io.unitycatalog.server.base.catalog.CatalogOperations;
import io.unitycatalog.server.base.schema.SchemaOperations;
import io.unitycatalog.server.base.table.BaseTableCRUDTestEnv;
import io.unitycatalog.server.base.table.TableOperations;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.persist.dao.ColumnInfoDAO;
import io.unitycatalog.server.persist.dao.TableInfoDAO;
import io.unitycatalog.server.sdk.catalog.SdkCatalogOperations;
import io.unitycatalog.server.sdk.schema.SdkSchemaOperations;
import io.unitycatalog.server.sdk.tables.SdkTableOperations;
import io.unitycatalog.server.utils.TestUtils;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.hibernate.LockMode;
import org.hibernate.Session;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * E2E tests for the Delta REST Catalog {@code DELETE table} endpoint (204 on success, 404 on
 * missing).
 */
public class SdkDeleteTableTest extends BaseServerTest {

  private CatalogOperations catalogOps;
  private SchemaOperations schemaOps;
  private TableOperations tableOps;
  private DeltaTablesApi deltaTablesApi;

  @BeforeEach
  public void setUp() {
    super.setUp();
    var apiClient = TestUtils.createApiClient(serverConfig);
    catalogOps = new SdkCatalogOperations(apiClient);
    schemaOps = new SdkSchemaOperations(apiClient);
    tableOps = new SdkTableOperations(apiClient);
    deltaTablesApi = new DeltaTablesApi(apiClient);
    cleanUp();
    createCatalogAndSchema();
  }

  private void cleanUp() {
    try {
      catalogOps.deleteCatalog(TestUtils.CATALOG_NAME, Optional.of(true));
    } catch (Exception e) {
      // Ignore
    }
  }

  private void createCatalogAndSchema() {
    try {
      catalogOps.createCatalog(new CreateCatalog().name(TestUtils.CATALOG_NAME).comment("test"));
      schemaOps.createSchema(
          new CreateSchema().name(TestUtils.SCHEMA_NAME).catalogName(TestUtils.CATALOG_NAME));
    } catch (ApiException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testDeleteTableEndpoint() throws Exception {
    createAndAssertDeleteThenNotFound(
        "tbl_delete_external",
        TableType.EXTERNAL,
        Optional.of("file:///tmp/uc_test/tbl_delete_external"));

    createAndAssertDeleteThenNotFound("tbl_delete_managed", TableType.MANAGED, Optional.empty());

    // -------- A non-Delta table is rejected and left in place --------
    TableInfo parquet =
        BaseTableCRUDTestEnv.createTestingTable(
            "tbl_delete_parquet",
            TableType.EXTERNAL,
            Optional.of("file:///tmp/uc_test/tbl_delete_parquet"),
            DataSourceFormat.PARQUET,
            tableOps);
    TestUtils.assertDeltaApiException(
        () ->
            deltaTablesApi.deleteTable(
                TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, parquet.getName()),
        DeltaErrorType.UNSUPPORTED_TABLE_FORMAT_EXCEPTION,
        "Table is not a Delta table");
    assertThat(
            tableOps
                .getTable(
                    String.join(
                        ".", TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, parquet.getName()))
                .getTableId())
        .isEqualTo(parquet.getTableId());

    // -------- Not-found: deleting a missing table returns 404 NoSuchTableException --------
    TestUtils.assertDeltaApiException(
        () ->
            deltaTablesApi.deleteTable(
                TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, "nonexistent"),
        DeltaErrorType.NO_SUCH_TABLE_EXCEPTION,
        "Table not found");
  }

  /** A rename that commits in between must not make the drop delete the renamed table. */
  @Test
  public void dropRacingARenameDoesNotDeleteTheRenamedTable() throws Exception {
    String oldName = TestUtils.SCHEMA_FULL_NAME + ".tbl_drop_race";
    TableInfo table =
        BaseTableCRUDTestEnv.createTestingTable(
            "tbl_drop_race",
            TableType.EXTERNAL,
            Optional.of("file:///tmp/uc_test/tbl_drop_race"),
            tableOps);

    TestUtils.assertApiException(
        () ->
            runBlockedByHeldWrite(
                held ->
                    setColumn(
                        held,
                        TableInfoDAO.class,
                        UUID.fromString(table.getTableId()),
                        "name",
                        "tbl_drop_race_renamed"),
                () -> {
                  tableOps.deleteTable(oldName);
                  return null;
                }),
        ErrorCode.TABLE_NOT_FOUND,
        "Table not found");

    assertThat(tableOps.getTable(oldName + "_renamed").getTableId()).isEqualTo(table.getTableId());
    // The refused drop leaves the renamed table's columns in place.
    assertThat(columnRows(table.getTableId())).isEqualTo(2);
  }

  /**
   * A commit that replaces the table's column rows while the drop waits for the table lock (as an
   * Iceberg commit does) must not make the drop fail or leave column rows behind.
   */
  @Test
  public void dropWaitingBehindAColumnRewriteDeletesTheNewColumns() throws Exception {
    TableInfo table =
        BaseTableCRUDTestEnv.createTestingTable(
            "tbl_drop_rewrite", TableType.EXTERNAL, Optional.of("file:///tmp/uc_test/c"), tableOps);
    UUID id = UUID.fromString(table.getTableId());

    runBlockedByHeldWrite(
        held -> {
          TableInfoDAO locked = held.get(TableInfoDAO.class, id, LockMode.PESSIMISTIC_WRITE);
          List<ColumnInfoDAO> rewritten =
              ColumnInfoDAO.fromList(ColumnInfoDAO.toList(locked.getColumns()));
          rewritten.forEach(
              column -> {
                column.setId(UUID.randomUUID());
                column.setTable(locked);
              });
          locked.getColumns().clear();
          held.flush();
          locked.getColumns().addAll(rewritten);
        },
        () -> {
          tableOps.deleteTable(TestUtils.SCHEMA_FULL_NAME + ".tbl_drop_rewrite");
          return null;
        });

    assertThat(columnRows(table.getTableId())).isZero();
    try (Session session = testDatabase.sessionFactory().openSession()) {
      assertThat(session.get(TableInfoDAO.class, id)).isNull();
    }
  }

  /** A drop deletes the table's row and its column rows, and no other table's columns. */
  @Test
  public void dropDeletesOnlyTheDroppedTablesColumns() throws Exception {
    TableInfo dropped =
        BaseTableCRUDTestEnv.createTestingTable(
            "tbl_drop_columns", TableType.EXTERNAL, Optional.of("file:///tmp/uc_test/a"), tableOps);
    TableInfo kept =
        BaseTableCRUDTestEnv.createTestingTable(
            "tbl_keep_columns", TableType.EXTERNAL, Optional.of("file:///tmp/uc_test/b"), tableOps);
    assertThat(columnRows(dropped.getTableId())).isEqualTo(2);

    tableOps.deleteTable(TestUtils.SCHEMA_FULL_NAME + ".tbl_drop_columns");

    assertThat(columnRows(dropped.getTableId())).isZero();
    try (Session session = testDatabase.sessionFactory().openSession()) {
      assertThat(session.get(TableInfoDAO.class, UUID.fromString(dropped.getTableId()))).isNull();
    }
    assertThat(columnRows(kept.getTableId())).isEqualTo(2);
  }

  private long columnRows(String tableId) {
    try (Session session = testDatabase.sessionFactory().openSession()) {
      return session
          .createSelectionQuery(
              "select count(*) from ColumnInfoDAO where table.id = :id", Long.class)
          .setParameter("id", UUID.fromString(tableId))
          .getSingleResult();
    }
  }

  /**
   * Creates a table, deletes it via the Delta REST endpoint, and asserts the delete returns 204 and
   * the table is no longer loadable.
   */
  private void createAndAssertDeleteThenNotFound(
      String tableName, TableType tableType, Optional<String> storageLocation) throws Exception {
    BaseTableCRUDTestEnv.createTestingTable(tableName, tableType, storageLocation, tableOps);

    ApiResponse<Void> response =
        deltaTablesApi.deleteTableWithHttpInfo(
            TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, tableName);
    assertThat(response.getStatusCode()).isEqualTo(204);

    TestUtils.assertDeltaApiException(
        () -> deltaTablesApi.loadTable(TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, tableName),
        DeltaErrorType.NO_SUCH_TABLE_EXCEPTION,
        "Table not found");
  }
}
