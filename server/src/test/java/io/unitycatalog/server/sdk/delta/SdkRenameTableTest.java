package io.unitycatalog.server.sdk.delta;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.ApiResponse;
import io.unitycatalog.client.delta.model.DeltaErrorType;
import io.unitycatalog.client.delta.model.DeltaLoadTableResponse;
import io.unitycatalog.client.delta.model.DeltaRenameTableRequest;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.catalog.CatalogOperations;
import io.unitycatalog.server.base.delta.DeltaBaseTableCRUDTestEnv;
import io.unitycatalog.server.base.schema.SchemaOperations;
import io.unitycatalog.server.sdk.catalog.SdkCatalogOperations;
import io.unitycatalog.server.sdk.schema.SdkSchemaOperations;
import io.unitycatalog.server.utils.TestUtils;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * E2E tests for the Delta REST Catalog {@code POST .../rename} endpoint (204 on success, 404 when
 * the source table is missing, 409 when the target name already exists).
 */
public class SdkRenameTableTest extends DeltaBaseTableCRUDTestEnv {

  @Override
  protected CatalogOperations createCatalogOperations(ServerConfig serverConfig) {
    return new SdkCatalogOperations(TestUtils.createApiClient(serverConfig));
  }

  @Override
  protected SchemaOperations createSchemaOperations(ServerConfig serverConfig) {
    return new SdkSchemaOperations(TestUtils.createApiClient(serverConfig));
  }

  @Test
  public void testRenameTableEndpoint() throws Exception {
    // -------- Happy path: rename returns 204, new name loads, old name is gone --------
    String oldName = "tbl_rename_src";
    String newName = "tbl_rename_dst";
    createDeltaManaged(oldName, Map.of());
    DeltaLoadTableResponse originalTable =
        deltaTablesApi.loadTable(TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, oldName);

    ApiResponse<Void> response =
        deltaTablesApi.renameTableWithHttpInfo(
            TestUtils.CATALOG_NAME,
            TestUtils.SCHEMA_NAME,
            oldName,
            new DeltaRenameTableRequest().newName(newName));
    assertThat(response.getStatusCode()).isEqualTo(204);

    // The new name resolves to the original table with its metadata and commit state intact.
    DeltaLoadTableResponse renamedTable =
        deltaTablesApi.loadTable(TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, newName);
    assertTableStateUnchanged(originalTable, renamedTable);

    // Old name no longer resolves.
    TestUtils.assertDeltaApiException(
        () -> deltaTablesApi.loadTable(TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, oldName),
        DeltaErrorType.NO_SUCH_TABLE_EXCEPTION,
        "Table not found");

    // -------- Not-found: renaming a missing source table returns 404 NoSuchTableException --------
    TestUtils.assertDeltaApiException(
        () ->
            deltaTablesApi.renameTable(
                TestUtils.CATALOG_NAME,
                TestUtils.SCHEMA_NAME,
                "nonexistent",
                new DeltaRenameTableRequest().newName("whatever")),
        DeltaErrorType.NO_SUCH_TABLE_EXCEPTION,
        "Table not found");
  }

  @Test
  public void testRenameToExistingTargetConflicts() throws Exception {
    // -------- Conflict: renaming onto an existing name returns 409 AlreadyExistsException --------
    String source = "tbl_rename_conflict_src";
    String target = "tbl_rename_conflict_dst";
    createDeltaManaged(source, Map.of());
    createDeltaManaged(target, Map.of());

    TestUtils.assertDeltaApiException(
        () ->
            deltaTablesApi.renameTable(
                TestUtils.CATALOG_NAME,
                TestUtils.SCHEMA_NAME,
                source,
                new DeltaRenameTableRequest().newName(target)),
        DeltaErrorType.ALREADY_EXISTS_EXCEPTION,
        "already exists");
  }

  @Test
  public void testRenameToSameNameIsNoOp() throws Exception {
    String tableName = "tbl_rename_same_name";
    createDeltaManaged(tableName, Map.of());
    DeltaLoadTableResponse originalTable =
        deltaTablesApi.loadTable(TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, tableName);

    ApiResponse<Void> response =
        deltaTablesApi.renameTableWithHttpInfo(
            TestUtils.CATALOG_NAME,
            TestUtils.SCHEMA_NAME,
            tableName,
            new DeltaRenameTableRequest().newName(tableName));

    assertThat(response.getStatusCode()).isEqualTo(204);
    DeltaLoadTableResponse unchangedTable =
        deltaTablesApi.loadTable(TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, tableName);
    assertTableStateUnchanged(originalTable, unchangedTable);
  }

  @ParameterizedTest
  @NullSource
  @ValueSource(strings = {"", " ", "\t"})
  public void testRenameTableRejectsMissingNewName(String newName) {
    assertThatThrownBy(
            () ->
                deltaTablesApi.renameTable(
                    TestUtils.CATALOG_NAME,
                    TestUtils.SCHEMA_NAME,
                    "tbl_rename_src",
                    new DeltaRenameTableRequest().newName(newName)))
        .isInstanceOfSatisfying(
            ApiException.class,
            e -> {
              assertThat(e.getCode()).isEqualTo(400);
              assertThat(e.getMessage()).contains("New table name is required");
            });
  }

  private static void assertTableStateUnchanged(
      DeltaLoadTableResponse originalTable, DeltaLoadTableResponse currentTable) {
    assertThat(currentTable.getMetadata().getTableUuid())
        .isEqualTo(originalTable.getMetadata().getTableUuid());
    assertThat(currentTable.getMetadata().getLocation())
        .isEqualTo(originalTable.getMetadata().getLocation());
    assertThat(currentTable.getMetadata().getCreatedTime())
        .isEqualTo(originalTable.getMetadata().getCreatedTime());
    assertThat(currentTable.getMetadata().getProperties())
        .isEqualTo(originalTable.getMetadata().getProperties());
    assertThat(currentTable.getCommits()).isEqualTo(originalTable.getCommits());
    assertThat(currentTable.getLatestTableVersion())
        .isEqualTo(originalTable.getLatestTableVersion());
  }
}
