package io.unitycatalog.server.sdk.delta;

import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.ApiResponse;
import io.unitycatalog.client.delta.api.DeltaTablesApi;
import io.unitycatalog.client.delta.model.DeltaErrorType;
import io.unitycatalog.client.delta.model.DeltaLoadTableResponse;
import io.unitycatalog.client.delta.model.DeltaRenameTableRequest;
import io.unitycatalog.client.model.CreateCatalog;
import io.unitycatalog.client.model.CreateSchema;
import io.unitycatalog.client.model.TableType;
import io.unitycatalog.server.base.BaseServerTest;
import io.unitycatalog.server.base.catalog.CatalogOperations;
import io.unitycatalog.server.base.schema.SchemaOperations;
import io.unitycatalog.server.base.table.BaseTableCRUDTestEnv;
import io.unitycatalog.server.base.table.TableOperations;
import io.unitycatalog.server.sdk.catalog.SdkCatalogOperations;
import io.unitycatalog.server.sdk.schema.SdkSchemaOperations;
import io.unitycatalog.server.sdk.tables.SdkTableOperations;
import io.unitycatalog.server.utils.TestUtils;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * E2E tests for the Delta REST Catalog {@code POST .../rename} endpoint (204 on success, 404 when
 * the source table is missing, 409 when the target name already exists).
 */
public class SdkRenameTableTest extends BaseServerTest {

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
  public void testRenameTableEndpoint() throws Exception {
    // -------- Happy path: rename returns 204, new name loads, old name is gone --------
    String oldName = "tbl_rename_src";
    String newName = "tbl_rename_dst";
    BaseTableCRUDTestEnv.createTestingTable(oldName, TableType.MANAGED, Optional.empty(), tableOps);
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
    assertThat(renamedTable.getMetadata().getTableUuid())
        .isEqualTo(originalTable.getMetadata().getTableUuid());
    assertThat(renamedTable.getMetadata().getLocation())
        .isEqualTo(originalTable.getMetadata().getLocation());
    assertThat(renamedTable.getMetadata().getCreatedTime())
        .isEqualTo(originalTable.getMetadata().getCreatedTime());
    assertThat(renamedTable.getMetadata().getProperties())
        .isEqualTo(originalTable.getMetadata().getProperties());
    assertThat(renamedTable.getCommits()).isEqualTo(originalTable.getCommits());
    assertThat(renamedTable.getLatestTableVersion())
        .isEqualTo(originalTable.getLatestTableVersion());

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
    BaseTableCRUDTestEnv.createTestingTable(source, TableType.MANAGED, Optional.empty(), tableOps);
    BaseTableCRUDTestEnv.createTestingTable(target, TableType.MANAGED, Optional.empty(), tableOps);

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
}
