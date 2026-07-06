package io.unitycatalog.server.sdk.delta;

import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.ApiResponse;
import io.unitycatalog.client.delta.api.DeltaTablesApi;
import io.unitycatalog.client.delta.model.DeltaErrorType;
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

    // -------- Not-found: deleting a missing table returns 404 NoSuchTableException --------
    TestUtils.assertDeltaApiException(
        () ->
            deltaTablesApi.deleteTable(
                TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, "nonexistent"),
        DeltaErrorType.NO_SUCH_TABLE_EXCEPTION,
        "Table not found");
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
