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
    String source = "renameSource";
    String existingTarget = "existingTarget";
    String missingSource = "missingSource";
    String unusedTarget = "unusedTarget";

    // Missing or blank target names return 400 before source lookup.
    for (String newName : new String[] {null, "", " ", "\t"}) {
      assertThatThrownBy(
              () ->
                  deltaTablesApi.renameTable(
                      TestUtils.CATALOG_NAME,
                      TestUtils.SCHEMA_NAME,
                      source,
                      new DeltaRenameTableRequest().newName(newName)))
          .isInstanceOfSatisfying(
              ApiException.class,
              e -> {
                assertThat(e.getCode()).isEqualTo(400);
                assertThat(e.getMessage()).contains("New table name is required");
              });
    }

    // A missing source returns 404 whether the target is unused, unchanged, or already exists.
    createDeltaManaged(existingTarget, Map.of());
    for (String newName : new String[] {unusedTarget, missingSource, existingTarget}) {
      TestUtils.assertDeltaApiException(
          () ->
              deltaTablesApi.renameTable(
                  TestUtils.CATALOG_NAME,
                  TestUtils.SCHEMA_NAME,
                  missingSource,
                  new DeltaRenameTableRequest().newName(newName)),
          DeltaErrorType.NO_SUCH_TABLE_EXCEPTION,
          "Table not found");
    }

    createDeltaManaged(source, Map.of());
    DeltaLoadTableResponse originalTable =
        deltaTablesApi.loadTable(TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, source);

    // Renaming to an existing target returns 409.
    TestUtils.assertDeltaApiException(
        () ->
            deltaTablesApi.renameTable(
                TestUtils.CATALOG_NAME,
                TestUtils.SCHEMA_NAME,
                source,
                new DeltaRenameTableRequest().newName(existingTarget)),
        DeltaErrorType.ALREADY_EXISTS_EXCEPTION,
        "already exists");

    // Renaming a table to its current name is a no-op.
    ApiResponse<Void> noOpResponse =
        deltaTablesApi.renameTableWithHttpInfo(
            TestUtils.CATALOG_NAME,
            TestUtils.SCHEMA_NAME,
            source,
            new DeltaRenameTableRequest().newName(source));
    assertThat(noOpResponse.getStatusCode()).isEqualTo(204);

    DeltaLoadTableResponse afterNoOp =
        deltaTablesApi.loadTable(TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, source);
    assertTableStateUnchanged(originalTable, afterNoOp);
    assertThat(afterNoOp.getMetadata().getUpdatedTime())
        .isEqualTo(originalTable.getMetadata().getUpdatedTime());
    assertThat(afterNoOp.getMetadata().getEtag()).isEqualTo(originalTable.getMetadata().getEtag());

    // A successful rename preserves table state and removes the old name.
    ApiResponse<Void> renameResponse =
        deltaTablesApi.renameTableWithHttpInfo(
            TestUtils.CATALOG_NAME,
            TestUtils.SCHEMA_NAME,
            source,
            new DeltaRenameTableRequest().newName(unusedTarget));
    assertThat(renameResponse.getStatusCode()).isEqualTo(204);

    DeltaLoadTableResponse renamedTable =
        deltaTablesApi.loadTable(TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, unusedTarget);
    assertTableStateUnchanged(afterNoOp, renamedTable);

    TestUtils.assertDeltaApiException(
        () -> deltaTablesApi.loadTable(TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, source),
        DeltaErrorType.NO_SUCH_TABLE_EXCEPTION,
        "Table not found");
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
