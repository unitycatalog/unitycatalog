package io.unitycatalog.server.sdk.delta;

import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.ApiResponse;
import io.unitycatalog.client.delta.model.DeltaErrorType;
import io.unitycatalog.client.delta.model.DeltaReportMetricsRequest;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.catalog.CatalogOperations;
import io.unitycatalog.server.base.delta.DeltaBaseTableCRUDTestEnv;
import io.unitycatalog.server.base.schema.SchemaOperations;
import io.unitycatalog.server.sdk.catalog.SdkCatalogOperations;
import io.unitycatalog.server.sdk.schema.SdkSchemaOperations;
import io.unitycatalog.server.utils.TestUtils;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * E2E tests for the Delta REST Catalog {@code POST .../metrics} endpoint (204 on success, 400 when
 * the body {@code table-id} is missing or does not match the path table, 404 when the table is
 * missing).
 */
public class SdkReportMetricsTest extends DeltaBaseTableCRUDTestEnv {

  @Override
  protected CatalogOperations createCatalogOperations(ServerConfig serverConfig) {
    return new SdkCatalogOperations(TestUtils.createApiClient(serverConfig));
  }

  @Override
  protected SchemaOperations createSchemaOperations(ServerConfig serverConfig) {
    return new SdkSchemaOperations(TestUtils.createApiClient(serverConfig));
  }

  @Test
  public void testReportMetricsEndpoint() throws Exception {
    Handle table = createDeltaManaged("metricsTable", Map.of());

    // A matching table-id is acknowledged with 204 No Content.
    ApiResponse<Void> response =
        deltaTablesApi.reportMetricsWithHttpInfo(
            TestUtils.CATALOG_NAME,
            TestUtils.SCHEMA_NAME,
            table.name(),
            new DeltaReportMetricsRequest().tableId(table.tableId()));
    assertThat(response.getStatusCode()).isEqualTo(204);

    // A missing table-id is rejected before the table is looked up.
    TestUtils.assertDeltaApiException(
        () ->
            deltaTablesApi.reportMetrics(
                TestUtils.CATALOG_NAME,
                TestUtils.SCHEMA_NAME,
                table.name(),
                new DeltaReportMetricsRequest()),
        DeltaErrorType.INVALID_PARAMETER_VALUE_EXCEPTION,
        "table-id is required");

    // A table-id that resolves to a different table than the path is rejected.
    TestUtils.assertDeltaApiException(
        () ->
            deltaTablesApi.reportMetrics(
                TestUtils.CATALOG_NAME,
                TestUtils.SCHEMA_NAME,
                table.name(),
                new DeltaReportMetricsRequest().tableId(UUID.randomUUID())),
        DeltaErrorType.INVALID_PARAMETER_VALUE_EXCEPTION,
        "does not match table");

    // A missing table returns 404 even when the table-id is well-formed.
    TestUtils.assertDeltaApiException(
        () ->
            deltaTablesApi.reportMetrics(
                TestUtils.CATALOG_NAME,
                TestUtils.SCHEMA_NAME,
                "missingTable",
                new DeltaReportMetricsRequest().tableId(table.tableId())),
        DeltaErrorType.NO_SUCH_TABLE_EXCEPTION,
        "Table not found");
  }
}
