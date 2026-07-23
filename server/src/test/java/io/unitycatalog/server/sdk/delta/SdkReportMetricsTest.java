package io.unitycatalog.server.sdk.delta;

import static io.unitycatalog.server.utils.TestUtils.assertApiExceptionStatusOnly;
import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.ApiResponse;
import io.unitycatalog.client.delta.api.DeltaTablesApi;
import io.unitycatalog.client.delta.model.DeltaErrorType;
import io.unitycatalog.client.delta.model.DeltaReportMetricsRequest;
import io.unitycatalog.client.model.ColumnInfo;
import io.unitycatalog.client.model.ColumnTypeName;
import io.unitycatalog.client.model.CreateCatalog;
import io.unitycatalog.client.model.CreateSchema;
import io.unitycatalog.client.model.CreateTable;
import io.unitycatalog.client.model.DataSourceFormat;
import io.unitycatalog.client.model.TableInfo;
import io.unitycatalog.client.model.TableType;
import io.unitycatalog.server.base.BaseServerTest;
import io.unitycatalog.server.base.catalog.CatalogOperations;
import io.unitycatalog.server.base.schema.SchemaOperations;
import io.unitycatalog.server.base.table.TableOperations;
import io.unitycatalog.server.sdk.catalog.SdkCatalogOperations;
import io.unitycatalog.server.sdk.schema.SdkSchemaOperations;
import io.unitycatalog.server.sdk.tables.SdkTableOperations;
import io.unitycatalog.server.utils.TestUtils;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * End-to-end tests for the UC Delta API reportMetrics endpoint. Regression test for the missing
 * endpoint (issue #1654): the server advertised {@code POST .../tables/{table}/metrics} in its
 * config endpoint list but did not implement it, so every post-commit metrics report from Delta
 * clients failed with 404 and surfaced as a failed write.
 */
public class SdkReportMetricsTest extends BaseServerTest {

  private static final String TABLE_NAME = "metrics_test_table";

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

  private UUID createTable() throws ApiException, IOException {
    TableInfo table =
        tableOps.createTable(
            new CreateTable()
                .name(TABLE_NAME)
                .catalogName(TestUtils.CATALOG_NAME)
                .schemaName(TestUtils.SCHEMA_NAME)
                .tableType(TableType.EXTERNAL)
                .dataSourceFormat(DataSourceFormat.DELTA)
                .storageLocation("file:///tmp/uc_test/" + TABLE_NAME)
                .columns(
                    List.of(
                        new ColumnInfo()
                            .name("id")
                            .typeName(ColumnTypeName.LONG)
                            .typeText("bigint")
                            .typeJson(
                                "{\"name\":\"id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}}")
                            .position(0))));
    return UUID.fromString(table.getTableId());
  }

  @Test
  public void testReportMetrics() throws Exception {
    UUID tableId = createTable();

    // Success: matching table-id is acknowledged with 204 No Content
    ApiResponse<Void> response =
        deltaTablesApi.reportMetricsWithHttpInfo(
            TestUtils.CATALOG_NAME,
            TestUtils.SCHEMA_NAME,
            TABLE_NAME,
            new DeltaReportMetricsRequest().tableId(tableId));
    assertThat(response.getStatusCode()).isEqualTo(204);

    // Error: table-id not matching the path table returns 400
    TestUtils.assertDeltaApiException(
        () ->
            deltaTablesApi.reportMetrics(
                TestUtils.CATALOG_NAME,
                TestUtils.SCHEMA_NAME,
                TABLE_NAME,
                new DeltaReportMetricsRequest().tableId(UUID.randomUUID())),
        DeltaErrorType.INVALID_PARAMETER_VALUE_EXCEPTION,
        "does not match");

    // Error: missing table-id returns 400
    TestUtils.assertDeltaApiException(
        () ->
            deltaTablesApi.reportMetrics(
                TestUtils.CATALOG_NAME,
                TestUtils.SCHEMA_NAME,
                TABLE_NAME,
                new DeltaReportMetricsRequest()),
        DeltaErrorType.INVALID_PARAMETER_VALUE_EXCEPTION,
        "table-id");

    // Error: nonexistent table returns 404
    assertApiExceptionStatusOnly(
        () ->
            deltaTablesApi.reportMetrics(
                TestUtils.CATALOG_NAME,
                TestUtils.SCHEMA_NAME,
                "nonexistent_table",
                new DeltaReportMetricsRequest().tableId(tableId)),
        404);
  }
}
