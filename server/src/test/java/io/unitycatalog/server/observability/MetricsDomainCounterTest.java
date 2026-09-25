package io.unitycatalog.server.observability;

import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.catalog.CatalogOperations;
import io.unitycatalog.server.base.schema.SchemaOperations;
import io.unitycatalog.server.base.table.BaseTableCRUDTestEnv;
import io.unitycatalog.server.base.table.TableOperations;
import io.unitycatalog.server.sdk.catalog.SdkCatalogOperations;
import io.unitycatalog.server.sdk.schema.SdkSchemaOperations;
import io.unitycatalog.server.sdk.tables.SdkTableOperations;
import io.unitycatalog.server.utils.TestUtils;
import java.net.http.HttpResponse;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;

/**
 * End-to-end integration test that verifies the {@code uc_tables_created} counter is exported on
 * the live {@code /metrics} scrape endpoint after a table is created via the SDK.
 */
public class MetricsDomainCounterTest extends BaseTableCRUDTestEnv {

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

  @Test
  @SneakyThrows
  public void tableCreateAppearsOnMetricsScrape() {
    // Create a table via the SDK (catalog and schema are created in setUp by BaseTableCRUDTestEnv).
    createAndVerifyExternalTable();

    // Scrape the live /metrics endpoint and assert the uc_tables_created counter is present.
    HttpResponse<String> response = httpGet("/metrics");

    assertThat(response.statusCode()).isEqualTo(200);
    assertThat(response.body()).contains("uc_tables_created");

    // Parse and verify the counter value increased after creating a table.
    double created =
        response
            .body()
            .lines()
            // Match the sample line whether or not the counter gains label tags later:
            //   uc_tables_created_total 1.0   OR   uc_tables_created_total{k="v"} 1.0
            // (HELP/TYPE comment lines start with '#', so they are excluded.)
            .filter(line -> line.startsWith("uc_tables_created_total"))
            .mapToDouble(line -> Double.parseDouble(line.substring(line.lastIndexOf(' ') + 1)))
            .findFirst()
            .orElse(0.0);
    assertThat(created)
        .as("uc_tables_created_total should be >= 1 after creating a table")
        .isGreaterThanOrEqualTo(1.0);

    // The create-table call above is a real API request, so the MetricCollectingService decorator
    // (installed by ArmeriaServerBuilder.meterRegistry) must have recorded per-request http_server_
    // series. This guards against that decorator being dropped. (Health-check routes like /livez
    // are
    // not recorded by the decorator, so this assertion relies on the SDK's API traffic.)
    assertThat(response.body()).contains("http_server_");
  }
}
