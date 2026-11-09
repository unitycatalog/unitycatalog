package io.unitycatalog.server.sdk.tables;

import static io.unitycatalog.server.utils.TestUtils.sendRawGet;
import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.model.TableInfo;
import io.unitycatalog.client.model.TableType;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.catalog.CatalogOperations;
import io.unitycatalog.server.base.schema.SchemaOperations;
import io.unitycatalog.server.base.table.BaseTableCRUDTestEnv;
import io.unitycatalog.server.base.table.TableOperations;
import io.unitycatalog.server.cleanup.StorageCleanupTestSupport;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.sdk.catalog.SdkCatalogOperations;
import io.unitycatalog.server.sdk.schema.SdkSchemaOperations;
import io.unitycatalog.server.utils.TestUtils;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;

public class SdkManagedTableCleanupTest extends BaseTableCRUDTestEnv {
  private static final Duration CLEANUP_DEADLINE = Duration.ofSeconds(5);

  @Override
  protected void setUpProperties() {
    super.setUpProperties();
    observabilityEnabled = true;
    StorageCleanupTestSupport.configureFastCleanup(serverProperties);
  }

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
  public void testManagedTableDropDeletesLocalStorageAndCleanupTask() throws Exception {
    TableInfo table =
        createTestingTable(
            TestUtils.TABLE_NAME, TableType.MANAGED, Optional.empty(), tableOperations);
    Path tableDirectory = Path.of(URI.create(table.getStorageLocation()));
    Path marker = tableDirectory.resolve("data/marker.txt");
    Files.createDirectories(marker.getParent());
    Files.writeString(marker, "data");

    tableOperations.deleteTable(TestUtils.TABLE_FULL_NAME);

    TestUtils.assertApiException(
        () -> tableOperations.getTable(TestUtils.TABLE_FULL_NAME),
        ErrorCode.TABLE_NOT_FOUND,
        "Table not found: " + TestUtils.TABLE_FULL_NAME);
    StorageCleanupTestSupport.awaitCleanup(
        hibernateConfigurator.getSessionFactory(),
        UUID.fromString(table.getTableId()),
        CLEANUP_DEADLINE,
        marker,
        tableDirectory);
    // The worker counts the attempt just after it removes the task, so wait for the count too.
    long deadline = System.nanoTime() + CLEANUP_DEADLINE.toNanos();
    while (tableCleanupSuccesses() < 1.0 && System.nanoTime() < deadline) {
      Thread.sleep(10);
    }
    assertThat(tableCleanupSuccesses()).isEqualTo(1.0);
  }

  private double tableCleanupSuccesses() throws Exception {
    String series =
        "uc_storage_cleanup_attempts_total{resource_type=\"table\",result=\"success\"} ";
    return sendRawGet(observabilityServerConfig, "/metrics")
        .body()
        .lines()
        .filter(line -> line.startsWith(series))
        .mapToDouble(line -> Double.parseDouble(line.substring(series.length())))
        .findFirst()
        .orElseThrow();
  }
}
