package io.unitycatalog.server.sdk.tables;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.TableInfo;
import io.unitycatalog.client.model.TableType;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.catalog.CatalogOperations;
import io.unitycatalog.server.base.schema.SchemaOperations;
import io.unitycatalog.server.base.table.BaseTableCRUDTestEnv;
import io.unitycatalog.server.base.table.TableOperations;
import io.unitycatalog.server.persist.dao.StorageCleanupTaskDAO;
import io.unitycatalog.server.sdk.catalog.SdkCatalogOperations;
import io.unitycatalog.server.sdk.schema.SdkSchemaOperations;
import io.unitycatalog.server.utils.ServerProperties.Property;
import io.unitycatalog.server.utils.TestUtils;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import org.hibernate.Session;
import org.junit.jupiter.api.Test;

public class SdkManagedTableCleanupTest extends BaseTableCRUDTestEnv {
  private static final Duration CLEANUP_DEADLINE = Duration.ofSeconds(5);

  @Override
  protected void setUpProperties() {
    super.setUpProperties();
    serverProperties.setProperty(Property.STORAGE_CLEANUP_POLL_INTERVAL.getKey(), "PT0.01S");
    serverProperties.setProperty(Property.STORAGE_CLEANUP_INITIAL_DELAY.getKey(), "PT0.001S");
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

    assertThatThrownBy(() -> tableOperations.getTable(TestUtils.TABLE_FULL_NAME))
        .isInstanceOf(ApiException.class);
    awaitCleanup(UUID.fromString(table.getTableId()), marker, tableDirectory);
  }

  private void awaitCleanup(UUID tableId, Path marker, Path tableDirectory)
      throws InterruptedException {
    long deadline = System.nanoTime() + CLEANUP_DEADLINE.toNanos();
    while (System.nanoTime() < deadline) {
      if (Files.notExists(marker)
          && Files.notExists(tableDirectory)
          && findCleanupTask(tableId) == null) {
        return;
      }
      Thread.sleep(10);
    }
    assertThat(marker).doesNotExist();
    assertThat(tableDirectory).doesNotExist();
    assertThat(findCleanupTask(tableId)).isNull();
  }

  private StorageCleanupTaskDAO findCleanupTask(UUID resourceId) {
    try (Session session = hibernateConfigurator.getSessionFactory().openSession()) {
      return session.get(StorageCleanupTaskDAO.class, resourceId);
    }
  }
}
