package io.unitycatalog.server.sdk.volume;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.CreateCatalog;
import io.unitycatalog.client.model.CreateSchema;
import io.unitycatalog.client.model.CreateVolumeRequestContent;
import io.unitycatalog.client.model.VolumeInfo;
import io.unitycatalog.client.model.VolumeType;
import io.unitycatalog.server.base.BaseCRUDTest;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.catalog.CatalogOperations;
import io.unitycatalog.server.base.schema.SchemaOperations;
import io.unitycatalog.server.base.volume.VolumeOperations;
import io.unitycatalog.server.cleanup.StorageCleanupTestSupport;
import io.unitycatalog.server.sdk.catalog.SdkCatalogOperations;
import io.unitycatalog.server.sdk.schema.SdkSchemaOperations;
import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.TestUtils;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * End-to-end coverage for managed-volume storage cleanup, mirroring {@code
 * SdkManagedTableCleanupTest}. Creating the volume with no storage location makes the server
 * generate the managed location (via {@code getManagedLocationForVolume}); dropping it must run
 * that real generated location through the live cleanup worker (whose {@code validateTask} requires
 * a {@code /volumes/<id>} suffix) and reclaim the storage. Other volume tests hand-write the {@code
 * /volumes/<id>} path, so this is the only place the generation and validation sides are exercised
 * together against a running worker.
 */
public class SdkManagedVolumeCleanupTest extends BaseCRUDTest {
  private static final Duration CLEANUP_DEADLINE = Duration.ofSeconds(5);
  private static final String VOLUME_NAME = "uc_managedvolume";
  private static final String VOLUME_FULL_NAME =
      TestUtils.CATALOG_NAME + "." + TestUtils.SCHEMA_NAME + "." + VOLUME_NAME;

  private SchemaOperations schemaOperations;
  private VolumeOperations volumeOperations;

  @Override
  protected void setUpProperties() {
    super.setUpProperties();
    StorageCleanupTestSupport.configureFastCleanup(serverProperties);
  }

  @Override
  protected CatalogOperations createCatalogOperations(ServerConfig config) {
    return new SdkCatalogOperations(TestUtils.createApiClient(config));
  }

  @BeforeEach
  @Override
  public void setUp() {
    super.setUp();
    schemaOperations = new SdkSchemaOperations(TestUtils.createApiClient(serverConfig));
    volumeOperations = new SdkVolumeOperations(TestUtils.createApiClient(serverConfig));
    createCommonResources();
  }

  private void createCommonResources() {
    // A managed volume derives its location from the catalog/schema managed storage and does not
    // fall back to the TABLE_STORAGE_ROOT server property (unlike managed tables), so the catalog
    // must be created with a storage root.
    try {
      catalogOperations.createCatalog(
          new CreateCatalog()
              .name(TestUtils.CATALOG_NAME)
              .comment(TestUtils.COMMENT)
              .storageRoot(NormalizedURL.normalize(testDirectoryRoot.toString())));
      schemaOperations.createSchema(
          new CreateSchema().name(TestUtils.SCHEMA_NAME).catalogName(TestUtils.CATALOG_NAME));
    } catch (ApiException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testManagedVolumeDropDeletesLocalStorageAndCleanupTask() throws Exception {
    VolumeInfo volume =
        volumeOperations.createVolume(
            new CreateVolumeRequestContent()
                .name(VOLUME_NAME)
                .catalogName(TestUtils.CATALOG_NAME)
                .schemaName(TestUtils.SCHEMA_NAME)
                .volumeType(VolumeType.MANAGED));
    Path volumeDirectory = Path.of(URI.create(volume.getStorageLocation()));
    Path marker = volumeDirectory.resolve("data/marker.txt");
    Files.createDirectories(marker.getParent());
    Files.writeString(marker, "data");

    volumeOperations.deleteVolume(VOLUME_FULL_NAME);

    assertThatThrownBy(() -> volumeOperations.getVolume(VOLUME_FULL_NAME))
        .isInstanceOf(ApiException.class);
    StorageCleanupTestSupport.awaitCleanup(
        hibernateConfigurator.getSessionFactory(),
        UUID.fromString(volume.getVolumeId()),
        CLEANUP_DEADLINE,
        marker,
        volumeDirectory);
  }
}
