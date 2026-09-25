package io.unitycatalog.server.sdk.volume;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.CreateVolumeRequestContent;
import io.unitycatalog.client.model.VolumeInfo;
import io.unitycatalog.client.model.VolumeType;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.catalog.CatalogOperations;
import io.unitycatalog.server.base.schema.SchemaOperations;
import io.unitycatalog.server.base.volume.BaseVolumeCRUDTestEnv;
import io.unitycatalog.server.base.volume.VolumeOperations;
import io.unitycatalog.server.cleanup.StorageCleanupTestSupport;
import io.unitycatalog.server.sdk.catalog.SdkCatalogOperations;
import io.unitycatalog.server.sdk.schema.SdkSchemaOperations;
import io.unitycatalog.server.utils.TestUtils;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.UUID;
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
public class SdkManagedVolumeCleanupTest extends BaseVolumeCRUDTestEnv {
  private static final Duration CLEANUP_DEADLINE = Duration.ofSeconds(5);
  private static final String VOLUME_NAME = "uc_managedvolume";
  private static final String VOLUME_FULL_NAME =
      TestUtils.CATALOG_NAME + "." + TestUtils.SCHEMA_NAME + "." + VOLUME_NAME;

  @Override
  protected void setUpProperties() {
    super.setUpProperties();
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
  protected VolumeOperations createVolumeOperations(ServerConfig config) {
    return new SdkVolumeOperations(TestUtils.createApiClient(config));
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
