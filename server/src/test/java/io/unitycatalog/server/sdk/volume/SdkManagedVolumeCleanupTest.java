package io.unitycatalog.server.sdk.volume;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.CatalogsApi;
import io.unitycatalog.client.api.SchemasApi;
import io.unitycatalog.client.api.VolumesApi;
import io.unitycatalog.client.model.CreateCatalog;
import io.unitycatalog.client.model.CreateSchema;
import io.unitycatalog.client.model.CreateVolumeRequestContent;
import io.unitycatalog.client.model.VolumeInfo;
import io.unitycatalog.client.model.VolumeType;
import io.unitycatalog.server.base.BaseServerTest;
import io.unitycatalog.server.persist.dao.StorageCleanupTaskDAO;
import io.unitycatalog.server.utils.ServerProperties.Property;
import io.unitycatalog.server.utils.TestUtils;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class SdkManagedVolumeCleanupTest extends BaseServerTest {
  @Override
  protected void setUpProperties() {
    super.setUpProperties();
    serverProperties.setProperty(Property.STORAGE_CLEANUP_POLL_INTERVAL.getKey(), "PT0.01S");
    serverProperties.setProperty(Property.STORAGE_CLEANUP_INITIAL_DELAY.getKey(), "PT0.001S");
  }

  @Test
  void dropCleansOldVolumeWithoutTouchingReplacementOrExternalFiles() throws Exception {
    var client = TestUtils.createApiClient(serverConfig);
    new CatalogsApi(client).createCatalog(new CreateCatalog().name("catalog"));
    new SchemasApi(client)
        .createSchema(
            new CreateSchema()
                .catalogName("catalog")
                .name("schema")
                .storageRoot(testDirectoryRoot.resolve("managed").toUri().toString()));
    var volumes = new VolumesApi(client);
    CreateVolumeRequestContent request =
        new CreateVolumeRequestContent()
            .catalogName("catalog")
            .schemaName("schema")
            .name("managed")
            .volumeType(VolumeType.MANAGED);
    VolumeInfo original = volumes.createVolume(request);
    Path oldDirectory = Path.of(URI.create(original.getStorageLocation()));
    writeMarker(oldDirectory);
    Path externalDirectory = testDirectoryRoot.resolve("external");
    Path externalMarker = writeMarker(externalDirectory);
    VolumeInfo external =
        volumes.createVolume(
            new CreateVolumeRequestContent()
                .catalogName("catalog")
                .schemaName("schema")
                .name("external")
                .volumeType(VolumeType.EXTERNAL)
                .storageLocation(externalDirectory.toUri().toString()));

    volumes.deleteVolume(original.getFullName());
    assertThatThrownBy(() -> volumes.getVolume(original.getFullName()))
        .isInstanceOf(ApiException.class);
    VolumeInfo replacement = volumes.createVolume(request);
    assertThat(replacement.getVolumeId()).isNotEqualTo(original.getVolumeId());
    Path replacementMarker = writeMarker(Path.of(URI.create(replacement.getStorageLocation())));
    volumes.deleteVolume(external.getFullName());

    long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
    while (System.nanoTime() < deadline && (Files.exists(oldDirectory) || hasTask(original))) {
      Thread.sleep(10);
    }
    assertThat(oldDirectory).doesNotExist();
    assertThat(hasTask(original)).isFalse();
    assertThat(hasTask(external)).isFalse();
    assertThat(Files.readString(replacementMarker)).isEqualTo("retained data");
    assertThat(Files.readString(externalMarker)).isEqualTo("retained data");
    assertThat(volumes.getVolume(replacement.getFullName()).getVolumeId())
        .isEqualTo(replacement.getVolumeId());
  }

  private Path writeMarker(Path directory) throws Exception {
    Files.createDirectories(directory.resolve("nested"));
    return Files.writeString(directory.resolve("nested/data.txt"), "retained data");
  }

  private boolean hasTask(VolumeInfo volume) {
    try (var session = hibernateConfigurator.getSessionFactory().openSession()) {
      return session.get(StorageCleanupTaskDAO.class, UUID.fromString(volume.getVolumeId()))
          != null;
    }
  }
}
