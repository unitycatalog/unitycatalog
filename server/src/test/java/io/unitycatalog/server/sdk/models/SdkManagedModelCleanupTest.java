package io.unitycatalog.server.sdk.models;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.CatalogsApi;
import io.unitycatalog.client.api.ModelVersionsApi;
import io.unitycatalog.client.api.RegisteredModelsApi;
import io.unitycatalog.client.api.SchemasApi;
import io.unitycatalog.client.model.CreateCatalog;
import io.unitycatalog.client.model.CreateModelVersion;
import io.unitycatalog.client.model.CreateRegisteredModel;
import io.unitycatalog.client.model.CreateSchema;
import io.unitycatalog.client.model.FinalizeModelVersion;
import io.unitycatalog.client.model.ModelVersionInfo;
import io.unitycatalog.client.model.RegisteredModelInfo;
import io.unitycatalog.server.base.BaseServerTest;
import io.unitycatalog.server.cleanup.StorageCleanupTestSupport;
import io.unitycatalog.server.utils.TestUtils;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class SdkManagedModelCleanupTest extends BaseServerTest {
  private static final Duration CLEANUP_DEADLINE = Duration.ofSeconds(10);

  @Override
  protected void setUpProperties() {
    super.setUpProperties();
    StorageCleanupTestSupport.configureFastCleanup(serverProperties);
  }

  @Test
  void versionAndModelCleanupPreserveLiveVersionsReplacementAndSource() throws Exception {
    var client = TestUtils.createApiClient(serverConfig);
    new CatalogsApi(client).createCatalog(new CreateCatalog().name("catalog"));
    new SchemasApi(client)
        .createSchema(
            new CreateSchema()
                .catalogName("catalog")
                .name("schema")
                .storageRoot(testDirectoryRoot.resolve("managed").toUri().toString()));
    var models = new RegisteredModelsApi(client);
    var versions = new ModelVersionsApi(client);
    CreateRegisteredModel request =
        new CreateRegisteredModel().catalogName("catalog").schemaName("schema").name("model");
    RegisteredModelInfo model = models.createRegisteredModel(request);
    Path source = Files.writeString(testDirectoryRoot.resolve("source.bin"), "source artifacts");
    CreateModelVersion versionRequest =
        new CreateModelVersion()
            .catalogName("catalog")
            .schemaName("schema")
            .modelName("model")
            .source(source.toUri().toString());
    ModelVersionInfo first = versions.createModelVersion(versionRequest);
    Path firstDirectory = Path.of(URI.create(first.getStorageLocation()));
    writeMarker(firstDirectory);
    versions.finalizeModelVersion(
        model.getFullName(),
        first.getVersion(),
        new FinalizeModelVersion().fullName(model.getFullName()).version(first.getVersion()));
    ModelVersionInfo second = versions.createModelVersion(versionRequest);
    Path secondMarker = writeMarker(Path.of(URI.create(second.getStorageLocation())));

    versions.deleteModelVersion(model.getFullName(), first.getVersion());
    assertThatThrownBy(() -> versions.getModelVersion(model.getFullName(), first.getVersion()))
        .isInstanceOf(ApiException.class);
    StorageCleanupTestSupport.awaitCleanup(
        hibernateConfigurator.getSessionFactory(),
        UUID.fromString(first.getId()),
        CLEANUP_DEADLINE,
        firstDirectory);
    assertThat(Files.readString(secondMarker)).isEqualTo("model artifacts");
    assertThat(versions.getModelVersion(model.getFullName(), second.getVersion()).getId())
        .isEqualTo(second.getId());

    // A whole-model deletion also covers versions still awaiting registration.
    ModelVersionInfo third = versions.createModelVersion(versionRequest);
    Path thirdMarker = writeMarker(Path.of(URI.create(third.getStorageLocation())));
    models.deleteRegisteredModel(model.getFullName(), true);
    assertThatThrownBy(() -> models.getRegisteredModel(model.getFullName()))
        .isInstanceOf(ApiException.class);
    RegisteredModelInfo replacement = models.createRegisteredModel(request);
    assertThat(replacement.getId()).isNotEqualTo(model.getId());
    ModelVersionInfo replacementVersion = versions.createModelVersion(versionRequest);
    Path replacementMarker =
        writeMarker(Path.of(URI.create(replacementVersion.getStorageLocation())));

    StorageCleanupTestSupport.awaitCleanup(
        hibernateConfigurator.getSessionFactory(),
        UUID.fromString(model.getId()),
        CLEANUP_DEADLINE,
        Path.of(URI.create(model.getStorageLocation())));
    assertThat(secondMarker).doesNotExist();
    assertThat(thirdMarker).doesNotExist();
    assertThat(Files.readString(replacementMarker)).isEqualTo("model artifacts");
    assertThat(Files.readString(source)).isEqualTo("source artifacts");
    assertThat(
            versions
                .getModelVersion(replacement.getFullName(), replacementVersion.getVersion())
                .getId())
        .isEqualTo(replacementVersion.getId());
  }

  private Path writeMarker(Path directory) throws Exception {
    Files.createDirectories(directory.resolve("nested"));
    return Files.writeString(directory.resolve("nested/model.bin"), "model artifacts");
  }
}
