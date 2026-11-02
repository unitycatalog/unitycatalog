package io.unitycatalog.cli.volume;

import static io.unitycatalog.server.utils.TestUtils.CATALOG_NAME;
import static io.unitycatalog.server.utils.TestUtils.SCHEMA_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.unitycatalog.cli.TestUtils;
import io.unitycatalog.cli.UnityCatalogCli;
import io.unitycatalog.cli.catalog.CliCatalogOperations;
import io.unitycatalog.cli.delta.DeltaKernelUtils;
import io.unitycatalog.cli.schema.CliSchemaOperations;
import io.unitycatalog.cli.utils.CliException;
import io.unitycatalog.client.model.CreateVolumeRequestContent;
import io.unitycatalog.client.model.VolumeInfo;
import io.unitycatalog.client.model.VolumeType;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.catalog.CatalogOperations;
import io.unitycatalog.server.base.schema.SchemaOperations;
import io.unitycatalog.server.base.volume.BaseVolumeCRUDTest;
import io.unitycatalog.server.base.volume.VolumeOperations;
import io.unitycatalog.server.utils.NormalizedURL;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;

public class CliVolumeCRUDTest extends BaseVolumeCRUDTest {
  @Override
  protected void setUpProperties() {
    super.setUpProperties();
    serverProperties.setProperty("s3.bucketPath.0", "s3://test-bucket0");
    serverProperties.setProperty("s3.accessKey.0", "test-access-key");
    serverProperties.setProperty("s3.secretKey.0", "test-secret-key");
    serverProperties.setProperty("s3.sessionToken.0", "test-session-token");
  }

  @ParameterizedTest
  @CsvSource({
    "s3://test-bucket0/team_reports,",
    "s3://test-bucket0/team%20reports,",
    "s3://test-bucket0/team%2Freports,",
    // Outside the volume; it looks inside only when both paths are decoded.
    "s3://test-bucket0/team%2Freports, ../team/reports/data",
    // Local paths stay URI-decoded, like the server's local file access.
    "file:///tmp/uc-cli-volume-test/my%20vol,"
  })
  public void testReadVolumePathMatchesRegisteredLiteralLocation(
      String location, String relativePath) throws Exception {
    VolumeInfo volume =
        volumeOperations.createVolume(
            new CreateVolumeRequestContent()
                .name("literal_path")
                .catalogName(CATALOG_NAME)
                .schemaName(SCHEMA_NAME)
                .volumeType(VolumeType.EXTERNAL)
                .storageLocation(location));
    assertThat(volume.getStorageLocation()).isEqualTo(location);

    FileSystem fileSystem = mock(FileSystem.class);
    when(fileSystem.exists(any(Path.class))).thenReturn(true);
    when(fileSystem.getFileStatus(any(Path.class)))
        .thenReturn(new FileStatus(0, true, 1, 0, 0, new Path(location)));
    when(fileSystem.listStatus(any(Path.class))).thenReturn(new FileStatus[0]);
    List<String> arguments =
        new ArrayList<>(List.of("volume", "read", "--full_name", volume.getFullName()));
    if (relativePath != null) {
      arguments.addAll(List.of("--path", relativePath));
    }
    TestUtils.addServerAndAuthParams(arguments, serverConfig);

    // Keep CLI parsing, both HTTP requests, credential configuration and path conversion real.
    try (MockedStatic<DeltaKernelUtils> ignored =
        mockStatic(
            DeltaKernelUtils.class,
            invocation ->
                invocation.getMethod().getName().equals("getFileSystem")
                    ? fileSystem
                    : invocation.callRealMethod())) {
      if (relativePath != null) {
        assertThatThrownBy(() -> UnityCatalogCli.main(arguments.toArray(String[]::new)))
            .isInstanceOf(CliException.class)
            .hasMessageContaining("outside the base URI path");
        verify(fileSystem, never()).exists(any(Path.class));
        return;
      }
      UnityCatalogCli.main(arguments.toArray(String[]::new));
    }

    ArgumentCaptor<Path> path = ArgumentCaptor.forClass(Path.class);
    verify(fileSystem).exists(path.capture());
    // Compare without a trailing slash: Path(String) drops it.
    assertThat(path.getValue().toUri().getPath().replaceAll("/$", ""))
        .as("CLI read directory must match the registered location's literal credential path")
        .isEqualTo(
            NormalizedURL.from(volume.getStorageLocation()).getStoragePath().replaceAll("/$", ""));
  }

  @Override
  protected SchemaOperations createSchemaOperations(ServerConfig serverConfig) {
    return new CliSchemaOperations(serverConfig);
  }

  @Override
  protected VolumeOperations createVolumeOperations(ServerConfig config) {
    return new CliVolumeOperations(config);
  }

  @Override
  protected CatalogOperations createCatalogOperations(ServerConfig serverConfig) {
    return new CliCatalogOperations(serverConfig);
  }
}
