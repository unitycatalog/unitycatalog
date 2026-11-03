package io.unitycatalog.cli.volume;

import static io.unitycatalog.server.utils.TestUtils.CATALOG_NAME;
import static io.unitycatalog.server.utils.TestUtils.SCHEMA_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
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
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;
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
    "s3://test-bucket0/team_reports,, /team_reports",
    "s3://test-bucket0/team%20reports,, /team%20reports",
    "s3://test-bucket0/team%2Freports,, /team%2Freports",
    "s3://test-bucket0/team reports,, /team reports",
    // --path names the object literally, like the stored location.
    "s3://test-bucket0/team_reports, Q3 report.pdf, /team_reports/Q3 report.pdf",
    "s3://test-bucket0/team_reports, Q3%20report.pdf, /team_reports/Q3%20report.pdf",
    // A ':' is part of the name, not a URI scheme.
    "s3://test-bucket0/team_reports, Q3:report.pdf, /team_reports/Q3:report.pdf",
    // A full URI inside the volume is accepted; "://" later in a name is not a URI.
    "s3://test-bucket0/team_reports, s3://test-bucket0/team_reports/a.txt, /team_reports/a.txt",
    "s3://test-bucket0/team_reports, dir/a://b, /team_reports/dir/a:/b",
    "s3://test-bucket0/team_reports, ., /team_reports",
    // Outside the volume; it looks inside only when both paths are decoded.
    "s3://test-bucket0/team%2Freports, ../team/reports/data,",
    // Outside the volume: a sibling whose name starts with the volume name.
    "s3://test-bucket0/team_reports, ../team_reports2/data,",
    // Outside the volume: the same path in another bucket.
    "s3://test-bucket0/team_reports, s3://other-bucket/team_reports/x,",
    "s3://test-bucket0/team_reports, gs://test-bucket0/team_reports/x,",
    "s3://test-bucket0/team_reports, /other/x,",
    // Local paths stay URI-decoded, like the server's local file access.
    "file:///tmp/uc-cli-volume-test/my%20vol,, /tmp/uc-cli-volume-test/my vol",
    "file:///tmp/uc-cli-volume-test/my%20vol, Q3 report.pdf, /tmp/uc-cli-volume-test/my vol/Q3"
        + " report.pdf",
    // On a local volume --path is a URI reference, as in earlier releases.
    "file:///tmp/uc-cli-volume-test/my%20vol, Q3%20report.pdf, /tmp/uc-cli-volume-test/my vol/Q3"
        + " report.pdf",
    "file:///tmp/uc-cli-volume-test/my%20vol, file:///tmp/uc-cli-volume-test/my%20vol/a.txt,"
        + " /tmp/uc-cli-volume-test/my vol/a.txt",
    "file:///tmp/uc-cli-volume-test/my%20vol, ../my%20vol2/x,"
  })
  public void testReadVolumePathMatchesRegisteredLiteralLocation(
      String location, String relativePath, String expectedPath) throws Exception {
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
      if (expectedPath == null) {
        assertThatThrownBy(() -> UnityCatalogCli.main(arguments.toArray(String[]::new)))
            .isInstanceOf(CliException.class)
            .hasMessageContaining("Path is outside the volume location");
        // Rejected before credentials are configured and a FileSystem is opened.
        ignored.verify(() -> DeltaKernelUtils.getHDFSConfiguration(any(), any()), never());
        ignored.verify(() -> DeltaKernelUtils.getFileSystem(any(), any()), never());
        verify(fileSystem, never()).exists(any(Path.class));
        return;
      }
      UnityCatalogCli.main(arguments.toArray(String[]::new));
    }

    ArgumentCaptor<Path> path = ArgumentCaptor.forClass(Path.class);
    verify(fileSystem).exists(path.capture());
    // Compare without a trailing slash: the cloud Path(String) drops it, the local Path(URI) keeps
    // it.
    assertThat(path.getValue().toUri().getPath().replaceAll("/$", ""))
        .as("CLI read path must be the registered location joined with --path, both literal")
        .isEqualTo(expectedPath);
  }

  @Test
  public void testReadVolumeListsLiteralNames() throws Exception {
    VolumeInfo volume =
        volumeOperations.createVolume(
            new CreateVolumeRequestContent()
                .name("literal_listing")
                .catalogName(CATALOG_NAME)
                .schemaName(SCHEMA_NAME)
                .volumeType(VolumeType.EXTERNAL)
                .storageLocation("s3://test-bucket0/team_reports"));
    FileSystem fileSystem = mock(FileSystem.class);
    when(fileSystem.exists(any(Path.class))).thenReturn(true);
    when(fileSystem.getFileStatus(any(Path.class)))
        .thenAnswer(invocation -> new FileStatus(0, true, 1, 0, 0, invocation.getArgument(0)));
    // S3A builds child paths from the object names, as new Path(parent, name).
    when(fileSystem.listStatus(any(Path.class)))
        .thenAnswer(
            invocation ->
                Stream.of("Q3 report.pdf", "Q3%20report.pdf")
                    .map(
                        name ->
                            new FileStatus(
                                1,
                                false,
                                1,
                                0,
                                0,
                                new Path((Path) invocation.getArgument(0), name)))
                    .toArray(FileStatus[]::new));
    List<String> arguments =
        new ArrayList<>(List.of("volume", "read", "--full_name", volume.getFullName()));
    TestUtils.addServerAndAuthParams(arguments, serverConfig);

    PrintStream stdout = System.out;
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    try (MockedStatic<DeltaKernelUtils> ignored =
        mockStatic(
            DeltaKernelUtils.class,
            invocation ->
                invocation.getMethod().getName().equals("getFileSystem")
                    ? fileSystem
                    : invocation.callRealMethod())) {
      System.setOut(new PrintStream(output, true));
      UnityCatalogCli.main(arguments.toArray(String[]::new));
    } finally {
      System.setOut(stdout);
    }

    // The listed names are the ones --path reads.
    assertThat(output.toString()).contains("Q3 report.pdf [file]", "Q3%20report.pdf [file]");
  }

  @Test
  public void testWriteVolumeAtLiteralLocation() throws Exception {
    VolumeInfo volume =
        volumeOperations.createVolume(
            new CreateVolumeRequestContent()
                .name("literal_write")
                .catalogName(CATALOG_NAME)
                .schemaName(SCHEMA_NAME)
                .volumeType(VolumeType.EXTERNAL)
                .storageLocation("s3://test-bucket0/team reports"));
    FileSystem fileSystem = mock(FileSystem.class);
    when(fileSystem.create(any(Path.class), eq(true))).thenReturn(mock(FSDataOutputStream.class));
    List<String> arguments =
        new ArrayList<>(List.of("volume", "write", "--full_name", volume.getFullName()));
    TestUtils.addServerAndAuthParams(arguments, serverConfig);

    try (MockedStatic<DeltaKernelUtils> ignored =
        mockStatic(
            DeltaKernelUtils.class,
            invocation ->
                invocation.getMethod().getName().equals("getFileSystem")
                    ? fileSystem
                    : invocation.callRealMethod())) {
      UnityCatalogCli.main(arguments.toArray(String[]::new));
    }

    ArgumentCaptor<Path> path = ArgumentCaptor.forClass(Path.class);
    verify(fileSystem).create(path.capture(), eq(true));
    assertThat(path.getValue().getParent().toUri().getPath()).isEqualTo("/team reports");
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
