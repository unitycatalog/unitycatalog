package io.unitycatalog.cli.volume;

import static io.unitycatalog.server.utils.TestUtils.CATALOG_NAME;
import static io.unitycatalog.server.utils.TestUtils.SCHEMA_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
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
    "s3://test-bucket0/team%20reports,, /team%20reports",
    "s3://test-bucket0/team reports,, /team reports",
    "s3://test-bucket0/team_reports, Q3 report.pdf, /team_reports/Q3 report.pdf",
    "s3://test-bucket0/team_reports, Q3%20report.pdf, /team_reports/Q3%20report.pdf",
    "s3://test-bucket0/team_reports, Q3:report.pdf, /team_reports/Q3:report.pdf",
    "s3://test-bucket0/team_reports, s3://test-bucket0/team_reports/a.txt, /team_reports/a.txt",
    "s3://test-bucket0/team_reports, ., /team_reports",
    "s3://test-bucket0/team%2Freports, ../team/reports/data,",
    "s3://test-bucket0/team_reports, ../team_reports2/data,",
    "s3://test-bucket0/team_reports, /other/x,",
    "file:///tmp/uc-cli-volume-test/my%20vol, Q3 report.pdf, /tmp/uc-cli-volume-test/my vol/Q3"
        + " report.pdf",
    "file:///tmp/uc-cli-volume-test/my%20vol, Q3%20report.pdf, /tmp/uc-cli-volume-test/my vol/Q3"
        + " report.pdf",
    "file:///tmp/uc-cli-volume-test/my%20vol, file:///tmp/uc-cli-volume-test/my%20vol/a.txt,"
        + " /tmp/uc-cli-volume-test/my vol/a.txt",
    "file:///tmp/uc-cli-volume-test/my%20vol, file:/tmp/uc-cli-volume-test/my%20vol/a.txt,"
        + " /tmp/uc-cli-volume-test/my vol/a.txt",
    "file:///tmp/uc-cli-volume-test/my%20vol, %2E%2E/my%20vol2/x,",
    "file:///tmp/uc-cli-volume-test/my%20vol, Q3:report.pdf, /tmp/uc-cli-volume-test/my"
        + " vol/Q3:report.pdf"
  })
  public void testReadVolumePathMatchesRegisteredLiteralLocation(
      String location, String relativePath, String expectedPath) throws Exception {
    VolumeInfo volume = createExternalVolume("literal_path", location);

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

    if (expectedPath == null) {
      assertThatThrownBy(() -> runCli(arguments, fileSystem))
          .isInstanceOf(CliException.class)
          .hasMessageContaining("Resolved URI is outside the base URI path");
      return;
    }
    runCli(arguments, fileSystem);

    ArgumentCaptor<Path> path = ArgumentCaptor.forClass(Path.class);
    verify(fileSystem).exists(path.capture());
    assertThat(path.getValue().toUri().getPath().replaceAll("/$", ""))
        .as("CLI read path must be the registered location joined with --path")
        .isEqualTo(expectedPath);
  }

  @ParameterizedTest
  @CsvSource({
    "s3://test-bucket0/team_reports, Q3 report.pdf, Q3%20report.pdf",
    "file:///tmp/uc-cli-volume-test/listing, Q3%20report.pdf, Q3%2520report.pdf"
  })
  public void testReadVolumeListsNamesThatPathReads(
      String location, String listedSpaceName, String listedPercentName) throws Exception {
    VolumeInfo volume = createExternalVolume("literal_listing", location);
    FileSystem fileSystem = mock(FileSystem.class);
    when(fileSystem.exists(any(Path.class))).thenReturn(true);
    when(fileSystem.getFileStatus(any(Path.class)))
        .thenAnswer(invocation -> new FileStatus(0, true, 1, 0, 0, invocation.getArgument(0)));
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
    try {
      System.setOut(new PrintStream(output, true));
      runCli(arguments, fileSystem);
    } finally {
      System.setOut(stdout);
    }

    assertThat(output.toString().lines())
        .contains(listedSpaceName + " [file]", listedPercentName + " [file]");
  }

  @Test
  public void testWriteVolumeAtLiteralLocation() throws Exception {
    VolumeInfo volume = createExternalVolume("literal_write", "s3://test-bucket0/team reports");
    FileSystem fileSystem = mock(FileSystem.class);
    when(fileSystem.create(any(Path.class), eq(true))).thenReturn(mock(FSDataOutputStream.class));
    List<String> arguments =
        new ArrayList<>(List.of("volume", "write", "--full_name", volume.getFullName()));
    TestUtils.addServerAndAuthParams(arguments, serverConfig);

    runCli(arguments, fileSystem);

    ArgumentCaptor<Path> path = ArgumentCaptor.forClass(Path.class);
    verify(fileSystem).create(path.capture(), eq(true));
    assertThat(path.getValue().getParent().toUri().getPath()).isEqualTo("/team reports");
  }

  private VolumeInfo createExternalVolume(String name, String location) throws Exception {
    return volumeOperations.createVolume(
        new CreateVolumeRequestContent()
            .name(name)
            .catalogName(CATALOG_NAME)
            .schemaName(SCHEMA_NAME)
            .volumeType(VolumeType.EXTERNAL)
            .storageLocation(location));
  }

  private void runCli(List<String> arguments, FileSystem fileSystem) throws Exception {
    try (MockedStatic<DeltaKernelUtils> ignored =
        mockStatic(
            DeltaKernelUtils.class,
            invocation ->
                invocation.getMethod().getName().equals("getFileSystem")
                    ? fileSystem
                    : invocation.callRealMethod())) {
      UnityCatalogCli.main(arguments.toArray(String[]::new));
    }
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
