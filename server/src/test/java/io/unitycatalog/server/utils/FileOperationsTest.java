package io.unitycatalog.server.utils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.model.AwsCredentials;
import io.unitycatalog.server.model.AzureUserDelegationSAS;
import io.unitycatalog.server.model.GcpOauthToken;
import io.unitycatalog.server.model.TemporaryCredentials;
import io.unitycatalog.server.persist.utils.ExternalLocationUtils;
import io.unitycatalog.server.persist.utils.FileOperations;
import io.unitycatalog.server.persist.utils.SimpleLocalFileIO;
import io.unitycatalog.server.service.credential.StorageCredentialVendor;
import java.io.FileNotFoundException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import lombok.SneakyThrows;
import org.apache.iceberg.aws.AwsClientProperties;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.azure.AzureProperties;
import org.apache.iceberg.gcp.GCPProperties;
import org.apache.iceberg.io.BulkDeletionFailureException;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.ResolvingFileIO;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class FileOperationsTest {

  @TempDir Path rootBase;

  @SneakyThrows
  @Test
  public void testModelDirectory() {
    // Test model directory creation with a given storage root
    NormalizedURL parentStorageLocation = NormalizedURL.from(rootBase.toString());
    UUID modelId = UUID.randomUUID();
    NormalizedURL modelPathUri =
        ExternalLocationUtils.getManagedLocationForModel(parentStorageLocation, modelId);
    assertThat(modelPathUri.toString()).isEqualTo(parentStorageLocation + "/models/" + modelId);

    FileOperations.createStorageLocationDir(modelPathUri);
    FileOperations.deleteDirectory(modelPathUri);
  }

  @SneakyThrows
  @Test
  public void testDeleteDirectoryRemovesNestedFilesAndDirs() {
    // Populate a directory with nested files and subdirectories, then verify deleteDirectory
    // removes the entire tree (files and the emptied directories, bottom-up).
    Path dir = rootBase.resolve("table-" + UUID.randomUUID());
    Path nested = dir.resolve("sub/deeper");
    Files.createDirectories(nested);
    Files.writeString(dir.resolve("_metadata.json"), "{}");
    Files.writeString(dir.resolve("sub/part-0.parquet"), "data");
    Files.writeString(nested.resolve("part-1.parquet"), "data");

    FileOperations.deleteDirectory(NormalizedURL.from(dir.toString()));

    // The whole tree must be gone: nested files, the intermediate directories, and the root.
    assertThat(Files.exists(nested.resolve("part-1.parquet"))).isFalse();
    assertThat(Files.exists(dir.resolve("sub/part-0.parquet"))).isFalse();
    assertThat(Files.exists(nested)).isFalse();
    assertThat(Files.exists(dir.resolve("sub"))).isFalse();
    assertThat(Files.exists(dir)).isFalse();
  }

  @SneakyThrows
  @Test
  public void testDeletePrefixOnMissingDirectoryThrows() {
    // deletePrefix must signal a missing prefix (it yields the prefix dir itself when present,
    // so an empty walk means the directory does not exist).
    NormalizedURL missing =
        NormalizedURL.from(rootBase.resolve("does-not-exist-" + UUID.randomUUID()).toString());
    assertThatThrownBy(() -> SimpleLocalFileIO.deleteDirectory(missing.toString()))
        .isInstanceOf(UncheckedIOException.class)
        .hasCauseInstanceOf(FileNotFoundException.class);
  }

  @SneakyThrows
  @Test
  public void testListPrefixReturnsRegularFilesOnly() {
    // listPrefix must recurse into subdirectories, return every regular file, and exclude the
    // directories themselves (per the Iceberg FileInfo listing contract).
    Path dir = rootBase.resolve("table-" + UUID.randomUUID());
    Files.createDirectories(dir.resolve("sub"));
    Files.writeString(dir.resolve("a.json"), "a");
    Files.writeString(dir.resolve("sub/b.parquet"), "bb");

    List<String> listed;
    try (CloseableIterable<FileInfo> files =
        new SimpleLocalFileIO().listPrefix(dir.toUri().toString())) {
      listed =
          StreamSupport.stream(files.spliterator(), false)
              .map(FileInfo::location)
              .collect(Collectors.toList());
    }

    // Both files are present; neither the prefix dir nor "sub" appears in the listing.
    assertThat(listed)
        .containsExactlyInAnyOrder(
            dir.resolve("a.json").toUri().toString(),
            dir.resolve("sub/b.parquet").toUri().toString());
  }

  @SneakyThrows
  @Test
  public void testListPrefixReportsSize() {
    Path dir = rootBase.resolve("table-" + UUID.randomUUID());
    Files.createDirectories(dir);
    Files.writeString(dir.resolve("a.json"), "hello");

    try (CloseableIterable<FileInfo> files =
        new SimpleLocalFileIO().listPrefix(dir.toUri().toString())) {
      List<FileInfo> infos =
          StreamSupport.stream(files.spliterator(), false).collect(Collectors.toList());
      assertThat(infos).hasSize(1);
      assertThat(infos.get(0).size()).isEqualTo(5L);
    }
  }

  @SneakyThrows
  @Test
  public void testDeleteFilesDeletesListedFilesOnly() {
    Path dir = rootBase.resolve("table-" + UUID.randomUUID());
    Files.createDirectories(dir);
    Path a = dir.resolve("a.json");
    Path b = dir.resolve("b.json");
    Files.writeString(a, "a");
    Files.writeString(b, "b");

    new SimpleLocalFileIO().deleteFiles(List.of(a.toUri().toString()));

    assertThat(Files.exists(a)).isFalse();
    assertThat(Files.exists(b)).isTrue();
  }

  @SneakyThrows
  @Test
  public void testDeleteFilesThrowsBulkFailureWhenAnyFails() {
    Path dir = rootBase.resolve("table-" + UUID.randomUUID());
    Files.createDirectories(dir);
    Path existing = dir.resolve("a.json");
    Files.writeString(existing, "a");
    String missing = dir.resolve("missing.json").toUri().toString();

    // One deletable + one nonexistent path: the batch must surface a BulkDeletionFailureException,
    // and the deletable file is still removed.
    assertThatThrownBy(
            () ->
                new SimpleLocalFileIO().deleteFiles(List.of(existing.toUri().toString(), missing)))
        .isInstanceOf(BulkDeletionFailureException.class);
    assertThat(Files.exists(existing)).isFalse();
  }

  @Test
  public void testGetFileIOConfigLocalPathSkipsVending() {
    // Local paths must short-circuit to an empty config WITHOUT vending credentials.
    StorageCredentialVendor vendor = mock(StorageCredentialVendor.class);
    FileOperations fileOps = new FileOperations(vendor, new ServerProperties(new Properties()));

    Map<String, String> config =
        fileOps.getFileIOConfig(NormalizedURL.from("file:///tmp/some/table"));

    assertThat(config).isEmpty();
    verify(vendor, never()).vendCredential(any(), any());
  }

  @Test
  public void testGetFileIOConfigS3() {
    // Region for the bucket comes from server properties; credentials from the vendor.
    Properties props = new Properties();
    props.setProperty("s3.bucketPath.0", "s3://my-bucket");
    props.setProperty("s3.region.0", "us-west-2");
    props.setProperty("s3.awsRoleArn.0", "arn:aws:iam::123456789012:role/test");
    StorageCredentialVendor vendor = mock(StorageCredentialVendor.class);
    when(vendor.vendCredential(any(), any()))
        .thenReturn(
            new TemporaryCredentials()
                .awsTempCredentials(
                    new AwsCredentials()
                        .accessKeyId("AKIA")
                        .secretAccessKey("secret")
                        .sessionToken("token")));
    FileOperations fileOps = new FileOperations(vendor, new ServerProperties(props));

    Map<String, String> config =
        fileOps.getFileIOConfig(NormalizedURL.from("s3://my-bucket/table"));

    assertThat(config)
        .containsEntry(S3FileIOProperties.ACCESS_KEY_ID, "AKIA")
        .containsEntry(S3FileIOProperties.SECRET_ACCESS_KEY, "secret")
        .containsEntry(S3FileIOProperties.SESSION_TOKEN, "token")
        .containsEntry(AwsClientProperties.CLIENT_REGION, "us-west-2");
  }

  @Test
  public void testGetFileIOConfigGcsIncludesExpiry() {
    StorageCredentialVendor vendor = mock(StorageCredentialVendor.class);
    when(vendor.vendCredential(any(), any()))
        .thenReturn(
            new TemporaryCredentials()
                .gcpOauthToken(new GcpOauthToken().oauthToken("gcs-token"))
                .expirationTime(12345L));
    FileOperations fileOps = new FileOperations(vendor, new ServerProperties(new Properties()));

    Map<String, String> config =
        fileOps.getFileIOConfig(NormalizedURL.from("gs://my-bucket/table"));

    assertThat(config)
        .containsEntry(GCPProperties.GCS_OAUTH2_TOKEN, "gcs-token")
        .containsEntry(GCPProperties.GCS_OAUTH2_TOKEN_EXPIRES_AT, "12345");
  }

  @Test
  public void testGetFileIOConfigGcsOmitsExpiryWhenNull() {
    StorageCredentialVendor vendor = mock(StorageCredentialVendor.class);
    when(vendor.vendCredential(any(), any()))
        .thenReturn(
            new TemporaryCredentials().gcpOauthToken(new GcpOauthToken().oauthToken("gcs-token")));
    FileOperations fileOps = new FileOperations(vendor, new ServerProperties(new Properties()));

    Map<String, String> config =
        fileOps.getFileIOConfig(NormalizedURL.from("gs://my-bucket/table"));

    assertThat(config).containsEntry(GCPProperties.GCS_OAUTH2_TOKEN, "gcs-token");
    assertThat(config).doesNotContainKey(GCPProperties.GCS_OAUTH2_TOKEN_EXPIRES_AT);
  }

  @Test
  public void testGetFileIOConfigAdls() {
    StorageCredentialVendor vendor = mock(StorageCredentialVendor.class);
    when(vendor.vendCredential(any(), any()))
        .thenReturn(
            new TemporaryCredentials()
                .azureUserDelegationSas(new AzureUserDelegationSAS().sasToken("sas-token")));
    FileOperations fileOps = new FileOperations(vendor, new ServerProperties(new Properties()));

    Map<String, String> config =
        fileOps.getFileIOConfig(
            NormalizedURL.from("abfs://container@myaccount.dfs.core.windows.net/table"));

    assertThat(config)
        .containsEntry(
            AzureProperties.ADLS_SAS_TOKEN_PREFIX + "myaccount.dfs.core.windows.net", "sas-token");
  }

  @Test
  public void testGetFileIOConfigCloudWithUnrecognizedCredentialThrows() {
    // A cloud scheme that vends no recognized credential type must fail loudly rather than
    // silently returning a credential-less config.
    StorageCredentialVendor vendor = mock(StorageCredentialVendor.class);
    when(vendor.vendCredential(any(), any())).thenReturn(new TemporaryCredentials());
    FileOperations fileOps = new FileOperations(vendor, new ServerProperties(new Properties()));

    assertThatThrownBy(() -> fileOps.getFileIOConfig(NormalizedURL.from("gs://my-bucket/table")))
        .isInstanceOf(BaseException.class)
        .hasMessageContaining("No recognized storage credential");
  }

  @Test
  public void testGetFileIOConfigS3WithoutConfiguredRegionThrows() {
    // No region configured for the bucket: guard with a clear error instead of an opaque NPE.
    StorageCredentialVendor vendor = mock(StorageCredentialVendor.class);
    when(vendor.vendCredential(any(), any()))
        .thenReturn(
            new TemporaryCredentials()
                .awsTempCredentials(
                    new AwsCredentials()
                        .accessKeyId("AKIA")
                        .secretAccessKey("secret")
                        .sessionToken("token")));
    FileOperations fileOps = new FileOperations(vendor, new ServerProperties(new Properties()));

    assertThatThrownBy(() -> fileOps.getFileIOConfig(NormalizedURL.from("s3://my-bucket/table")))
        .isInstanceOf(BaseException.class)
        .hasMessageContaining("No S3 region configured");
  }

  @Test
  public void testGetFileIOForLocalPathReturnsSimpleLocalFileIO() {
    // Local paths must not be vended and must resolve to SimpleLocalFileIO (no ResolvingFileIO,
    // which would require hadoop-client-runtime for the file:// scheme).
    StorageCredentialVendor vendor = mock(StorageCredentialVendor.class);
    FileOperations fileOps = new FileOperations(vendor, new ServerProperties(new Properties()));

    try (FileIO fileIO = fileOps.getFileIO(NormalizedURL.from("file:///tmp/some/table"))) {
      assertThat(fileIO).isInstanceOf(SimpleLocalFileIO.class);
    }
    verify(vendor, never()).vendCredential(any(), any());
  }

  @SneakyThrows
  @Test
  public void testGetFileIOForLocalPathReadsThroughReturnedFileIO() {
    // End-to-end: getFileIO returns a working local FileIO that can read a real file.
    Path file = rootBase.resolve("data-" + UUID.randomUUID() + ".txt");
    Files.writeString(file, "hello world");

    StorageCredentialVendor vendor = mock(StorageCredentialVendor.class);
    FileOperations fileOps = new FileOperations(vendor, new ServerProperties(new Properties()));

    try (FileIO fileIO = fileOps.getFileIO(NormalizedURL.from(file.toUri().toString()))) {
      InputFile input = fileIO.newInputFile(file.toUri().toString());
      assertThat(input.getLength()).isEqualTo("hello world".length());
    }
  }

  @Test
  public void testGetFileIOForCloudPathReturnsResolvingFileIO() {
    // Cloud paths route through a credential-vended ResolvingFileIO.
    Properties props = new Properties();
    props.setProperty("s3.bucketPath.0", "s3://my-bucket");
    props.setProperty("s3.region.0", "us-west-2");
    props.setProperty("s3.awsRoleArn.0", "arn:aws:iam::123456789012:role/test");
    StorageCredentialVendor vendor = mock(StorageCredentialVendor.class);
    when(vendor.vendCredential(any(), any()))
        .thenReturn(
            new TemporaryCredentials()
                .awsTempCredentials(
                    new AwsCredentials()
                        .accessKeyId("AKIA")
                        .secretAccessKey("secret")
                        .sessionToken("token")));
    FileOperations fileOps = new FileOperations(vendor, new ServerProperties(props));

    try (FileIO fileIO = fileOps.getFileIO(NormalizedURL.from("s3://my-bucket/table"))) {
      assertThat(fileIO).isInstanceOf(ResolvingFileIO.class);
    }
  }
}
