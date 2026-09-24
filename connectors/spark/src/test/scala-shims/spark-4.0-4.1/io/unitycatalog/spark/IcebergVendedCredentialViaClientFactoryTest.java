package io.unitycatalog.spark;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.io.SeekableInputStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Proves Iceberg's {@code S3FileIO} picks up {@link MockS3ClientFactory} via the {@code
 * s3.client-factory-impl} property and hands it the vended {@code s3.*} credentials, then does all
 * object IO through the resulting {@link MockS3Client} over local files. This is the S3FileIO
 * (default for {@code s3://}) path -- not {@code HadoopFileIO} -- so the client factory hook is
 * actually consulted. No SparkSession or UC server is started: the config handed to {@code
 * S3FileIO} here is exactly what a table's {@code loadTable} config would carry.
 */
public class IcebergVendedCredentialViaClientFactoryTest {

  @TempDir Path bucketRoot;

  @Test
  public void s3FileIoUsesTheFactoryAndVendedCredentials() throws Exception {
    S3FileIO io = fileIoFrom(FakeS3.SESSION_TOKEN);

    // An s3:// location under the temp dir; its key is the absolute local path (see FakeS3).
    String location =
        "s3://" + FakeS3.BUCKET + bucketRoot.toAbsolutePath() + "/metadata/00000-abc.metadata.json";
    byte[] payload = "iceberg-metadata-bytes".getBytes(UTF_8);

    // Write: S3FileIO -> MockS3ClientFactory.s3() -> MockS3Client.putObject -> local file.
    try (PositionOutputStream out = io.newOutputFile(location).create()) {
      out.write(payload);
    }
    Path onDisk = bucketRoot.toAbsolutePath().resolve("metadata/00000-abc.metadata.json");
    assertThat(Files.exists(onDisk)).isTrue();
    assertThat(Files.readAllBytes(onDisk)).isEqualTo(payload);

    // Read back through Iceberg S3FileIO -> MockS3Client.getObject -> local.
    try (SeekableInputStream in = io.newInputFile(location).newStream()) {
      assertThat(in.readAllBytes()).isEqualTo(payload);
    }
  }

  @Test
  public void staleOrMissingVendedCredentialIsRejected() {
    S3FileIO io = fileIoFrom("not-the-vended-token");
    String location = "s3://" + FakeS3.BUCKET + bucketRoot.toAbsolutePath() + "/data/f.parquet";
    assertThatThrownBy(
            () -> {
              try (PositionOutputStream out = io.newOutputFile(location).create()) {
                out.write(new byte[] {1, 2, 3});
              }
            })
        .hasStackTraceContaining("s3.session-token");
  }

  /** Builds an S3FileIO wired to the mock factory via {@code s3.client-factory-impl}. */
  private S3FileIO fileIoFrom(String sessionToken) {
    Map<String, String> props = new HashMap<>();
    props.put("s3.client-factory-impl", MockS3ClientFactory.class.getName());
    props.put("s3.access-key-id", FakeS3.ACCESS_KEY_ID);
    props.put("s3.secret-access-key", FakeS3.SECRET_ACCESS_KEY);
    props.put("s3.session-token", sessionToken);
    S3FileIO io = new S3FileIO();
    io.initialize(props);
    return io;
  }
}
