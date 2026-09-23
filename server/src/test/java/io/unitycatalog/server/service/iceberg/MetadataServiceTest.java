package io.unitycatalog.server.service.iceberg;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.adobe.testing.s3mock.junit5.S3MockExtension;
import com.amazonaws.util.IOUtils;
import io.unitycatalog.server.persist.utils.FileOperations;
import io.unitycatalog.server.persist.utils.SimpleLocalFileIO;
import io.unitycatalog.server.utils.NormalizedURL;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import lombok.SneakyThrows;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;

@ExtendWith(S3MockExtension.class)
public class MetadataServiceTest {
  @RegisterExtension
  public static final S3MockExtension S3_MOCK = S3MockExtension.builder().silent().build();

  public static final String TEST_BUCKET = "test-bucket";
  // Matches the table root declared inside the simple-v1-iceberg fixture ("s3://test-bucket/
  // testLocation"), so the metadata file resolves under the persisted table location.
  public static final String TEST_LOCATION = "testLocation";
  public static final String TEST_SIMPLE_ICEBERG_V1_METADATA_FILE_NAME =
      "simple-v1-iceberg.metadata.json";

  private final FileOperations mockFileOperations = mock();
  private final S3Client mockS3Client = S3_MOCK.createS3ClientV2();

  private MetadataService metadataService;

  @SneakyThrows
  @BeforeEach
  public void setUp() {
    metadataService = new MetadataService(mockFileOperations);
  }

  @SneakyThrows
  @Test
  public void testGetTableMetadataFromS3() {
    when(mockFileOperations.getFileIO(any())).thenReturn(new S3FileIO(() -> mockS3Client));
    mockS3Client.createBucket(builder -> builder.bucket(TEST_BUCKET).build());
    String simpleMetadataJson =
        IOUtils.toString(
            Objects.requireNonNull(
                this.getClass()
                    .getResourceAsStream("/" + TEST_SIMPLE_ICEBERG_V1_METADATA_FILE_NAME)));
    mockS3Client.putObject(
        builder ->
            builder
                .bucket(TEST_BUCKET)
                .key(TEST_LOCATION + "/" + TEST_SIMPLE_ICEBERG_V1_METADATA_FILE_NAME)
                .build(),
        RequestBody.fromString(simpleMetadataJson));

    NormalizedURL tableLocation = NormalizedURL.from("s3://" + TEST_BUCKET + "/" + TEST_LOCATION);
    NormalizedURL metadataLocation =
        NormalizedURL.from(
            "s3://"
                + TEST_BUCKET
                + "/"
                + TEST_LOCATION
                + "/"
                + TEST_SIMPLE_ICEBERG_V1_METADATA_FILE_NAME);
    TableMetadata tableMetadata =
        metadataService.readTableMetadata(metadataLocation, tableLocation);
    assertThat(tableMetadata.uuid()).isEqualTo("11111111-2222-3333-4444-555555555555");
  }

  @SneakyThrows
  @Test
  public void testGetTableMetadataFromLocalFS(@TempDir Path tableRoot) {
    when(mockFileOperations.getFileIO(any())).thenReturn(new SimpleLocalFileIO());
    // Read a real Iceberg metadata fixture from local disk. The fixture's baked table root is
    // rewritten onto a hermetic temp directory, then written under it, so the metadata file
    // resolves inside the persisted table location the two-arg read validates against.
    NormalizedURL tableLocation = NormalizedURL.from(tableRoot.toUri());
    Path metadataFile = tableRoot.resolve("metadata/v1.metadata.json");
    Files.createDirectories(metadataFile.getParent());
    Files.writeString(metadataFile, fixtureWithTableRoot(tableLocation));
    NormalizedURL metadataLocation = NormalizedURL.from(metadataFile.toUri());

    TableMetadata tableMetadata =
        metadataService.readTableMetadata(metadataLocation, tableLocation);
    assertThat(tableMetadata.uuid()).isEqualTo("55d4dc69-5b14-4483-bfc8-f33b80f99f99");
  }

  /**
   * Loads the local Iceberg metadata fixture with its baked table root rewritten to {@code root}.
   */
  @SneakyThrows
  private String fixtureWithTableRoot(NormalizedURL root) {
    try (InputStream fixture =
        Objects.requireNonNull(this.getClass().getResourceAsStream("/iceberg.metadata.json"))) {
      return new String(fixture.readAllBytes(), StandardCharsets.UTF_8)
          .replace("file:/tmp/uniform_iceberg_table", root.toString());
    }
  }

  @SneakyThrows
  @Test
  public void testWriteAndDeleteTableMetadataOnS3() {
    when(mockFileOperations.getFileIO(any())).thenReturn(new S3FileIO(() -> mockS3Client));
    when(mockFileOperations.getFileIO(any(), any())).thenReturn(new S3FileIO(() -> mockS3Client));
    // Dedicated bucket: the S3Mock extension is static and shared across this class's tests.
    String bucket = "metadata-write-test";
    mockS3Client.createBucket(builder -> builder.bucket(bucket).build());

    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
    String tableLocation = "s3://" + bucket + "/write-roundtrip";
    TableMetadata tableMetadata =
        TableMetadata.newTableMetadata(
            schema, PartitionSpec.unpartitioned(), tableLocation, Map.of());
    NormalizedURL persistedTableLocation = NormalizedURL.from(tableLocation);
    NormalizedURL metadataLocation =
        NormalizedURL.from(
            tableLocation + "/metadata/00000-" + UUID.randomUUID() + ".metadata.json");

    // The metadata is written to S3 through the FileIO and reads back identically.
    metadataService.writeTableMetadata(tableMetadata, metadataLocation, persistedTableLocation);
    assertThat(metadataService.readTableMetadata(metadataLocation, persistedTableLocation).uuid())
        .isEqualTo(tableMetadata.uuid());

    // Delete removes the object, so a subsequent read of that location fails.
    metadataService.deleteTableMetadata(metadataLocation, persistedTableLocation);
    assertThatThrownBy(
            () -> metadataService.readTableMetadata(metadataLocation, persistedTableLocation))
        .isInstanceOf(RuntimeException.class);
  }

  @Test
  public void acceptsMetadataLocationsUnderPersistedTableLocation() {
    NormalizedURL tableLocation = NormalizedURL.from("s3://bucket/table");
    TableMetadata metadata =
        TableMetadata.newTableMetadata(
            new Schema(Types.NestedField.required(1, "id", Types.LongType.get())),
            PartitionSpec.unpartitioned(),
            tableLocation.toString(),
            Map.of(TableProperties.WRITE_METADATA_LOCATION, tableLocation + "/metadata"));

    assertThatCode(() -> MetadataService.validateTableMetadataLocation(metadata, tableLocation))
        .doesNotThrowAnyException();
    assertThatCode(
            () -> {
              MetadataService.validateMetadataLocation(
                  NormalizedURL.from(tableLocation + "/metadata/v1.metadata.json"), tableLocation);
            })
        .doesNotThrowAnyException();
  }

  @Test
  public void rejectsClientSuppliedMetadataLocationsOutsidePersistedTableLocation() {
    NormalizedURL tableLocation = NormalizedURL.from("s3://bucket/table");
    TableMetadata metadata =
        TableMetadata.newTableMetadata(
            new Schema(Types.NestedField.required(1, "id", Types.LongType.get())),
            PartitionSpec.unpartitioned(),
            tableLocation.toString(),
            Map.of(TableProperties.WRITE_METADATA_LOCATION, "s3://bucket/other-table/metadata"));

    assertThatThrownBy(() -> MetadataService.validateTableMetadataLocation(metadata, tableLocation))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("must be a subpath");
    assertThatThrownBy(
            () ->
                MetadataService.validateMetadataLocation(
                    NormalizedURL.from("s3://bucket/other-table/v1.metadata.json"), tableLocation))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("must be a subpath");

    TableMetadata metadataWithWrongTableRoot =
        TableMetadata.newTableMetadata(
            new Schema(Types.NestedField.required(1, "id", Types.LongType.get())),
            PartitionSpec.unpartitioned(),
            "s3://bucket/other-table",
            Map.of());
    assertThatThrownBy(
            () ->
                MetadataService.validateTableMetadataLocation(
                    metadataWithWrongTableRoot, tableLocation))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("must match the persisted table location");
  }

  @Test
  public void toIcebergMetadataLocationStripsFileSchemeButKeepsObjectStoreUris() {
    // Local files are handed to Iceberg as filesystem paths (no scheme), so SimpleLocalFileIO's
    // java.nio-based reads/writes resolve them.
    assertThat(
            MetadataService.toIcebergMetadataLocation(
                NormalizedURL.from("file:///tmp/table/metadata/v1.metadata.json")))
        .isEqualTo("/tmp/table/metadata/v1.metadata.json");

    // Object-store locations are passed through verbatim, scheme included.
    String s3Location = "s3://bucket/table/metadata/v1.metadata.json";
    assertThat(MetadataService.toIcebergMetadataLocation(NormalizedURL.from(s3Location)))
        .isEqualTo(s3Location);
  }
}
