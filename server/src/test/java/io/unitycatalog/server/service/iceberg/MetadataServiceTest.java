package io.unitycatalog.server.service.iceberg;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.adobe.testing.s3mock.junit5.S3MockExtension;
import com.amazonaws.util.IOUtils;
import java.util.Objects;
import lombok.SneakyThrows;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;

@ExtendWith(S3MockExtension.class)
public class MetadataServiceTest {
  @RegisterExtension
  public static final S3MockExtension S3_MOCK = S3MockExtension.builder().silent().build();

  public static final String TEST_BUCKET = "test-bucket";
  public static final String TEST_LOCATION = "test-bucket";
  public static final String TEST_SIMPLE_ICEBERG_V1_METADATA_FILE_NAME =
      "simple-v1-iceberg.metadata.json";

  private final FileIOFactory mockFileIOFactory = mock();
  private final S3Client mockS3Client = S3_MOCK.createS3ClientV2();

  private MetadataService metadataService;

  @SneakyThrows
  @BeforeEach
  public void setUp() {
    metadataService = new MetadataService(mockFileIOFactory);
  }

  @SneakyThrows
  @Test
  public void testGetTableMetadataFromS3() {
    when(mockFileIOFactory.getFileIO(any())).thenReturn(new S3FileIO(() -> mockS3Client));
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

    String metadataLocation =
        "s3://"
            + TEST_BUCKET
            + "/"
            + TEST_LOCATION
            + "/"
            + TEST_SIMPLE_ICEBERG_V1_METADATA_FILE_NAME;
    TableMetadata tableMetadata = metadataService.readTableMetadata(metadataLocation);
    assertThat(tableMetadata.uuid()).isEqualTo("11111111-2222-3333-4444-555555555555");
  }

  @SneakyThrows
  @Test
  public void testGetTableMetadataFromLocalFS() {
    when(mockFileIOFactory.getFileIO(any())).thenReturn(new SimpleLocalFileIO());
    String metadataLocation =
        Objects.requireNonNull(this.getClass().getResource("/iceberg.metadata.json"))
            .toURI()
            .toString();
    TableMetadata tableMetadata = metadataService.readTableMetadata(metadataLocation);
    assertThat(tableMetadata.uuid()).isEqualTo("55d4dc69-5b14-4483-bfc8-f33b80f99f99");
  }
}
