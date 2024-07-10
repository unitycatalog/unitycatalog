package io.unitycatalog.server.service.iceberg;

import com.adobe.testing.s3mock.junit5.S3MockExtension;
import com.amazonaws.util.IOUtils;
import lombok.SneakyThrows;
import org.apache.iceberg.TableMetadata;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;

import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

@ExtendWith(S3MockExtension.class)
public class MetadataServiceTest {
  @RegisterExtension
  public static final S3MockExtension S3_MOCK = S3MockExtension.builder().silent().build();

  public static final String TEST_BUCKET = "test-bucket";
  public static final String TEST_LOCATION = "testLocation";

  private MetadataService metadataService;
  private S3Client mockS3Client;

  @SneakyThrows
  @BeforeEach
  public void setUp() {
    mockS3Client = S3_MOCK.createS3ClientV2();
    FileIOFactory fileIOFactory = spy(new FileIOFactory());
    doReturn(mockS3Client).when(fileIOFactory).getS3Client(any(), any());
    metadataService = new MetadataService(fileIOFactory);
  }

  @SneakyThrows
  @Test
  public void testGetTableMetadataFromS3() {
    mockS3Client.createBucket(builder -> builder.bucket(TEST_BUCKET).build());
    String simpleMetadataJson = IOUtils.toString(
      Objects.requireNonNull(this.getClass().getResourceAsStream("/simple-v1-iceberg.metadata.json")));
    mockS3Client.putObject(
      builder -> builder.bucket(TEST_BUCKET).key(TEST_LOCATION + "/simple-v1-iceberg.metadata.json").build(),
      RequestBody.fromString(simpleMetadataJson));

    String metadataLocation = "s3://" + TEST_BUCKET + "/" + TEST_LOCATION + "/simple-v1-iceberg.metadata.json";
    TableMetadata tableMetadata = metadataService.readTableMetadata(metadataLocation);
    assertThat(tableMetadata.uuid()).isEqualTo("11111111-2222-3333-4444-555555555555");
  }

  @SneakyThrows
  @Test
  public void testGetTableMetadataFromLocalFS() {
    String metadataLocation = Objects.requireNonNull(
      this.getClass().getResource("/metadata.json")).toURI().toString();
    TableMetadata tableMetadata = metadataService.readTableMetadata(metadataLocation);
    assertThat(tableMetadata.uuid()).isEqualTo("55d4dc69-5b14-4483-bfc8-f33b80f99f99");
  }

}
