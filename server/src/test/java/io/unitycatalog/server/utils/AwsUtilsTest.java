package io.unitycatalog.server.utils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

public class AwsUtilsTest {

  private S3Client mockS3Client;

  @BeforeEach
  public void setUp() {
    mockS3Client = mock(S3Client.class);
  }

  // Tests for getAwsCredentialsProvider(String, String, String)
  @Test
  public void testGetAwsCredentialsProviderWithValidCredentials() {
    String accessKeyId = "test-access-key";
    String secretAccessKey = "test-secret-key";
    String sessionToken = "test-session-token";

    AwsCredentialsProvider provider =
        AwsUtils.getAwsCredentialsProvider(accessKeyId, secretAccessKey, sessionToken);

    assertThat(provider).isNotNull();
    assertThat(provider.resolveCredentials()).isNotNull();
    assertThat(provider.resolveCredentials().accessKeyId()).isEqualTo(accessKeyId);
    assertThat(provider.resolveCredentials().secretAccessKey()).isEqualTo(secretAccessKey);
  }

  @Test
  public void testGetAwsCredentialsProviderWithEmptySessionToken() {
    String accessKeyId = "test-access-key";
    String secretAccessKey = "test-secret-key";
    String sessionToken = "";

    AwsCredentialsProvider provider =
        AwsUtils.getAwsCredentialsProvider(accessKeyId, secretAccessKey, sessionToken);

    assertThat(provider).isNotNull();
    assertThat(provider.resolveCredentials()).isNotNull();
    assertThat(provider.resolveCredentials().accessKeyId()).isEqualTo(accessKeyId);
  }

  @Test
  public void testGetAwsCredentialsProviderWithMultipleCredentials() {
    // Test that multiple providers can be created independently
    AwsCredentialsProvider provider1 =
        AwsUtils.getAwsCredentialsProvider("key1", "secret1", "token1");
    AwsCredentialsProvider provider2 =
        AwsUtils.getAwsCredentialsProvider("key2", "secret2", "token2");

    assertThat(provider1).isNotNull();
    assertThat(provider2).isNotNull();
    assertThat(provider1.resolveCredentials().accessKeyId()).isEqualTo("key1");
    assertThat(provider2.resolveCredentials().accessKeyId()).isEqualTo("key2");
  }

  // Tests for getAwsCredentialsProvider(Credentials)
  @Test
  public void testGetAwsCredentialsProviderWithCredentialsObject() {
    software.amazon.awssdk.services.sts.model.Credentials awsCredentials =
        software.amazon.awssdk.services.sts.model.Credentials.builder()
            .accessKeyId("test-access-key")
            .secretAccessKey("test-secret-key")
            .sessionToken("test-session-token")
            .build();

    AwsCredentialsProvider provider = AwsUtils.getAwsCredentialsProvider(awsCredentials);

    assertThat(provider).isNotNull();
    assertThat(provider.resolveCredentials()).isNotNull();
    assertThat(provider.resolveCredentials().accessKeyId()).isEqualTo("test-access-key");
    assertThat(provider.resolveCredentials().secretAccessKey()).isEqualTo("test-secret-key");
  }

  // Tests for getS3Client
  @Test
  public void testGetS3ClientWithValidParameters() {
    AwsCredentialsProvider credentialsProvider =
        AwsUtils.getAwsCredentialsProvider("key", "secret", "token");
    String region = "us-east-1";
    String endpointUrl = null;

    S3Client client = AwsUtils.getS3Client(credentialsProvider, region, endpointUrl);

    assertThat(client).isNotNull();
    client.close();
  }

  @Test
  public void testGetS3ClientWithCustomEndpointUrl() {
    AwsCredentialsProvider credentialsProvider =
        AwsUtils.getAwsCredentialsProvider("key", "secret", "token");
    String region = "us-west-2";
    String endpointUrl = "http://localhost:9000";

    S3Client client = AwsUtils.getS3Client(credentialsProvider, region, endpointUrl);
    assertThat(client).isNotNull();
    client.close();
  }

  @Test
  public void testGetS3ClientWithEmptyEndpointUrl() {
    AwsCredentialsProvider credentialsProvider =
        AwsUtils.getAwsCredentialsProvider("key", "secret", "token");
    String region = "eu-west-1";
    String endpointUrl = "";

    S3Client client = AwsUtils.getS3Client(credentialsProvider, region, endpointUrl);

    assertThat(client).isNotNull();
    client.close();
  }

  @Test
  public void testGetS3ClientWithDifferentRegions() {
    AwsCredentialsProvider credentialsProvider =
        AwsUtils.getAwsCredentialsProvider("key", "secret", "token");

    String[] regions = {"us-east-1", "us-west-2", "eu-west-1", "ap-northeast-1"};
    for (String region : regions) {
      S3Client client = AwsUtils.getS3Client(credentialsProvider, region, null);
      assertThat(client).isNotNull();
      client.close();
    }
  }

  // Tests for doesObjectExist
  @Test
  public void testDoesObjectExistWhenObjectExists() {
    when(mockS3Client.headObject(any(HeadObjectRequest.class))).thenReturn(null);

    boolean exists = AwsUtils.doesObjectExist(mockS3Client, "test-bucket", "test-key");

    assertThat(exists).isTrue();
    verify(mockS3Client).headObject(any(HeadObjectRequest.class));
  }

  @Test
  public void testDoesObjectExistWhenObjectDoesNotExist() {
    when(mockS3Client.headObject(any(HeadObjectRequest.class)))
        .thenThrow(NoSuchKeyException.builder().build());

    boolean exists = AwsUtils.doesObjectExist(mockS3Client, "test-bucket", "test-key");

    assertThat(exists).isFalse();
    verify(mockS3Client).headObject(any(HeadObjectRequest.class));
  }

  @Test
  public void testDoesObjectExistWithEmptyBucketName() {
    assertThatThrownBy(() -> AwsUtils.doesObjectExist(mockS3Client, "", "test-key"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("The bucket name must not be null or an empty string");
  }

  @Test
  public void testDoesObjectExistWithNullBucketName() {
    assertThatThrownBy(() -> AwsUtils.doesObjectExist(mockS3Client, null, "test-key"))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("The bucket name must not be null or an empty string");
  }

  @Test
  public void testDoesObjectExistWithValidBucketAndKey() {
    when(mockS3Client.headObject(any(HeadObjectRequest.class))).thenReturn(null);

    boolean exists = AwsUtils.doesObjectExist(mockS3Client, "my-bucket", "path/to/object");

    assertThat(exists).isTrue();

    ArgumentCaptor<HeadObjectRequest> captor = ArgumentCaptor.forClass(HeadObjectRequest.class);
    verify(mockS3Client).headObject(captor.capture());

    HeadObjectRequest request = captor.getValue();
    assertThat(request.bucket()).isEqualTo("my-bucket");
    assertThat(request.key()).isEqualTo("path/to/object");
  }

  @Test
  public void testDoesObjectExistWithKeyContainingSpecialCharacters() {
    when(mockS3Client.headObject(any(HeadObjectRequest.class))).thenReturn(null);

    String keyWithSpecialChars = "path/to/object-with_special.chars(123).txt";
    boolean exists = AwsUtils.doesObjectExist(mockS3Client, "test-bucket", keyWithSpecialChars);

    assertThat(exists).isTrue();

    ArgumentCaptor<HeadObjectRequest> captor = ArgumentCaptor.forClass(HeadObjectRequest.class);
    verify(mockS3Client).headObject(captor.capture());

    assertThat(captor.getValue().key()).isEqualTo(keyWithSpecialChars);
  }

  @Test
  public void testDoesObjectExistMultipleCallsWithDifferentKeys() {
    when(mockS3Client.headObject(any(HeadObjectRequest.class)))
        .thenReturn(null)
        .thenThrow(NoSuchKeyException.builder().build())
        .thenReturn(null);

    boolean exists1 = AwsUtils.doesObjectExist(mockS3Client, "bucket", "key1");
    boolean exists2 = AwsUtils.doesObjectExist(mockS3Client, "bucket", "key2");
    boolean exists3 = AwsUtils.doesObjectExist(mockS3Client, "bucket", "key3");

    assertThat(exists1).isTrue();
    assertThat(exists2).isFalse();
    assertThat(exists3).isTrue();
  }
}
