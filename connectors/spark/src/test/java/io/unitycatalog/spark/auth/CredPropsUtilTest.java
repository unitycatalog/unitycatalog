package io.unitycatalog.spark.auth;

import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.model.AwsCredentials;
import io.unitycatalog.client.model.PathOperation;
import io.unitycatalog.client.model.TableOperation;
import io.unitycatalog.client.model.TemporaryCredentials;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class CredPropsUtilTest {

  private static final String TEST_URI = "http://localhost:8080";
  private static final String TEST_TOKEN = "test-token";
  private static final String TEST_TABLE_ID = "test-table-id";
  private static final String TEST_PATH = "s3://test-bucket/test-path";

  private TemporaryCredentials createAwsCredentials(String serviceEndpoint) {
    AwsCredentials awsCred = new AwsCredentials();
    awsCred.setAccessKeyId("test-access-key");
    awsCred.setSecretAccessKey("test-secret-key");
    awsCred.setSessionToken("test-session-token");

    TemporaryCredentials tempCred = new TemporaryCredentials();
    tempCred.setAwsTempCredentials(awsCred);
    tempCred.setExpirationTime(System.currentTimeMillis() + 3600000L);

    if (serviceEndpoint != null) {
      tempCred.setServiceEndpoint(serviceEndpoint);
    }

    return tempCred;
  }

  @Test
  public void testCreateTableCredPropsWithoutServiceEndpoint() {
    // Test AWS S3 credentials (no serviceEndpoint)
    TemporaryCredentials tempCreds = createAwsCredentials(null);

    Map<String, String> props =
        CredPropsUtil.createTableCredProps(
            true, // renewCredEnabled
            "s3",
            TEST_URI,
            TEST_TOKEN,
            TEST_TABLE_ID,
            TableOperation.READ,
            tempCreds);

    // Verify fs.s3a.endpoint is NOT set for AWS S3
    assertThat(props).doesNotContainKey("fs.s3a.endpoint");

    // Verify path style access is always enabled
    assertThat(props).containsEntry("fs.s3a.path.style.access", "true");

    // Verify standard S3A properties are set
    assertThat(props).containsKey("fs.s3a.impl.disable.cache");
    assertThat(props).containsKey("fs.s3.impl.disable.cache");
  }

  @Test
  public void testCreateTableCredPropsWithServiceEndpoint() {
    // Test MinIO credentials (with serviceEndpoint)
    String minioEndpoint = "https://minio.example.com:9000";
    TemporaryCredentials tempCreds = createAwsCredentials(minioEndpoint);

    Map<String, String> props =
        CredPropsUtil.createTableCredProps(
            true, // renewCredEnabled
            "s3",
            TEST_URI,
            TEST_TOKEN,
            TEST_TABLE_ID,
            TableOperation.READ_WRITE,
            tempCreds);

    // Verify fs.s3a.endpoint IS set for MinIO
    assertThat(props).containsEntry("fs.s3a.endpoint", minioEndpoint);

    // Verify path style access is enabled (required for MinIO)
    assertThat(props).containsEntry("fs.s3a.path.style.access", "true");

    // Verify standard S3A properties are set
    assertThat(props).containsKey("fs.s3a.impl.disable.cache");
    assertThat(props).containsKey("fs.s3.impl.disable.cache");
  }

  @Test
  public void testCreatePathCredPropsWithoutServiceEndpoint() {
    // Test AWS S3 credentials (no serviceEndpoint)
    TemporaryCredentials tempCreds = createAwsCredentials(null);

    Map<String, String> props =
        CredPropsUtil.createPathCredProps(
            true, // renewCredEnabled
            "s3",
            TEST_URI,
            TEST_TOKEN,
            TEST_PATH,
            PathOperation.PATH_READ,
            tempCreds);

    // Verify fs.s3a.endpoint is NOT set for AWS S3
    assertThat(props).doesNotContainKey("fs.s3a.endpoint");

    // Verify path style access is always enabled
    assertThat(props).containsEntry("fs.s3a.path.style.access", "true");
  }

  @Test
  public void testCreatePathCredPropsWithServiceEndpoint() {
    // Test MinIO credentials (with serviceEndpoint)
    String minioEndpoint = "http://minio.local:9000";
    TemporaryCredentials tempCreds = createAwsCredentials(minioEndpoint);

    Map<String, String> props =
        CredPropsUtil.createPathCredProps(
            true, // renewCredEnabled
            "s3",
            TEST_URI,
            TEST_TOKEN,
            TEST_PATH,
            PathOperation.PATH_READ_WRITE,
            tempCreds);

    // Verify fs.s3a.endpoint IS set for MinIO
    assertThat(props).containsEntry("fs.s3a.endpoint", minioEndpoint);

    // Verify path style access is enabled (required for MinIO)
    assertThat(props).containsEntry("fs.s3a.path.style.access", "true");
  }

  @Test
  public void testCreateTableCredPropsWithEmptyServiceEndpoint() {
    // Test with empty string serviceEndpoint (should behave like AWS S3)
    TemporaryCredentials tempCreds = createAwsCredentials("");

    Map<String, String> props =
        CredPropsUtil.createTableCredProps(
            true, // renewCredEnabled
            "s3",
            TEST_URI,
            TEST_TOKEN,
            TEST_TABLE_ID,
            TableOperation.READ,
            tempCreds);

    // Verify fs.s3a.endpoint is NOT set for empty endpoint
    assertThat(props).doesNotContainKey("fs.s3a.endpoint");

    // Verify path style access is still enabled
    assertThat(props).containsEntry("fs.s3a.path.style.access", "true");
  }

  @Test
  public void testFixedCredPropsDoesNotSetEndpoint() {
    // Test fixed (static) credentials don't use serviceEndpoint
    TemporaryCredentials tempCreds = createAwsCredentials("https://minio.example.com:9000");

    Map<String, String> props =
        CredPropsUtil.createTableCredProps(
            false, // renewCredEnabled = false (fixed creds)
            "s3",
            TEST_URI,
            TEST_TOKEN,
            TEST_TABLE_ID,
            TableOperation.READ,
            tempCreds);

    // Fixed credentials use different credential provider, but should still work
    // Note: Fixed creds don't go through s3TempCredPropsBuilder, they use s3FixedCredProps
    assertThat(props).containsKey("fs.s3a.access.key");
    assertThat(props).containsKey("fs.s3a.secret.key");
    assertThat(props).containsEntry("fs.s3a.path.style.access", "true");
  }

  @Test
  public void testMultipleEndpointsInSameSession() {
    // Simulate mixed AWS S3 and MinIO in same Spark session
    // This tests that each credential set can have different endpoints

    // AWS S3 credentials
    TemporaryCredentials awsCreds = createAwsCredentials(null);
    Map<String, String> awsProps =
        CredPropsUtil.createTableCredProps(
            true, "s3", TEST_URI, TEST_TOKEN, "aws-table-id", TableOperation.READ, awsCreds);

    // MinIO credentials
    TemporaryCredentials minioCreds = createAwsCredentials("https://minio.example.com:9000");
    Map<String, String> minioProps =
        CredPropsUtil.createTableCredProps(
            true, "s3", TEST_URI, TEST_TOKEN, "minio-table-id", TableOperation.READ, minioCreds);

    // Verify AWS props don't have endpoint
    assertThat(awsProps).doesNotContainKey("fs.s3a.endpoint");

    // Verify MinIO props have endpoint
    assertThat(minioProps).containsEntry("fs.s3a.endpoint", "https://minio.example.com:9000");

    // Both should have path style access
    assertThat(awsProps).containsEntry("fs.s3a.path.style.access", "true");
    assertThat(minioProps).containsEntry("fs.s3a.path.style.access", "true");
  }

  @Test
  public void testServiceEndpointWithVariousFormats() {
    // Test various valid endpoint URL formats

    String[] validEndpoints = {
      "http://localhost:9000",
      "https://minio.example.com",
      "https://minio.example.com:9000",
      "http://192.168.1.100:9000",
      "https://s3-compatible.service.cloud"
    };

    for (String endpoint : validEndpoints) {
      TemporaryCredentials tempCreds = createAwsCredentials(endpoint);

      Map<String, String> props =
          CredPropsUtil.createTableCredProps(
              true, "s3", TEST_URI, TEST_TOKEN, TEST_TABLE_ID, TableOperation.READ, tempCreds);

      // Verify endpoint is set correctly
      assertThat(props)
          .as("Endpoint format: " + endpoint)
          .containsEntry("fs.s3a.endpoint", endpoint);
    }
  }
}
