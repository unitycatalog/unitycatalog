package io.unitycatalog.spark;

import java.util.Map;
import org.apache.iceberg.aws.s3.S3FileIOAwsClientFactory;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * Iceberg {@link S3FileIOAwsClientFactory} that hands {@code S3FileIO} a {@link MockS3Client}. It
 * is wired via the {@code s3.client-factory-impl} catalog property; Iceberg calls {@link
 * #initialize} with the merged table config -- which carries the credentials UC vends in {@code
 * loadTable} -- so the factory builds the client from the vended {@code s3.*} credentials, and
 * {@link MockS3Client} enforces them. The S3-only factory keeps the AWS surface to S3 (no
 * Glue/KMS/DynamoDB), unlike the general {@code AwsClientFactory}.
 */
public class MockS3ClientFactory implements S3FileIOAwsClientFactory {

  // The keys Iceberg's own S3FileIOProperties reads; here they carry the UC-vended credentials.
  private static final String ACCESS_KEY_ID_PROP = "s3.access-key-id";
  private static final String SECRET_ACCESS_KEY_PROP = "s3.secret-access-key";
  private static final String SESSION_TOKEN_PROP = "s3.session-token";

  private String accessKeyId;
  private String secretKey;
  private String sessionToken;

  @Override
  public void initialize(Map<String, String> properties) {
    this.accessKeyId = properties.get(ACCESS_KEY_ID_PROP);
    this.secretKey = properties.get(SECRET_ACCESS_KEY_PROP);
    this.sessionToken = properties.get(SESSION_TOKEN_PROP);
  }

  @Override
  public S3Client s3() {
    return new MockS3Client(accessKeyId, secretKey, sessionToken);
  }

  @Override
  public S3AsyncClient s3Async() {
    throw new UnsupportedOperationException("s3Async is not used by these tests");
  }
}
