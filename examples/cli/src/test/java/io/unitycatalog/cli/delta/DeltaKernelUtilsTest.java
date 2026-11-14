package io.unitycatalog.cli.delta;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.client.model.AwsCredentials;
import io.unitycatalog.client.model.TemporaryCredentials;
import java.net.URI;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

public class DeltaKernelUtilsTest {
  private static final String COMPAT_TABLE = "s3://bucket/t";

  @Test
  public void testS3CompatibleConfigurationUsesTheVendedEndpoint() {
    String endpoint = "http://storage.example.com:9000";
    Configuration withKeys =
        DeltaKernelUtils.getHDFSConfiguration(
            URI.create(COMPAT_TABLE),
            new TemporaryCredentials()
                .awsTempCredentials(
                    new AwsCredentials()
                        .accessKeyId("key-id-1")
                        .secretAccessKey("key-value-1")
                        .s3Endpoint(endpoint)
                        .s3Region("eu-test-1")));
    // At the table's bucket scope, which a bucket option in core-site.xml cannot override.
    assertThat(withKeys.get("fs.s3a.bucket.bucket.endpoint")).isEqualTo(endpoint);
    assertThat(withKeys.get("fs.s3a.bucket.bucket.endpoint.region")).isEqualTo("eu-test-1");
    assertThat(withKeys.get("fs.s3a.bucket.bucket.path.style.access")).isEqualTo("true");
    assertThat(withKeys.get("fs.s3a.bucket.bucket.session.token")).isEmpty();
    assertThat(withKeys.get("fs.s3a.bucket.bucket.access.key")).isEqualTo("key-id-1");
    assertThat(withKeys.get("fs.s3a.bucket.bucket.aws.credentials.provider"))
        .isEqualTo("org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
    assertThat(withKeys.get("fs.s3a.access.key")).isEqualTo("key-id-1");
    // Empty, so that a session token already configured is not paired with the key pair.
    assertThat(withKeys.get("fs.s3a.session.token")).isEmpty();

    // A credential that does not vend keys sends only the endpoint.
    Configuration endpointOnly =
        DeltaKernelUtils.getHDFSConfiguration(
            URI.create(COMPAT_TABLE),
            new TemporaryCredentials()
                .awsTempCredentials(new AwsCredentials().s3Endpoint(endpoint)));
    assertThat(endpointOnly.get("fs.s3a.bucket.bucket.endpoint")).isEqualTo(endpoint);
    assertThat(endpointOnly.get("fs.s3a.bucket.bucket.session.token")).isNull();
    assertThat(endpointOnly.get("fs.s3a.bucket.bucket.access.key")).isNull();
    assertThat(endpointOnly.get("fs.s3a.access.key")).isNull();
  }

  @Test
  public void testAwsConfigurationIsUnchanged() {
    Configuration aws =
        DeltaKernelUtils.getHDFSConfiguration(
            URI.create(COMPAT_TABLE),
            new TemporaryCredentials()
                .awsTempCredentials(
                    new AwsCredentials()
                        .accessKeyId("key-id-1")
                        .secretAccessKey("key-value-1")
                        .sessionToken("session-1")));
    assertThat(aws.get("fs.s3a.session.token")).isEqualTo("session-1");
    assertThat(aws.get("fs.s3a.bucket.bucket.endpoint")).isNull();
    assertThat(aws.get("fs.s3a.bucket.bucket.endpoint.region")).isNull();
  }

  private record Malformed(AwsCredentials credentials, String error) {}

  @Test
  public void testMalformedCredentialsAreRejected() {
    // Only a response with just the endpoint leaves the keys to the environment.
    List<Malformed> cases =
        List.of(
            new Malformed(new AwsCredentials(), "AWS access key is missing"),
            new Malformed(new AwsCredentials().secretAccessKey("v"), "AWS access key is missing"),
            new Malformed(
                new AwsCredentials().s3Endpoint("http://storage.example.com").sessionToken("t"),
                "AWS access key is missing"),
            new Malformed(new AwsCredentials().accessKeyId("k"), "AWS secret key is missing"),
            // AWS S3 always vends a session token.
            new Malformed(
                new AwsCredentials().accessKeyId("k").secretAccessKey("v"),
                "AWS session token is missing"));
    for (Malformed c : cases) {
      assertThatThrownBy(
              () ->
                  DeltaKernelUtils.getHDFSConfiguration(
                      URI.create(COMPAT_TABLE),
                      new TemporaryCredentials().awsTempCredentials(c.credentials())))
          .as(c.toString())
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessage(c.error());
    }
  }
}
