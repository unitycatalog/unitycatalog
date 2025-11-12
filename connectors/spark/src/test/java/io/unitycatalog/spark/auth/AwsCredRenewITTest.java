package io.unitycatalog.spark.auth;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import io.unitycatalog.server.service.credential.aws.CredentialsGenerator;
import io.unitycatalog.spark.UCHadoopConf;
import java.time.Instant;
import java.util.Map;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.services.sts.model.Credentials;

public class AwsCredRenewITTest extends BaseCredRenewITTest {
  private static final String SCHEME = "s3";

  @Override
  protected void setUpProperties() {
    super.setUpProperties();
    serverProperties.put("s3.bucketPath.0", "s3://" + BUCKET_NAME);
    serverProperties.put("s3.accessKey.0", "accessKey0");
    serverProperties.put("s3.secretKey.0", "secretKey0");
    serverProperties.put("s3.sessionToken.0", "sessionToken0");
    // Customize the test credential generator to issue a new credential every 30-second interval.
    // This allows us to verify whether credential renewal is functioning correctly by checking
    // if the current credential matches the expected time window.
    serverProperties.put("s3.credentialsGenerator.0", AwsCredGenerator.class.getName());
  }

  @Override
  protected String scheme() {
    return SCHEME;
  }

  @Override
  protected Map<String, String> catalogExtraProps() {
    return Map.of("fs.s3.impl", S3CredFileSystem.class.getName());
  }

  public static class AwsCredGenerator extends TimeBasedCredGenerator<Credentials>
      implements CredentialsGenerator {
    @Override
    protected Credentials newTimeBasedCred(long ts) {
      return Credentials.builder()
          .accessKeyId("accessKeyId" + ts)
          .secretAccessKey("secretAccessKey" + ts)
          .sessionToken("sessionToken" + ts)
          .expiration(Instant.ofEpochMilli(ts + DEFAULT_INTERVAL_MILLIS))
          .build();
    }
  }

  public static class S3CredFileSystem extends CredRenewFileSystem<AwsCredentialsProvider> {
    @Override
    protected String scheme() {
      return String.format("%s:", SCHEME);
    }

    @Override
    protected AwsCredentialsProvider createProvider() {
      String clazz = getConf().get(UCHadoopConf.S3A_CREDENTIALS_PROVIDER);
      assertThat(clazz).isEqualTo(AwsVendedTokenProvider.class.getName());

      // This will validate if the hadoop configuration is correct or not, since it will fail the
      // provider constructor if given an incorrect setting here.
      return new AwsVendedTokenProvider(getConf());
    }

    @Override
    protected void assertCredentials(AwsCredentialsProvider provider, long ts) {
      AwsSessionCredentials cred = (AwsSessionCredentials) provider.resolveCredentials();
      assertThat(cred.accessKeyId()).isEqualTo("accessKeyId" + ts);
      assertThat(cred.secretAccessKey()).isEqualTo("secretAccessKey" + ts);
      assertThat(cred.sessionToken()).isEqualTo("sessionToken" + ts);
    }
  }
}
