package io.unitycatalog.spark.auth;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.service.credential.aws.CredentialsGenerator;
import io.unitycatalog.spark.UCHadoopConf;
import io.unitycatalog.spark.utils.Clock;
import java.io.IOException;
import java.net.URI;
import java.time.Instant;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterAll;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.services.sts.model.Credentials;

public class AwsCredRenewITTest extends BaseCredRenewITTest {
  private static final String CLOCK_NAME = AwsCredRenewITTest.class.getSimpleName();
  private static final String CREDENTIALS_GENERATOR = TimeBasedCredentialsGenerator.class.getName();
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
    serverProperties.put("s3.credentialsGenerator.0", CREDENTIALS_GENERATOR);
  }

  @Override
  protected String scheme() {
    return SCHEME;
  }

  @Override
  protected Map<String, String> catalogProps() {
    return Map.of("fs.s3.impl", S3CredFileSystem.class.getName());
  }

  @Override
  protected CredRenewFileSystem createTestFileSystem(URI uri, Configuration conf)
      throws IOException {
    FileSystem fs = FileSystem.get(uri, conf);
    assertThat(fs).isInstanceOf(S3CredFileSystem.class);
    return (S3CredFileSystem) fs;
  }

  @AfterAll
  public static void afterAll() {
    Clock.removeManualClock(CLOCK_NAME);
  }

  public static Clock testClock() {
    return Clock.getManualClock(CLOCK_NAME);
  }

  /**
   * A customized {@link CredentialsGenerator} that generates credentials based on time intervals.
   * The entire timeline is divided into consecutive 30-second windows, and all requests that fall
   * within the same window will receive the same credential. This generator is dynamically loaded
   * by the Unity Catalog server and serves credential generation requests from client REST API
   * calls.
   */
  public static class TimeBasedCredentialsGenerator implements CredentialsGenerator {

    @Override
    public Credentials generate(CredentialContext credentialContext) {
      long curTsMillis = testClock().now().toEpochMilli();
      // Align it into the window [starTs, starTs + DEFAULT_INTERVAL_MILLIS].
      long startTsMillis = curTsMillis / DEFAULT_INTERVAL_MILLIS * DEFAULT_INTERVAL_MILLIS;
      return Credentials.builder()
          .accessKeyId("accessKeyId" + startTsMillis)
          .secretAccessKey("secretAccessKey" + startTsMillis)
          .sessionToken("sessionToken" + startTsMillis)
          .expiration(Instant.ofEpochMilli(startTsMillis + DEFAULT_INTERVAL_MILLIS))
          .build();
    }
  }

  /**
   * A testing S3 filesystem used to verify credential renewal behavior. For each {@code
   * checkCredentials()} call, the previous credentials should automatically renew as the 30-second
   * time window advances. The test tracks how many distinct credentials this filesystem receives,
   * which should match the expected number of credential renewals. We use this filesystem to
   * accurately track how many renewal happened.
   */
  public static class S3CredFileSystem extends CredRenewFileSystem {
    private final Set<Long> verifiedTs = new HashSet<>();
    private volatile AwsCredentialsProvider lazyProvider;

    @Override
    protected void checkCredentials(Path f) {
      String host = f.toUri().getHost();

      if (credentialCheckEnabled && BUCKET_NAME.equals(host)) {
        AwsCredentialsProvider provider = accessProvider();
        assertThat(provider).isNotNull();

        AwsSessionCredentials cred = (AwsSessionCredentials) provider.resolveCredentials();
        long ts =
            testClock().now().toEpochMilli() / DEFAULT_INTERVAL_MILLIS * DEFAULT_INTERVAL_MILLIS;
        assertThat(cred.accessKeyId()).isEqualTo("accessKeyId" + ts);
        assertThat(cred.secretAccessKey()).isEqualTo("secretAccessKey" + ts);
        assertThat(cred.sessionToken()).isEqualTo("sessionToken" + ts);

        verifiedTs.add(ts);
      }
    }

    @Override
    public int renewalCount() {
      return verifiedTs.size() - 1;
    }

    @Override
    protected String scheme() {
      return String.format("%s:", SCHEME);
    }

    private synchronized AwsCredentialsProvider accessProvider() {
      if (lazyProvider != null) {
        return lazyProvider;
      }

      String clazz = getConf().get(UCHadoopConf.S3A_CREDENTIALS_PROVIDER);
      assertThat(clazz).isEqualTo(AwsVendedTokenProvider.class.getName());

      // This will validate if the hadoop configuration is correct or not, since it will fail the
      // provider constructor if given an incorrect setting here.
      lazyProvider = new AwsVendedTokenProvider(getConf());

      return lazyProvider;
    }
  }
}
