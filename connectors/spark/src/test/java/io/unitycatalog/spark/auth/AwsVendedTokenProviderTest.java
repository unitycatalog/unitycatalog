package io.unitycatalog.spark.auth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

import io.unitycatalog.client.api.TemporaryCredentialsApi;
import io.unitycatalog.client.model.AwsCredentials;
import io.unitycatalog.client.model.TemporaryCredentials;
import io.unitycatalog.spark.RetryableTemporaryCredentialsApi;
import io.unitycatalog.spark.UCHadoopConf;
import io.unitycatalog.spark.utils.Clock;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.AWSCredentialProviderList;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.fs.s3a.auth.CredentialProviderListFactory;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;

public class AwsVendedTokenProviderTest extends BaseTokenProviderTest<AwsVendedTokenProvider> {

  @Override
  protected AwsVendedTokenProvider createTestProvider(
      Clock clock,
      long renewalLeadTimeMillis,
      Configuration conf,
      TemporaryCredentialsApi mockApi) {
    return new TestAwsVendedTokenProvider(clock, renewalLeadTimeMillis, conf, mockApi);
  }

  static class TestAwsVendedTokenProvider extends AwsVendedTokenProvider {
    private final RetryableTemporaryCredentialsApi retryableApi;

    TestAwsVendedTokenProvider(
        Clock clock,
        long renewalLeadTimeMillis,
        Configuration conf,
        TemporaryCredentialsApi tempCredApi) {
      super(conf, clock, renewalLeadTimeMillis);
      this.retryableApi = new RetryableTemporaryCredentialsApi(tempCredApi, conf);
    }

    @Override
    protected RetryableTemporaryCredentialsApi temporaryCredentialsApi() {
      return retryableApi;
    }
  }

  @Override
  protected TemporaryCredentials newTempCred(String id, long expirationMillis) {
    AwsCredentials awsCred = new AwsCredentials();
    awsCred.setAccessKeyId("accessKeyId" + id);
    awsCred.setSecretAccessKey("secretAccessKey" + id);
    awsCred.setSessionToken("sessionToken" + id);

    TemporaryCredentials tempCred = new TemporaryCredentials();
    tempCred.setAwsTempCredentials(awsCred);
    tempCred.setExpirationTime(expirationMillis);

    return tempCred;
  }

  @Override
  protected void setInitialCred(Configuration conf, TemporaryCredentials cred) {
    conf.set(UCHadoopConf.S3A_INIT_ACCESS_KEY, cred.getAwsTempCredentials().getAccessKeyId());
    conf.set(UCHadoopConf.S3A_INIT_SECRET_KEY, cred.getAwsTempCredentials().getSecretAccessKey());
    conf.set(UCHadoopConf.S3A_INIT_SESSION_TOKEN, cred.getAwsTempCredentials().getSessionToken());
    conf.setLong(UCHadoopConf.S3A_INIT_CRED_EXPIRED_TIME, cred.getExpirationTime());
  }

  @Override
  protected void assertCred(AwsVendedTokenProvider provider, TemporaryCredentials expected) {
    software.amazon.awssdk.auth.credentials.AwsCredentials actual = provider.resolveCredentials();

    assertThat(actual).isInstanceOf(AwsSessionCredentials.class);
    AwsSessionCredentials actualSessionCred = (AwsSessionCredentials) actual;

    assertThat(expected.getAwsTempCredentials()).isNotNull();
    AwsCredentials expectedAwsCred = expected.getAwsTempCredentials();

    assertThat(actualSessionCred.accessKeyId()).isEqualTo(expectedAwsCred.getAccessKeyId());
    assertThat(actualSessionCred.secretAccessKey()).isEqualTo(expectedAwsCred.getSecretAccessKey());
    assertThat(actualSessionCred.sessionToken()).isEqualTo(expectedAwsCred.getSessionToken());
  }

  @Test
  public void testConstructor() {
    Configuration conf = new Configuration();

    // Verify the UC URI validation error message.
    assertThatThrownBy(() -> new AwsVendedTokenProvider(conf))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("'%s' is not set in hadoop configuration", UCHadoopConf.UC_URI_KEY);

    // Verify the UC Token validation error message.
    conf.set(UCHadoopConf.UC_URI_KEY, "http://localhost:8080");
    assertThatThrownBy(() -> new AwsVendedTokenProvider(conf))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("'%s' is not set in hadoop configuration", UCHadoopConf.UC_TOKEN_KEY);

    // Verify the UID validation error message.
    conf.set(UCHadoopConf.UC_TOKEN_KEY, "unity-catalog-token");
    assertThatThrownBy(() -> new AwsVendedTokenProvider(conf))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "Credential UID cannot be null or empty, '%s' is not set in hadoop configuration",
            UCHadoopConf.UC_CREDENTIALS_UID_KEY);
  }

  @Test
  public void testLoadProvider() throws IOException {
    Configuration conf = newTableBasedConf("unity-catalog-table");
    conf.set(Constants.AWS_CREDENTIALS_PROVIDER, AwsVendedTokenProvider.class.getName());

    AWSCredentialProviderList list =
        CredentialProviderListFactory.buildAWSProviderList(
            URI.create("s3://bucket/key"),
            conf,
            Constants.AWS_CREDENTIALS_PROVIDER,
            new ArrayList<>(),
            new HashSet<>());

    List<AwsCredentialsProvider> providers = list.getProviders();
    assertThat(providers).hasSize(1);
    assertThat(providers.get(0)).isInstanceOf(AwsVendedTokenProvider.class);
  }
}
