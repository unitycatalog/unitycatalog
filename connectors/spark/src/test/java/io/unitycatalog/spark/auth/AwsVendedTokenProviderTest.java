package io.unitycatalog.spark.auth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import io.unitycatalog.client.api.TemporaryCredentialsApi;
import io.unitycatalog.client.model.AwsCredentials;
import io.unitycatalog.client.model.TemporaryCredentials;
import io.unitycatalog.spark.UCHadoopConf;
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
      Configuration conf, TemporaryCredentialsApi mockApi) {
    return new TestAwsVendedTokenProvider(conf, mockApi);
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

  @Test
  public void testCustomEndpointUrlCredentialsReviewWithInitialCredentials() {
    Configuration conf = newTableBasedConf("unity-catalog-table");
    String customEndpoint = "http://custom-endpoint-url";
    conf.setLong(UCHadoopConf.UC_RENEWAL_LEAD_TIME_KEY, 1000L);
    conf.set(UCHadoopConf.S3A_INIT_ENDPOINT_URL, customEndpoint);

    // Create credentials with a custom endpoint URL
    TemporaryCredentials cred0 = newTempCred("0", System.currentTimeMillis() + 10000L);
    cred0.setEndpointUrl(customEndpoint);

    // Set initial credentials
    setInitialCred(conf, cred0);

    // Create a mock API (won't be called since we're using initial credentials)
    TemporaryCredentialsApi mockApi = mock(TemporaryCredentialsApi.class);

    // Create the provider
    AwsVendedTokenProvider provider = createTestProvider(conf, mockApi);

    // Verify the credentials are correctly loaded
    assertCred(provider, cred0);

    // Verify the endpoint URL is preserved in the generic credential
    GenericCredential genericCred = provider.accessCredentials();
    assertThat(genericCred.temporaryCredentials().getEndpointUrl()).isEqualTo(customEndpoint);
  }

  static class TestAwsVendedTokenProvider extends AwsVendedTokenProvider {
    private final TemporaryCredentialsApi tempCredApi;

    TestAwsVendedTokenProvider(Configuration conf, TemporaryCredentialsApi tempCredApi) {
      super(conf);
      this.tempCredApi = tempCredApi;
    }

    @Override
    protected TemporaryCredentialsApi temporaryCredentialsApi() {
      return tempCredApi;
    }
  }
}
