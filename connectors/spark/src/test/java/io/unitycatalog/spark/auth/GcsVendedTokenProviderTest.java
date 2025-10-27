package io.unitycatalog.spark.auth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

import com.google.cloud.hadoop.util.AccessTokenProvider;
import io.unitycatalog.client.api.TemporaryCredentialsApi;
import io.unitycatalog.client.model.GcpOauthToken;
import io.unitycatalog.client.model.TemporaryCredentials;
import io.unitycatalog.spark.UCHadoopConf;
import io.unitycatalog.spark.utils.Clock;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

public class GcsVendedTokenProviderTest extends BaseTokenProviderTest<GcsVendedTokenProvider> {
  @Override
  protected GcsVendedTokenProvider createTestProvider(
      Clock clock,
      long renewalLeadTimeMillis,
      Configuration conf,
      TemporaryCredentialsApi mockApi) {
    return new TestGcsVendedTokenProvider(clock, renewalLeadTimeMillis, conf, mockApi);
  }

  static class TestGcsVendedTokenProvider extends GcsVendedTokenProvider {
    private final TemporaryCredentialsApi mockApi;

    TestGcsVendedTokenProvider(
        Clock clock,
        long renewalLeadTimeMillis,
        Configuration conf,
        TemporaryCredentialsApi mockApi) {
      super(clock, renewalLeadTimeMillis);
      setConf(conf);
      this.mockApi = mockApi;
    }

    @Override
    protected TemporaryCredentialsApi temporaryCredentialsApi() {
      return mockApi;
    }
  }

  @Override
  protected TemporaryCredentials newTempCred(String id, long expirationMillis) {
    GcpOauthToken oauthToken = new GcpOauthToken();
    oauthToken.setOauthToken("oauthToken" + id);

    TemporaryCredentials tempCred = new TemporaryCredentials();
    tempCred.setGcpOauthToken(oauthToken);
    tempCred.setExpirationTime(expirationMillis);
    return tempCred;
  }

  @Override
  protected void setInitialCred(Configuration conf, TemporaryCredentials cred) {
    assertThat(cred.getGcpOauthToken()).isNotNull();
    assertThat(cred.getExpirationTime()).isNotNull();
    conf.set(UCHadoopConf.GCS_INIT_OAUTH_TOKEN, cred.getGcpOauthToken().getOauthToken());
    conf.setLong(UCHadoopConf.GCS_INIT_OAUTH_TOKEN_EXPIRED_TIME, cred.getExpirationTime());
  }

  @Override
  protected void assertCred(GcsVendedTokenProvider provider, TemporaryCredentials expected) {
    AccessTokenProvider.AccessToken accessToken = provider.getAccessToken();

    assertThat(expected.getGcpOauthToken()).isNotNull();
    GcpOauthToken expectedToken = expected.getGcpOauthToken();

    assertThat(accessToken.getToken()).isEqualTo(expectedToken.getOauthToken());
    assertThat(accessToken.getExpirationTime().toEpochMilli())
        .isEqualTo(expected.getExpirationTime());
  }

  @Test
  public void testConstructor() {
    Configuration conf = new Configuration();
    GcsVendedTokenProvider provider = new GcsVendedTokenProvider();

    // Verify the UC URI validation error message.
    assertThatThrownBy(() -> provider.setConf(conf))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("'%s' is not set in hadoop configuration", UCHadoopConf.UC_URI_KEY);

    // Verify the UC Token validation error message.
    conf.set(UCHadoopConf.UC_URI_KEY, "http://localhost:8080");
    assertThatThrownBy(() -> provider.setConf(conf))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("'%s' is not set in hadoop configuration", UCHadoopConf.UC_TOKEN_KEY);

    // Verify the UID validation error message.
    conf.set(UCHadoopConf.UC_TOKEN_KEY, "unity-catalog-token");
    assertThatThrownBy(() -> provider.setConf(conf))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "Credential UID cannot be null or empty, '%s' is not set in hadoop configuration",
            UCHadoopConf.UC_CREDENTIALS_UID_KEY);
  }

  @Test
  public void testLoadProvider() {
    Configuration conf = newTableBasedConf();
    conf.set("fs.gs.auth.type", "ACCESS_TOKEN_PROVIDER");
    conf.set("fs.gs.auth.access.token.provider", GcsVendedTokenProvider.class.getName());

    // Create a provider using the configuration
    GcsVendedTokenProvider provider = new GcsVendedTokenProvider();
    provider.setConf(conf);

    assertThat(provider).isInstanceOf(GcsVendedTokenProvider.class);
    assertThat(provider.getConf()).isEqualTo(conf);
  }
}

