package io.unitycatalog.spark.auth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.hadoop.util.AccessTokenProvider;
import com.google.cloud.hadoop.util.AccessTokenProvider.AccessToken;
import com.google.cloud.hadoop.util.HadoopCredentialsConfiguration;
import com.google.cloud.hadoop.util.HadoopCredentialsConfiguration.AccessTokenProviderCredentials;
import io.unitycatalog.client.api.TemporaryCredentialsApi;
import io.unitycatalog.client.model.GcpOauthToken;
import io.unitycatalog.client.model.TemporaryCredentials;
import io.unitycatalog.spark.UCHadoopConf;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.jupiter.api.Test;

public class GcsVendedTokenProviderTest extends BaseTokenProviderTest<GcsVendedTokenProvider> {

  @Override
  protected GcsVendedTokenProvider createTestProvider(
      Configuration conf, TemporaryCredentialsApi mockApi) {
    return new TestGcsVendedTokenProvider(conf, mockApi);
  }

  static class TestGcsVendedTokenProvider extends GcsVendedTokenProvider {
    private final TemporaryCredentialsApi mockApi;

    TestGcsVendedTokenProvider(Configuration conf, TemporaryCredentialsApi mockApi) {
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
    conf.set(UCHadoopConf.GCS_INIT_OAUTH_TOKEN, cred.getGcpOauthToken().getOauthToken());
    if (cred.getExpirationTime() != null) {
      conf.setLong(UCHadoopConf.GCS_INIT_OAUTH_TOKEN_EXPIRATION_TIME, cred.getExpirationTime());
    }
  }

  @Override
  protected void assertCred(GcsVendedTokenProvider provider, TemporaryCredentials expected) {
    AccessToken token = provider.getAccessToken();

    assertThat(token).isNotNull();
    assertThat(expected.getGcpOauthToken()).isNotNull();
    assertThat(token.getToken()).isEqualTo(expected.getGcpOauthToken().getOauthToken());

    Long expectedExpiration = expected.getExpirationTime();
    if (expectedExpiration == null) {
      assertThat(token.getExpirationTime()).isNull();
    } else {
      assertThat(token.getExpirationTime()).isNotNull();
      assertThat(token.getExpirationTime().toEpochMilli()).isEqualTo(expectedExpiration);
    }
  }

  @Test
  public void testConstructor() {
    Configuration conf = new Configuration();
    GcsVendedTokenProvider provider = new GcsVendedTokenProvider();

    assertThatThrownBy(() -> provider.setConf(conf))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("'%s' is not set in hadoop configuration", UCHadoopConf.UC_URI_KEY);

    conf.set(UCHadoopConf.UC_URI_KEY, "http://localhost:8080");
    assertThatThrownBy(() -> provider.setConf(conf))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("'%s' is not set in hadoop configuration", UCHadoopConf.UC_TOKEN_KEY);

    conf.set(UCHadoopConf.UC_TOKEN_KEY, "unity-catalog-token");
    assertThatThrownBy(() -> provider.setConf(conf))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "Credential UID cannot be null or empty, '%s' is not set in hadoop configuration",
            UCHadoopConf.UC_CREDENTIALS_UID_KEY);
  }

  @Test
  public void testLoadProvider() throws Exception {
    Configuration conf = newTableBasedConf();
    String ghfsClassName = "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem";
    conf.set("fs.gs.impl", ghfsClassName);
    conf.setBoolean("fs.gs.impl.disable.cache", true);
    conf.set("fs.gs.project.id", "test-project");
    conf.set("fs.gs.auth.type", "ACCESS_TOKEN_PROVIDER");
    conf.set("fs.gs.auth.access.token.provider", GcsVendedTokenProvider.class.getName());
    conf.set("fs.gs.auth.access.token.provider.impl", GcsVendedTokenProvider.class.getName());

    FileSystem fs = FileSystem.newInstance(new URI("gs://test-bucket0"), conf);
    try {
      assertThat(fs.getClass().getName()).isEqualTo(ghfsClassName);

      GoogleCredentials credentials =
          HadoopCredentialsConfiguration.getCredentials(
              fs.getConf(), "fs.gs");

      assertThat(credentials).isInstanceOf(AccessTokenProviderCredentials.class);

      AccessTokenProvider provider =
          ((AccessTokenProviderCredentials) credentials).getAccessTokenProvider();

      assertThat(provider).isInstanceOf(GcsVendedTokenProvider.class);
    } finally {
      fs.close();
    }
  }
}
