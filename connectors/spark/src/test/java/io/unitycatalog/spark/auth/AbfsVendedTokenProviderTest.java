package io.unitycatalog.spark.auth;

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_SAS_TOKEN_PROVIDER_TYPE;
import static org.apache.hadoop.fs.azurebfs.services.AuthType.SAS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

import io.unitycatalog.client.api.TemporaryCredentialsApi;
import io.unitycatalog.client.model.AzureUserDelegationSAS;
import io.unitycatalog.client.model.TemporaryCredentials;
import io.unitycatalog.spark.RetryableTemporaryCredentialsApi;
import io.unitycatalog.spark.UCHadoopConf;
import io.unitycatalog.spark.utils.Clock;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.extensions.SASTokenProvider;
import org.junit.jupiter.api.Test;

public class AbfsVendedTokenProviderTest extends BaseTokenProviderTest<AbfsVendedTokenProvider> {
  @Override
  protected AbfsVendedTokenProvider createTestProvider(
      Clock clock,
      long renewalLeadTimeMillis,
      Configuration conf,
      TemporaryCredentialsApi mockApi) {
    return new TestAbfsVendedTokenProvider(clock, renewalLeadTimeMillis, conf, mockApi);
  }

  static class TestAbfsVendedTokenProvider extends AbfsVendedTokenProvider {
    private final RetryableTemporaryCredentialsApi retryableApi;

    TestAbfsVendedTokenProvider(
        Clock clock,
        long renewalLeadTimeMillis,
        Configuration conf,
        TemporaryCredentialsApi mockApi) {
      super(clock, renewalLeadTimeMillis);
      initialize(conf);
      // Wrap the mocked API in the retryable wrapper
      this.retryableApi = new RetryableTemporaryCredentialsApi(mockApi, conf);
    }

    @Override
    protected RetryableTemporaryCredentialsApi temporaryCredentialsApi() {
      return retryableApi;
    }
  }

  @Override
  protected TemporaryCredentials newTempCred(String id, long expirationMillis) {
    AzureUserDelegationSAS sas = new AzureUserDelegationSAS();
    sas.setSasToken("sasToken" + id);

    TemporaryCredentials tempCred = new TemporaryCredentials();
    tempCred.setAzureUserDelegationSas(sas);
    tempCred.setExpirationTime(expirationMillis);
    return tempCred;
  }

  @Override
  protected void setInitialCred(Configuration conf, TemporaryCredentials cred) {
    assertThat(cred.getAzureUserDelegationSas()).isNotNull();
    assertThat(cred.getExpirationTime()).isNotNull();
    conf.set(UCHadoopConf.AZURE_INIT_SAS_TOKEN, cred.getAzureUserDelegationSas().getSasToken());
    conf.setLong(UCHadoopConf.AZURE_INIT_SAS_TOKEN_EXPIRED_TIME, cred.getExpirationTime());
  }

  @Override
  protected void assertCred(AbfsVendedTokenProvider provider, TemporaryCredentials expected) {
    String sasToken = provider.getSASToken("account", "fileSystem", "path", "operation");

    assertThat(expected.getAzureUserDelegationSas()).isNotNull();
    AzureUserDelegationSAS expectedSAS = expected.getAzureUserDelegationSas();

    assertThat(expectedSAS.getSasToken()).isEqualTo(sasToken);
  }

  @Test
  public void testConstructor() {
    Configuration conf = new Configuration();
    AbfsVendedTokenProvider provider = new AbfsVendedTokenProvider();

    // Verify the UC URI validation error message.
    assertThatThrownBy(() -> provider.initialize(conf))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("'%s' is not set in hadoop configuration", UCHadoopConf.UC_URI_KEY);

    // Verify the UC Token validation error message.
    conf.set(UCHadoopConf.UC_URI_KEY, "http://localhost:8080");
    assertThatThrownBy(() -> provider.initialize(conf))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("'%s' is not set in hadoop configuration", UCHadoopConf.UC_TOKEN_KEY);

    // Verify the UID validation error message.
    conf.set(UCHadoopConf.UC_TOKEN_KEY, "unity-catalog-token");
    assertThatThrownBy(() -> provider.initialize(conf))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "Credential UID cannot be null or empty, '%s' is not set in hadoop configuration",
            UCHadoopConf.UC_CREDENTIALS_UID_KEY);
  }

  @Test
  public void testLoadProvider() throws Exception {
    Configuration conf = newTableBasedConf();
    conf.setEnum(FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME, SAS);
    conf.set(FS_AZURE_SAS_TOKEN_PROVIDER_TYPE, AbfsVendedTokenProvider.class.getName());

    AbfsConfiguration abfsConf = new AbfsConfiguration(conf, "account");

    SASTokenProvider provider = abfsConf.getSASTokenProvider();
    assertThat(provider).isInstanceOf(AbfsVendedTokenProvider.class);
  }
}
