package io.unitycatalog.hadoop.internal.auth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

import io.unitycatalog.client.api.TemporaryCredentialsApi;
import io.unitycatalog.client.model.AzureUserDelegationSAS;
import io.unitycatalog.client.model.TemporaryCredentials;
import io.unitycatalog.hadoop.internal.UCHadoopConfConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys;
import org.apache.hadoop.fs.azurebfs.extensions.SASTokenProvider;
import org.apache.hadoop.fs.azurebfs.services.AuthType;
import org.junit.jupiter.api.Test;

public class AbfsVendedTokenProviderTest extends BaseTokenProviderTest<AbfsVendedTokenProvider> {
  @Override
  protected AbfsVendedTokenProvider createTestProvider(
      Configuration conf, TemporaryCredentialsApi mockApi) {
    return new TestAbfsVendedTokenProvider(conf, mockApi);
  }

  static class TestAbfsVendedTokenProvider extends AbfsVendedTokenProvider {
    private final TempCredentialApi credentialApi;

    TestAbfsVendedTokenProvider(Configuration conf, TemporaryCredentialsApi mockApi) {
      initialize(conf);
      this.credentialApi = new UCTempCredentialApi(conf, mockApi);
    }

    @Override
    TempCredentialApi tempCredentialApi() {
      return credentialApi;
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
    conf.set(
        UCHadoopConfConstants.AZURE_INIT_SAS_TOKEN, cred.getAzureUserDelegationSas().getSasToken());
    conf.setLong(UCHadoopConfConstants.AZURE_INIT_SAS_TOKEN_EXPIRED_TIME, cred.getExpirationTime());
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

    assertThatThrownBy(() -> provider.initialize(conf))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "Credential UID cannot be null or empty, '%s' is not set in hadoop configuration",
            UCHadoopConfConstants.UC_CREDENTIALS_UID_KEY);
  }

  @Test
  public void testLoadProvider() throws Exception {
    Configuration conf = newTableBasedConf();
    conf.setEnum(ConfigurationKeys.FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME, AuthType.SAS);
    conf.set(
        ConfigurationKeys.FS_AZURE_SAS_TOKEN_PROVIDER_TYPE,
        AbfsVendedTokenProvider.class.getName());

    AbfsConfiguration abfsConf = new AbfsConfiguration(conf, "account");

    SASTokenProvider provider = abfsConf.getSASTokenProvider();
    assertThat(provider).isInstanceOf(AbfsVendedTokenProvider.class);
  }
}
