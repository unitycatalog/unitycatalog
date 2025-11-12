package io.unitycatalog.spark.auth;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.service.credential.azure.ADLSStorageConfig;
import io.unitycatalog.server.service.credential.azure.AzureCredential;
import io.unitycatalog.server.service.credential.azure.AzureCredentialsGenerator;
import io.unitycatalog.spark.UCHadoopConf;
import java.util.Map;

public class AbfsCredRenewITTest extends BaseCredRenewITTest {
  private static final String SCHEME = "abfs";

  @Override
  protected void setUpProperties() {
    super.setUpProperties();
    serverProperties.put("adls.storageAccountName.0", BUCKET_NAME);
    serverProperties.put("adls.tenantId.0", "tenantId0");
    serverProperties.put("adls.clientId.0", "clientId0");
    serverProperties.put("adls.clientSecret.0", "clientSecret0");
    serverProperties.put("adls.credentialsGenerator.0", TimeBasedCredGenerator.class.getName());
  }

  @Override
  protected String scheme() {
    return SCHEME;
  }

  @Override
  protected Map<String, String> catalogExtraProps() {
    return Map.of("fs.abfs.impl", AbfsCredFileSystem.class.getName());
  }

  public static class TimeBasedCredGenerator implements AzureCredentialsGenerator {
    // Default constructor for AzureCredentialGenerator reflection.
    public TimeBasedCredGenerator(ADLSStorageConfig ignore) {
    }

    @Override
    public AzureCredential generate(CredentialContext credentialContext) {
      long curTsMillis = testClock().now().toEpochMilli();
      // Align it into the window [starTs, starTs + DEFAULT_INTERVAL_MILLIS].
      long startTsMillis = curTsMillis / DEFAULT_INTERVAL_MILLIS * DEFAULT_INTERVAL_MILLIS;
      String sasToken = String.format("sasToken-%s", startTsMillis);
      return AzureCredential.builder()
          .sasToken(sasToken)
          .expirationTimeInEpochMillis(startTsMillis + DEFAULT_INTERVAL_MILLIS)
          .build();
    }
  }

  public static class AbfsCredFileSystem extends CredRenewFileSystem<AbfsVendedTokenProvider> {

    @Override
    protected String scheme() {
      return String.format("%s:", SCHEME);
    }

    @Override
    protected AbfsVendedTokenProvider createProvider() {
      String clazz = getConf().get(UCHadoopConf.FS_AZURE_SAS_TOKEN_PROVIDER_TYPE);
      assertThat(clazz).isEqualTo(AbfsVendedTokenProvider.class.getName());

      AbfsVendedTokenProvider provider = new AbfsVendedTokenProvider();
      provider.initialize(getConf(), "testAccount");
      return provider;
    }

    @Override
    protected void assertCredentials(AbfsVendedTokenProvider provider, long ts) {
      String sasToken = provider.getSASToken("testAccount", "testFs", "testPath", "testOperation");
      assertThat(sasToken).isEqualTo("sasToken-" + ts);
    }
  }
}
