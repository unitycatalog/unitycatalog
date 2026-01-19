package io.unitycatalog.spark.auth.storage;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

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
    // Customize the time based credential generator to issue a new credential every 30 sec.
    serverProperties.put("adls.credentialsGenerator.0", AzureCredGenerator.class.getName());
  }

  @Override
  protected String scheme() {
    return SCHEME;
  }

  @Override
  protected Map<String, String> catalogExtraProps() {
    return Map.of("fs.abfs.impl", AbfsCredFileSystem.class.getName());
  }

  public static class AzureCredGenerator extends TimeBasedCredGenerator<AzureCredential>
      implements AzureCredentialsGenerator {
    // Default constructor for AzureCredGenerator reflection.
    public AzureCredGenerator(ADLSStorageConfig ignore) {}

    @Override
    protected AzureCredential newTimeBasedCred(long ts) {
      String sasToken = String.format("sasToken-%s", ts);
      return AzureCredential.builder()
          .sasToken(sasToken)
          .expirationTimeInEpochMillis(ts + DEFAULT_INTERVAL_MILLIS)
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
