package io.unitycatalog.spark;

import static io.unitycatalog.spark.UCHadoopConf.FS_AZURE_SAS_TOKEN_PROVIDER_TYPE;
import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.spark.auth.AbfsVendedTokenProvider;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.sparkproject.guava.base.Objects;

public class AzureCredentialTestFileSystem extends CredentialTestFileSystem {
  private volatile AbfsVendedTokenProvider provider;

  @Override
  protected void checkCredentials(Path f) {
    Configuration conf = getConf();
    String host = f.toUri().getHost();
    if (credentialCheckEnabled) {
      if ("test-bucket0".equals(host)) {
        provider = accessProvider(conf);
        if (provider == null) {
          String mockToken = conf.get(AbfsVendedTokenProvider.ACCESS_TOKEN_KEY);
          assertThat(mockToken).isEqualTo("tenantId0/clientId0/clientSecret0");
        } else {
          String sasToken = provider.getSASToken("testAccount", "testFs", "testPath", "testOp");
          assertThat(sasToken).isEqualTo("tenantId0/clientId0/clientSecret0");
        }
      } else if ("test-bucket1".equals(host)) {
        provider = accessProvider(conf);
        if (provider == null) {
          String mockToken = conf.get(AbfsVendedTokenProvider.ACCESS_TOKEN_KEY);
          assertThat(mockToken).isEqualTo("tenantId1/clientId1/clientSecret1");
        } else {
          String sasToken = provider.getSASToken("testAccount", "testFs", "testPath", "testOp");
          assertThat(sasToken).isEqualTo("tenantId1/clientId1/clientSecret1");
        }
      } else {
        throw new RuntimeException("invalid path: " + f);
      }
    }
  }

  private AbfsVendedTokenProvider accessProvider(Configuration conf) {
    if (provider == null) {
      synchronized (this) {
        if (provider == null) {
          String clazz = conf.get(FS_AZURE_SAS_TOKEN_PROVIDER_TYPE);
          if (Objects.equal(clazz, AbfsVendedTokenProvider.class.getName())) {
            provider = new AbfsVendedTokenProvider();
            provider.initialize(conf, "testAccountName");
          }
        }
      }
    }

    return provider;
  }

  @Override
  protected String scheme() {
    return "abfs:";
  }
}
