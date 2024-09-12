package io.unitycatalog.spark;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class AzureCredentialTestFileSystem extends CredentialTestFileSystem {
  @Override
  void checkCredentials(Path f) {
    Configuration conf = getConf();
    String host = f.toUri().getHost();
    if (credentialCheckEnabled) {
      String mockToken = conf.get(AbfsVendedTokenProvider.ACCESS_TOKEN_KEY);
      if ("test-bucket0".equals(host)) {
        assertThat(mockToken).isEqualTo("tenantId0/clientId0/clientSecret0");
      } else if ("test-bucket1".equals(host)) {
        assertThat(mockToken).isEqualTo("tenantId1/clientId1/clientSecret1");
      } else {
        throw new RuntimeException("invalid path: " + f);
      }
    }
  }

  @Override
  String scheme() {
    return "abfs:";
  }
}
