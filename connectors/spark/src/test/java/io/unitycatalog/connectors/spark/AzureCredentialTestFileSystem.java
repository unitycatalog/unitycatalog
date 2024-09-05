package io.unitycatalog.connectors.spark;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class AzureCredentialTestFileSystem extends CredentialTestFileSystem {
  @Override
  void checkCredentials(Path f) {
    Configuration conf = getConf();
    String host = f.toUri().getHost();
    if (credentialCheckEnabled) {
      String tokenProp = format("fs.azure.sas.fixed.token.%s.dfs.core.windows.net", host);
      String token = conf.get(tokenProp);
      if ("test-bucket0".equals(host)) {
        assertThat(token).isEqualTo("tenantId0/clientId0/clientSecret0");
      } else if ("test-bucket1".equals(host)) {
        assertThat(token).isEqualTo("tenantId1/clientId1/clientSecret1");
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
