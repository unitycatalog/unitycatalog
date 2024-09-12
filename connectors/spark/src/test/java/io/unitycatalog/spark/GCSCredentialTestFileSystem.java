package io.unitycatalog.spark;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class GCSCredentialTestFileSystem extends CredentialTestFileSystem {
  @Override
  void checkCredentials(Path f) {
    Configuration conf = getConf();
    String host = f.toUri().getHost();
    if (credentialCheckEnabled) {
      if ("test-bucket0".equals(host)) {
        assertThat(conf.get(GcsVendedTokenProvider.ACCESS_TOKEN_KEY)).isEqualTo("testing://0");
        assertThat(conf.get(GcsVendedTokenProvider.ACCESS_TOKEN_EXPIRATION_KEY))
            .isEqualTo("253370790000000");
      } else if ("test-bucket1".equals(host)) {
        assertThat(conf.get(GcsVendedTokenProvider.ACCESS_TOKEN_KEY)).isEqualTo("testing://1");
        assertThat(conf.get(GcsVendedTokenProvider.ACCESS_TOKEN_EXPIRATION_KEY))
            .isEqualTo("253370790000000");
      } else {
        throw new RuntimeException("invalid path: " + f);
      }
    }
  }

  @Override
  String scheme() {
    return "gs:";
  }
}
