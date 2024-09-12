package io.unitycatalog.spark;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class S3CredentialTestFileSystem extends CredentialTestFileSystem {
  @Override
  void checkCredentials(Path f) {
    Configuration conf = getConf();
    String host = f.toUri().getHost();
    if (credentialCheckEnabled) {
      if ("test-bucket0".equals(host)) {
        assertThat(conf.get("fs.s3a.access.key")).isEqualTo("accessKey0");
        assertThat(conf.get("fs.s3a.secret.key")).isEqualTo("secretKey0");
        assertThat(conf.get("fs.s3a.session.token")).isEqualTo("sessionToken0");
      } else if ("test-bucket1".equals(host)) {
        assertThat(conf.get("fs.s3a.access.key")).isEqualTo("accessKey1");
        assertThat(conf.get("fs.s3a.secret.key")).isEqualTo("secretKey1");
        assertThat(conf.get("fs.s3a.session.token")).isEqualTo("sessionToken1");
      } else {
        throw new RuntimeException("invalid path: " + f);
      }
    }
  }

  @Override
  String scheme() {
    return "s3:";
  }
}
