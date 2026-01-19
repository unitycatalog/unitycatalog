package io.unitycatalog.spark;

import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.spark.auth.storage.AwsVendedTokenProvider;
import java.io.IOException;
import java.net.URI;
import java.util.Objects;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;

public class S3CredentialTestFileSystem extends CredentialTestFileSystem {
  private volatile AwsCredentialsProvider provider;

  @Override
  public void initialize(URI uri, Configuration conf) throws IOException {
    super.initialize(uri, conf);
  }

  @Override
  protected void checkCredentials(Path f) {
    Configuration conf = getConf();
    String host = f.toUri().getHost();

    if (credentialCheckEnabled) {
      if ("test-bucket0".equals(host)) {
        provider = accessProvider(conf);
        if (provider == null) {
          assertThat(conf.get("fs.s3a.access.key")).isEqualTo("accessKey0");
          assertThat(conf.get("fs.s3a.secret.key")).isEqualTo("secretKey0");
          assertThat(conf.get("fs.s3a.session.token")).isEqualTo("sessionToken0");
        } else {
          AwsSessionCredentials credentials = (AwsSessionCredentials) provider.resolveCredentials();
          assertThat(credentials.accessKeyId()).isEqualTo("accessKey0");
          assertThat(credentials.secretAccessKey()).isEqualTo("secretKey0");
          assertThat(credentials.sessionToken()).isEqualTo("sessionToken0");
        }
      } else if ("test-bucket1".equals(host)) {
        provider = accessProvider(conf);
        if (provider == null) {
          assertThat(conf.get("fs.s3a.access.key")).isEqualTo("accessKey1");
          assertThat(conf.get("fs.s3a.secret.key")).isEqualTo("secretKey1");
          assertThat(conf.get("fs.s3a.session.token")).isEqualTo("sessionToken1");
        } else {
          AwsSessionCredentials credentials = (AwsSessionCredentials) provider.resolveCredentials();
          assertThat(credentials.accessKeyId()).isEqualTo("accessKey1");
          assertThat(credentials.secretAccessKey()).isEqualTo("secretKey1");
          assertThat(credentials.sessionToken()).isEqualTo("sessionToken1");
        }
      } else {
        throw new RuntimeException("invalid path: " + f);
      }
    }
  }

  private AwsCredentialsProvider accessProvider(Configuration conf) {
    if (provider == null) {
      synchronized (this) {
        if (provider == null) {
          // Assert that it's the expected credential provider.
          String clazz = conf.get(UCHadoopConf.S3A_CREDENTIALS_PROVIDER);
          if (Objects.equals(clazz, AwsVendedTokenProvider.class.getName())) {
            provider = new AwsVendedTokenProvider(conf);
          }
        }
      }
    }

    return provider;
  }

  @Override
  protected String scheme() {
    return "s3:";
  }
}
