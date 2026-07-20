package io.unitycatalog.spark;

import io.unitycatalog.hadoop.internal.UCHadoopConfConstants;
import io.unitycatalog.hadoop.internal.auth.AwsVendedTokenProvider;
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
  protected void checkCredentials(Path f) throws IOException {
    Configuration conf = getConf();
    String host = f.toUri().getHost();

    if (credentialCheckEnabled) {
      if ("test-bucket0".equals(host)) {
        provider = accessProvider(conf);
        if (provider == null) {
          requireConfValue(conf, "fs.s3a.access.key", "accessKey0");
          requireConfValue(conf, "fs.s3a.secret.key", "secretKey0");
          requireConfValue(conf, "fs.s3a.session.token", "sessionToken0");
        } else {
          AwsSessionCredentials credentials = (AwsSessionCredentials) provider.resolveCredentials();
          requireValue("accessKeyId", credentials.accessKeyId(), "accessKey0");
          requireValue("secretAccessKey", credentials.secretAccessKey(), "secretKey0");
          requireValue("sessionToken", credentials.sessionToken(), "sessionToken0");
        }
      } else if ("test-bucket1".equals(host)) {
        provider = accessProvider(conf);
        if (provider == null) {
          requireConfValue(conf, "fs.s3a.access.key", "accessKey1");
          requireConfValue(conf, "fs.s3a.secret.key", "secretKey1");
          requireConfValue(conf, "fs.s3a.session.token", "sessionToken1");
        } else {
          AwsSessionCredentials credentials = (AwsSessionCredentials) provider.resolveCredentials();
          requireValue("accessKeyId", credentials.accessKeyId(), "accessKey1");
          requireValue("secretAccessKey", credentials.secretAccessKey(), "secretKey1");
          requireValue("sessionToken", credentials.sessionToken(), "sessionToken1");
        }
      } else {
        throw new RuntimeException("invalid path: " + f);
      }
    }
  }

  private static void requireConfValue(Configuration conf, String key, String expected)
      throws IOException {
    requireValue(key, conf.get(key), expected);
  }

  private static void requireValue(String name, String actual, String expected) throws IOException {
    if (!Objects.equals(expected, actual)) {
      throw new IOException(
          String.format(
              "missing vended credential %s: expected %s but was %s", name, expected, actual));
    }
  }

  private AwsCredentialsProvider accessProvider(Configuration conf) {
    if (provider == null) {
      synchronized (this) {
        if (provider == null) {
          // Assert that it's the expected credential provider.
          String clazz = conf.get(UCHadoopConfConstants.S3A_CREDENTIALS_PROVIDER);
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
