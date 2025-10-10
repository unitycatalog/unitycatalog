package io.unitycatalog.spark;

import static io.unitycatalog.spark.auth.AwsVendedTokenProviderTest.newAwsTempCredentials;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.TemporaryCredentialsApi;
import io.unitycatalog.client.model.TableOperation;
import io.unitycatalog.client.model.TemporaryCredentials;
import io.unitycatalog.spark.auth.AwsVendedTokenProvider;
import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;

public class S3CredentialTestFileSystem extends CredentialTestFileSystem {
  private volatile AwsVendedTokenProvider provider;

  @Override
  public void initialize(URI uri, Configuration conf) throws IOException {
    super.initialize(uri, conf);

    if (credentialCheckEnabled) {
      String clazz = conf.get("fs.s3a.aws.credentials.provider");
      assertThat(clazz).isEqualTo(AwsVendedTokenProvider.class.getName());

      conf.set(UCHadoopConf.UC_URI_KEY, "http://localhost:8080");
      conf.set(UCHadoopConf.UC_TOKEN_KEY, "unity-catalog-token");

      // For table-based temporary requests.
      conf.set(UCHadoopConf.UC_CREDENTIALS_TYPE_TABLE_VALUE,
          UCHadoopConf.UC_CREDENTIALS_TYPE_TABLE_VALUE);
      conf.set(UCHadoopConf.UC_TABLE_ID_KEY, "test");
      conf.set(UCHadoopConf.UC_TABLE_OPERATION_KEY, TableOperation.READ.getValue());

      long expirationTime1 = System.currentTimeMillis() + 1000 + 3000 * 1000L;
      TemporaryCredentials cred1 =
          newAwsTempCredentials(
              "temp-accessKeyId1", "temp-secretAccessKey1", "temp-sessionToken1", expirationTime1);

      // Mock the table-based temporary credentials' generation.
      TemporaryCredentialsApi tempCredApi = mock(TemporaryCredentialsApi.class);
      try {
        when(tempCredApi.generateTemporaryTableCredentials(any())).thenReturn(cred1);
      } catch (ApiException e) {
        throw new RuntimeException(e);
      }

      provider = new TestAwsVendedTokenProvider(conf, tempCredApi);
    }
  }


  @Override
  void checkCredentials(Path f) {
    Configuration conf = getConf();
    String host = f.toUri().getHost();
    if (credentialCheckEnabled) {

      assertThat(provider).isNotNull();
      AwsSessionCredentials awsSessionCredentials =
          (AwsSessionCredentials) provider.resolveCredentials();
      assertThat(awsSessionCredentials).isNotNull();
      assertThat(awsSessionCredentials.accessKeyId()).isEqualTo("temp-accessKeyId1");
      assertThat(awsSessionCredentials.secretAccessKey()).isEqualTo("temp-secretAccessKey1");
      assertThat(awsSessionCredentials.sessionToken()).isEqualTo("temp-sessionToken1");

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

  public static class TestAwsVendedTokenProvider extends AwsVendedTokenProvider {
    private final TemporaryCredentialsApi tempCredApi;

    public TestAwsVendedTokenProvider(Configuration conf, TemporaryCredentialsApi tempCredApi) {
      super(conf);
      this.tempCredApi = tempCredApi;
    }

    @Override
    protected TemporaryCredentialsApi temporaryCredentialsApi() {
      return tempCredApi;
    }
  }
}
