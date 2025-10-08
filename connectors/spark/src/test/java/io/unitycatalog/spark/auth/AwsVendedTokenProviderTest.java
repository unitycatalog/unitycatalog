package io.unitycatalog.spark.auth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.unitycatalog.client.api.TemporaryCredentialsApi;
import io.unitycatalog.client.model.PathOperation;
import io.unitycatalog.client.model.TableOperation;
import io.unitycatalog.client.model.TemporaryCredentials;
import io.unitycatalog.spark.UCHadoopConf;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;

public class AwsVendedTokenProviderTest {

  @Test
  public void testConstructor() {
    Configuration conf = new Configuration();

    // Verify the UC URI validation error message.
    assertThatThrownBy(() -> new AwsVendedTokenProvider(null, conf))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("'%s' is not set in hadoop configuration", UCHadoopConf.UC_URI);

    // Verify the UC Token validation error message.
    conf.set(UCHadoopConf.UC_URI, "http://localhost:8080");
    assertThatThrownBy(() -> new AwsVendedTokenProvider(null, conf))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("'%s' is not set in hadoop configuration", UCHadoopConf.UC_TOKEN);
  }

  @Test
  public void testTableTemporaryCredentialsRenew() throws Exception {
    Configuration conf = new Configuration();
    conf.set(UCHadoopConf.UC_URI, "http://localhost:8080");
    conf.set(UCHadoopConf.UC_TOKEN, "unity-catalog-token");

    // For table-based temporary requests.
    conf.set(UCHadoopConf.UC_CREDENTIALS_TYPE, UCHadoopConf.UC_CREDENTIALS_TYPE_TABLE_VALUE);
    conf.set(UCHadoopConf.UC_TABLE_ID, "test");
    conf.set(UCHadoopConf.UC_TABLE_OPERATION, TableOperation.READ.getValue());

    long expirationTime1 = System.currentTimeMillis() + 1000 + 3 * 1000L;
    long expirationTime2 = System.currentTimeMillis() + 1000 + 6 * 1000L;
    TemporaryCredentials cred1 =
        newAwsTempCredentials("accessKeyId1", "secretAccessKey1", "sessionToken1", expirationTime1);
    TemporaryCredentials cred2 =
        newAwsTempCredentials("accessKeyId2", "secretAccessKey2", "sessionToken2", expirationTime2);

    // Mock the table-based temporary credentials' generation.
    TemporaryCredentialsApi tempCredApi = mock(TemporaryCredentialsApi.class);
    when(tempCredApi.generateTemporaryTableCredentials(any())).thenReturn(cred1).thenReturn(cred2);

    AwsVendedTokenProvider provider = new TestAwsVendedTokenProvider(null, conf, tempCredApi);
    provider.setRenewalLeadTime(3 * 1000L);

    // Use the cred1 for the 1st access.
    AwsSessionCredentials awsCred1 = (AwsSessionCredentials) provider.resolveCredentials();
    assertThat(awsCred1.accessKeyId()).isEqualTo("accessKeyId1");
    assertThat(awsCred1.secretAccessKey()).isEqualTo("secretAccessKey1");
    assertThat(awsCred1.sessionToken()).isEqualTo("sessionToken1");

    // Use the cred1 for the 2nd access, since it's valid.
    AwsSessionCredentials awsCred2 = (AwsSessionCredentials) provider.resolveCredentials();
    assertThat(awsCred2.accessKeyId()).isEqualTo("accessKeyId1");
    assertThat(awsCred2.secretAccessKey()).isEqualTo("secretAccessKey1");
    assertThat(awsCred2.sessionToken()).isEqualTo("sessionToken1");

    // Sleep 4 seconds to wait the cred1 to be expired.
    Thread.sleep(4 * 1000L);

    // Use the cred2 for the 3rd access, since cred1 it's expired.
    AwsSessionCredentials awsCred3 = (AwsSessionCredentials) provider.resolveCredentials();
    assertThat(awsCred3.accessKeyId()).isEqualTo("accessKeyId2");
    assertThat(awsCred3.secretAccessKey()).isEqualTo("secretAccessKey2");
    assertThat(awsCred3.sessionToken()).isEqualTo("sessionToken2");

    // Use the cred3 for the 4th access, since cred2 is valid.
    AwsSessionCredentials awsCred4 = (AwsSessionCredentials) provider.resolveCredentials();
    assertThat(awsCred4.accessKeyId()).isEqualTo("accessKeyId2");
    assertThat(awsCred4.secretAccessKey()).isEqualTo("secretAccessKey2");
    assertThat(awsCred4.sessionToken()).isEqualTo("sessionToken2");
  }

  @Test
  public void testTableTemporaryCredentialsRenewWithInitialCredentials() throws Exception {
    Configuration conf = new Configuration();
    conf.set(UCHadoopConf.UC_URI, "http://localhost:8080");
    conf.set(UCHadoopConf.UC_TOKEN, "unity-catalog-token");

    // For table-based temporary requests.
    conf.set(UCHadoopConf.UC_CREDENTIALS_TYPE, UCHadoopConf.UC_CREDENTIALS_TYPE_TABLE_VALUE);
    conf.set(UCHadoopConf.UC_TABLE_ID, "test");
    conf.set(UCHadoopConf.UC_TABLE_OPERATION, TableOperation.READ.getValue());

    // Use the generated credential to initialize the provider.
    conf.set(UCHadoopConf.S3A_INIT_ACCESS_KEY, "accessKeyId0");
    conf.set(UCHadoopConf.S3A_INIT_SECRET_KEY, "secretAccessKey0");
    conf.set(UCHadoopConf.S3A_INIT_SESSION_TOKEN, "sessionToken0");
    conf.setLong(UCHadoopConf.S3A_INIT_CRED_EXPIRED_TIME, System.currentTimeMillis() + 4 * 1000L);

    // Mock the path-based temporary credentials' generation.
    TemporaryCredentialsApi tempCredApi = mock(TemporaryCredentialsApi.class);
    when(tempCredApi.generateTemporaryTableCredentials(any()))
        .thenReturn(
            newAwsTempCredentials(
                "accessKeyId1",
                "secretAccessKey1",
                "sessionToken1",
                System.currentTimeMillis() + 7 * 1000L))
        .thenReturn(
            newAwsTempCredentials(
                "accessKeyId2",
                "secretAccessKey2",
                "sessionToken2",
                System.currentTimeMillis() + 10 * 1000L));

    // Initialize the credential provider.
    AwsVendedTokenProvider provider = new TestAwsVendedTokenProvider(null, conf, tempCredApi);
    provider.setRenewalLeadTime(3 * 1000L);

    // cred0 is valid.
    AwsSessionCredentials awsCred0 = (AwsSessionCredentials) provider.resolveCredentials();
    assertThat(awsCred0.accessKeyId()).isEqualTo("accessKeyId0");
    assertThat(awsCred0.secretAccessKey()).isEqualTo("secretAccessKey0");
    assertThat(awsCred0.sessionToken()).isEqualTo("sessionToken0");

    // cred0 is still valid.
    awsCred0 = (AwsSessionCredentials) provider.resolveCredentials();
    assertThat(awsCred0.accessKeyId()).isEqualTo("accessKeyId0");
    assertThat(awsCred0.secretAccessKey()).isEqualTo("secretAccessKey0");
    assertThat(awsCred0.sessionToken()).isEqualTo("sessionToken0");

    Thread.sleep(3 * 1000L);

    // cred0 is invalid while cred1 is valid.
    AwsSessionCredentials awsCred1 = (AwsSessionCredentials) provider.resolveCredentials();
    assertThat(awsCred1.accessKeyId()).isEqualTo("accessKeyId1");
    assertThat(awsCred1.secretAccessKey()).isEqualTo("secretAccessKey1");
    assertThat(awsCred1.sessionToken()).isEqualTo("sessionToken1");

    // cred1 is still valid.
    awsCred1 = (AwsSessionCredentials) provider.resolveCredentials();
    assertThat(awsCred1.accessKeyId()).isEqualTo("accessKeyId1");
    assertThat(awsCred1.secretAccessKey()).isEqualTo("secretAccessKey1");
    assertThat(awsCred1.sessionToken()).isEqualTo("sessionToken1");

    Thread.sleep(3 * 1000L);

    // cred1 is expired, while cred2 is valid.
    AwsSessionCredentials awsCred2 = (AwsSessionCredentials) provider.resolveCredentials();
    assertThat(awsCred2.accessKeyId()).isEqualTo("accessKeyId2");
    assertThat(awsCred2.secretAccessKey()).isEqualTo("secretAccessKey2");
    assertThat(awsCred2.sessionToken()).isEqualTo("sessionToken2");

    // cred2 is still valid.
    awsCred2 = (AwsSessionCredentials) provider.resolveCredentials();
    assertThat(awsCred2.accessKeyId()).isEqualTo("accessKeyId2");
    assertThat(awsCred2.secretAccessKey()).isEqualTo("secretAccessKey2");
    assertThat(awsCred2.sessionToken()).isEqualTo("sessionToken2");
  }

  @Test
  public void testPathTemporaryCredentialsRenew() throws Exception {
    Configuration conf = new Configuration();
    conf.set(UCHadoopConf.UC_URI, "http://localhost:8080");
    conf.set(UCHadoopConf.UC_TOKEN, "unity-catalog-token");

    // For path-based temporary requests.
    conf.set(UCHadoopConf.UC_CREDENTIALS_TYPE, UCHadoopConf.UC_CREDENTIALS_TYPE_PATH_VALUE);
    conf.set(UCHadoopConf.UC_PATH, "test");
    conf.set(UCHadoopConf.UC_PATH_OPERATION, PathOperation.PATH_READ.getValue());

    long expirationTime1 = System.currentTimeMillis() + 1000 + 3 * 1000L;
    long expirationTime2 = System.currentTimeMillis() + 1000 + 6 * 1000L;
    TemporaryCredentials cred1 =
        newAwsTempCredentials("accessKeyId1", "secretAccessKey1", "sessionToken1", expirationTime1);
    TemporaryCredentials cred2 =
        newAwsTempCredentials("accessKeyId2", "secretAccessKey2", "sessionToken2", expirationTime2);

    // Mock the path-based temporary credentials' generation.
    TemporaryCredentialsApi tempCredApi = mock(TemporaryCredentialsApi.class);
    when(tempCredApi.generateTemporaryPathCredentials(any())).thenReturn(cred1).thenReturn(cred2);

    AwsVendedTokenProvider provider = new TestAwsVendedTokenProvider(null, conf, tempCredApi);
    provider.setRenewalLeadTime(3 * 1000L);

    // Use the cred1 for the 1st access.
    AwsSessionCredentials awsCred1 = (AwsSessionCredentials) provider.resolveCredentials();
    assertThat(awsCred1.accessKeyId()).isEqualTo("accessKeyId1");
    assertThat(awsCred1.secretAccessKey()).isEqualTo("secretAccessKey1");
    assertThat(awsCred1.sessionToken()).isEqualTo("sessionToken1");

    // Use the cred1 for the 2nd access, since it's valid.
    AwsSessionCredentials awsCred2 = (AwsSessionCredentials) provider.resolveCredentials();
    assertThat(awsCred2.accessKeyId()).isEqualTo("accessKeyId1");
    assertThat(awsCred2.secretAccessKey()).isEqualTo("secretAccessKey1");
    assertThat(awsCred2.sessionToken()).isEqualTo("sessionToken1");

    // Sleep 4 seconds to wait the cred1 to be expired.
    Thread.sleep(4 * 1000L);

    // Use the cred2 for the 3rd access, since cred1 it's expired.
    AwsSessionCredentials awsCred3 = (AwsSessionCredentials) provider.resolveCredentials();
    assertThat(awsCred3.accessKeyId()).isEqualTo("accessKeyId2");
    assertThat(awsCred3.secretAccessKey()).isEqualTo("secretAccessKey2");
    assertThat(awsCred3.sessionToken()).isEqualTo("sessionToken2");

    // Use the cred3 for the 4th access, since cred2 is valid.
    AwsSessionCredentials awsCred4 = (AwsSessionCredentials) provider.resolveCredentials();
    assertThat(awsCred4.accessKeyId()).isEqualTo("accessKeyId2");
    assertThat(awsCred4.secretAccessKey()).isEqualTo("secretAccessKey2");
    assertThat(awsCred4.sessionToken()).isEqualTo("sessionToken2");
  }

  @Test
  public void testPathTemporaryCredentialsRenewWithInitialCredentials() throws Exception {
    Configuration conf = new Configuration();
    conf.set(UCHadoopConf.UC_URI, "http://localhost:8080");
    conf.set(UCHadoopConf.UC_TOKEN, "unity-catalog-token");

    // For path-based temporary requests.
    conf.set(UCHadoopConf.UC_CREDENTIALS_TYPE, UCHadoopConf.UC_CREDENTIALS_TYPE_PATH_VALUE);
    conf.set(UCHadoopConf.UC_PATH, "test");
    conf.set(UCHadoopConf.UC_PATH_OPERATION, PathOperation.PATH_READ.getValue());

    // Use the generated credential to initialize the provider.
    conf.set(UCHadoopConf.S3A_INIT_ACCESS_KEY, "accessKeyId0");
    conf.set(UCHadoopConf.S3A_INIT_SECRET_KEY, "secretAccessKey0");
    conf.set(UCHadoopConf.S3A_INIT_SESSION_TOKEN, "sessionToken0");
    conf.setLong(UCHadoopConf.S3A_INIT_CRED_EXPIRED_TIME, System.currentTimeMillis() + 4 * 1000L);

    // Mock the path-based temporary credentials' generation.
    TemporaryCredentialsApi tempCredApi = mock(TemporaryCredentialsApi.class);
    when(tempCredApi.generateTemporaryPathCredentials(any()))
        .thenReturn(
            newAwsTempCredentials(
                "accessKeyId1",
                "secretAccessKey1",
                "sessionToken1",
                System.currentTimeMillis() + 7 * 1000L))
        .thenReturn(
            newAwsTempCredentials(
                "accessKeyId2",
                "secretAccessKey2",
                "sessionToken2",
                System.currentTimeMillis() + 10 * 1000L));

    // Initialize the credential provider.
    AwsVendedTokenProvider provider = new TestAwsVendedTokenProvider(null, conf, tempCredApi);
    provider.setRenewalLeadTime(3 * 1000L);

    // cred0 is valid.
    AwsSessionCredentials awsCred0 = (AwsSessionCredentials) provider.resolveCredentials();
    assertThat(awsCred0.accessKeyId()).isEqualTo("accessKeyId0");
    assertThat(awsCred0.secretAccessKey()).isEqualTo("secretAccessKey0");
    assertThat(awsCred0.sessionToken()).isEqualTo("sessionToken0");

    // cred0 is still valid.
    awsCred0 = (AwsSessionCredentials) provider.resolveCredentials();
    assertThat(awsCred0.accessKeyId()).isEqualTo("accessKeyId0");
    assertThat(awsCred0.secretAccessKey()).isEqualTo("secretAccessKey0");
    assertThat(awsCred0.sessionToken()).isEqualTo("sessionToken0");

    Thread.sleep(3 * 1000L);

    // cred0 is invalid while cred1 is valid.
    AwsSessionCredentials awsCred1 = (AwsSessionCredentials) provider.resolveCredentials();
    assertThat(awsCred1.accessKeyId()).isEqualTo("accessKeyId1");
    assertThat(awsCred1.secretAccessKey()).isEqualTo("secretAccessKey1");
    assertThat(awsCred1.sessionToken()).isEqualTo("sessionToken1");

    // cred1 is still valid.
    awsCred1 = (AwsSessionCredentials) provider.resolveCredentials();
    assertThat(awsCred1.accessKeyId()).isEqualTo("accessKeyId1");
    assertThat(awsCred1.secretAccessKey()).isEqualTo("secretAccessKey1");
    assertThat(awsCred1.sessionToken()).isEqualTo("sessionToken1");

    Thread.sleep(3 * 1000L);

    // cred1 is expired, while cred2 is valid.
    AwsSessionCredentials awsCred2 = (AwsSessionCredentials) provider.resolveCredentials();
    assertThat(awsCred2.accessKeyId()).isEqualTo("accessKeyId2");
    assertThat(awsCred2.secretAccessKey()).isEqualTo("secretAccessKey2");
    assertThat(awsCred2.sessionToken()).isEqualTo("sessionToken2");

    // cred2 is still valid.
    awsCred2 = (AwsSessionCredentials) provider.resolveCredentials();
    assertThat(awsCred2.accessKeyId()).isEqualTo("accessKeyId2");
    assertThat(awsCred2.secretAccessKey()).isEqualTo("secretAccessKey2");
    assertThat(awsCred2.sessionToken()).isEqualTo("sessionToken2");
  }

  private TemporaryCredentials newAwsTempCredentials(
      String accessKeyId, String secretAccessKey, String sessionToken, long expirationTime) {
    io.unitycatalog.client.model.AwsCredentials awsCred =
        new io.unitycatalog.client.model.AwsCredentials();
    awsCred.setAccessKeyId(accessKeyId);
    awsCred.setSecretAccessKey(secretAccessKey);
    awsCred.setSessionToken(sessionToken);

    TemporaryCredentials tempCred = new TemporaryCredentials();

    tempCred.setAwsTempCredentials(awsCred);
    tempCred.setExpirationTime(expirationTime);

    return tempCred;
  }

  static class TestAwsVendedTokenProvider extends AwsVendedTokenProvider {
    private final TemporaryCredentialsApi tempCredApi;

    TestAwsVendedTokenProvider(
        URI ignored, Configuration conf, TemporaryCredentialsApi tempCredApi) {
      super(ignored, conf);
      this.tempCredApi = tempCredApi;
    }

    @Override
    protected TemporaryCredentialsApi temporaryCredentialsApi() {
      return tempCredApi;
    }
  }
}
