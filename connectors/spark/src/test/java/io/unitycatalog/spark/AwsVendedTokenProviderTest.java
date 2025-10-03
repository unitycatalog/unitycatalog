package io.unitycatalog.spark;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.unitycatalog.client.api.TemporaryCredentialsApi;
import io.unitycatalog.client.model.PathOperation;
import io.unitycatalog.client.model.TableOperation;
import io.unitycatalog.client.model.TemporaryCredentials;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;

public class AwsVendedTokenProviderTest {

  @Test
  public void testTableBasedTemporaryCredentialsRenew() throws Exception {
    Configuration conf = new Configuration();
    conf.set(Constants.UNITY_CATALOG_URI, "http://localhost:8080");
    conf.set(Constants.UNITY_CATALOG_TOKEN, "unity-catalog-token");

    // For table-based temporary requests.
    conf.set(
        Constants.UNITY_CATALOG_CREDENTIALS_TYPE, Constants.UNITY_CATALOG_TABLE_CREDENTIALS_TYPE);
    conf.set(Constants.UNITY_CATALOG_TABLE, "test");
    conf.set(Constants.UNITY_CATALOG_TABLE_OPERATION, TableOperation.READ.getValue());

    long expirationTime1 = System.currentTimeMillis() + 1000 + 3 * 1000L;
    long expirationTime2 = System.currentTimeMillis() + 1000 + 6 * 1000L;
    TemporaryCredentials cred1 =
        initAwsTempCredentials(
            "accessKeyId1", "secretAccessKey1", "sessionToken1", expirationTime1);
    TemporaryCredentials cred2 =
        initAwsTempCredentials(
            "accessKeyId2", "secretAccessKey2", "sessionToken2", expirationTime2);

    // Mock the table-based temporary credentials' generation.
    TemporaryCredentialsApi tempCredApi = mock(TemporaryCredentialsApi.class);
    when(tempCredApi.generateTemporaryTableCredentials(any())).thenReturn(cred1).thenReturn(cred2);

    testCredentialsRenew(conf, tempCredApi);
  }

  @Test
  public void testPathBasedTemporaryCredentialsRenew() throws Exception {
    Configuration conf = new Configuration();
    conf.set(Constants.UNITY_CATALOG_URI, "http://localhost:8080");
    conf.set(Constants.UNITY_CATALOG_TOKEN, "unity-catalog-token");

    // For path-based temporary requests.
    conf.set(
        Constants.UNITY_CATALOG_CREDENTIALS_TYPE, Constants.UNITY_CATALOG_PATH_CREDENTIALS_TYPE);
    conf.set(Constants.UNITY_CATALOG_PATH, "test");
    conf.set(Constants.UNITY_CATALOG_PATH_OPERATION, PathOperation.PATH_READ.getValue());

    long expirationTime1 = System.currentTimeMillis() + 1000 + 3 * 1000L;
    long expirationTime2 = System.currentTimeMillis() + 1000 + 6 * 1000L;
    TemporaryCredentials cred1 =
        initAwsTempCredentials(
            "accessKeyId1", "secretAccessKey1", "sessionToken1", expirationTime1);
    TemporaryCredentials cred2 =
        initAwsTempCredentials(
            "accessKeyId2", "secretAccessKey2", "sessionToken2", expirationTime2);

    // Mock the path-based temporary credentials' generation.
    TemporaryCredentialsApi tempCredApi = mock(TemporaryCredentialsApi.class);
    when(tempCredApi.generateTemporaryPathCredentials(any())).thenReturn(cred1).thenReturn(cred2);

    testCredentialsRenew(conf, tempCredApi);
  }

  public void testCredentialsRenew(Configuration conf, TemporaryCredentialsApi tempCredApi)
      throws Exception {
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

  TemporaryCredentials initAwsTempCredentials(
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

    public TestAwsVendedTokenProvider(
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
