package io.unitycatalog.spark.auth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.unitycatalog.client.api.TemporaryCredentialsApi;
import io.unitycatalog.client.model.AwsCredentials;
import io.unitycatalog.client.model.PathOperation;
import io.unitycatalog.client.model.TableOperation;
import io.unitycatalog.client.model.TemporaryCredentials;
import io.unitycatalog.spark.UCHadoopConf;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.AWSCredentialProviderList;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.fs.s3a.auth.CredentialProviderListFactory;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;

public class AwsVendedTokenProviderTest {

  @Test
  public void testConstructor() {
    Configuration conf = new Configuration();

    // Verify the UC URI validation error message.
    assertThatThrownBy(() -> new AwsVendedTokenProvider(conf))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("'%s' is not set in hadoop configuration", UCHadoopConf.UC_URI_KEY);

    // Verify the UC Token validation error message.
    conf.set(UCHadoopConf.UC_URI_KEY, "http://localhost:8080");
    assertThatThrownBy(() -> new AwsVendedTokenProvider(conf))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("'%s' is not set in hadoop configuration", UCHadoopConf.UC_TOKEN_KEY);
  }

  @Test
  public void testLoadingTokenProvider() throws IOException {
    Configuration conf = new Configuration();
    conf.set(Constants.AWS_CREDENTIALS_PROVIDER, AwsVendedTokenProvider.class.getName());
    conf.set(UCHadoopConf.UC_URI_KEY, "http://localhost:8080");
    conf.set(UCHadoopConf.UC_TOKEN_KEY, "unity-catalog-token");

    AWSCredentialProviderList list =
        CredentialProviderListFactory.buildAWSProviderList(
            URI.create("s3://bucket/key"),
            conf,
            Constants.AWS_CREDENTIALS_PROVIDER,
            new ArrayList<>(),
            new HashSet<>());

    List<AwsCredentialsProvider> providers = list.getProviders();
    assertThat(providers).hasSize(1);
    assertThat(providers.get(0)).isInstanceOf(AwsVendedTokenProvider.class);
  }

  @Test
  public void testTableTemporaryCredentialsRenew() throws Exception {
    Configuration conf = newTableBasedConf();

    long expirationTime1 = System.currentTimeMillis() + 1000 + 1000L;
    long expirationTime2 = System.currentTimeMillis() + 1000 + 2000L;
    TemporaryCredentials cred1 =
        newAwsTempCredentials("accessKeyId1", "secretAccessKey1", "sessionToken1", expirationTime1);
    TemporaryCredentials cred2 =
        newAwsTempCredentials("accessKeyId2", "secretAccessKey2", "sessionToken2", expirationTime2);

    // Mock the table-based temporary credentials' generation.
    TemporaryCredentialsApi tempCredApi = mock(TemporaryCredentialsApi.class);
    when(tempCredApi.generateTemporaryTableCredentials(any())).thenReturn(cred1).thenReturn(cred2);

    AwsVendedTokenProvider provider = new TestAwsVendedTokenProvider(conf, tempCredApi);
    provider.setRenewalLeadTimeMillis(1000L);

    // Use the cred1 for the 1st access.
    assertCredentials(provider.resolveCredentials(), cred1);

    // Use the cred1 for the 2nd access, since it's valid.
    assertCredentials(provider.resolveCredentials(), cred1);

    // Sleep to renew, cred2 will be valid.
    Thread.sleep(1000L);

    // Use the cred2 for the 3rd access, since cred1 it's expired.
    assertCredentials(provider.resolveCredentials(), cred2);

    // Use the cred3 for the 4th access, since cred2 is valid.
    assertCredentials(provider.resolveCredentials(), cred2);
  }

  @Test
  public void testTableTemporaryCredentialsRenewWithInitialCredentials() throws Exception {
    Configuration conf = newTableBasedConf();

    // Use the generated credential to initialize the provider.
    TemporaryCredentials cred0 =
        newAwsTempCredentials(
            "accessKeyId0",
            "secretAccessKey0",
            "sessionToken0",
            System.currentTimeMillis() + 2000L);
    setInitialCredentials(conf, cred0);

    // Mock the path-based temporary credentials' generation.
    TemporaryCredentialsApi tempCredApi = mock(TemporaryCredentialsApi.class);
    TemporaryCredentials cred1 =
        newAwsTempCredentials(
            "accessKeyId1",
            "secretAccessKey1",
            "sessionToken1",
            System.currentTimeMillis() + 3000L);
    TemporaryCredentials cred2 =
        newAwsTempCredentials(
            "accessKeyId2",
            "secretAccessKey2",
            "sessionToken2",
            System.currentTimeMillis() + 4000L);

    when(tempCredApi.generateTemporaryTableCredentials(any())).thenReturn(cred1).thenReturn(cred2);

    // Initialize the credential provider.
    AwsVendedTokenProvider provider = new TestAwsVendedTokenProvider(conf, tempCredApi);
    provider.setRenewalLeadTimeMillis(1000L);

    // cred0 is valid.
    assertCredentials(provider.resolveCredentials(), cred0);

    // cred0 is still valid.
    assertCredentials(provider.resolveCredentials(), cred0);

    Thread.sleep(1000L);

    // cred0 is invalid while cred1 is valid.
    assertCredentials(provider.resolveCredentials(), cred1);

    // cred1 is still valid.
    assertCredentials(provider.resolveCredentials(), cred1);

    Thread.sleep(1000L);

    // cred1 is expired, while cred2 is valid.
    assertCredentials(provider.resolveCredentials(), cred2);

    // cred2 is still valid.
    assertCredentials(provider.resolveCredentials(), cred2);
  }

  @Test
  public void testPathTemporaryCredentialsRenew() throws Exception {
    Configuration conf = newPathBasedConf();

    long expirationTime1 = System.currentTimeMillis() + 2000L;
    long expirationTime2 = System.currentTimeMillis() + 3000L;
    TemporaryCredentials cred1 =
        newAwsTempCredentials("accessKeyId1", "secretAccessKey1", "sessionToken1", expirationTime1);
    TemporaryCredentials cred2 =
        newAwsTempCredentials("accessKeyId2", "secretAccessKey2", "sessionToken2", expirationTime2);

    // Mock the path-based temporary credentials' generation.
    TemporaryCredentialsApi tempCredApi = mock(TemporaryCredentialsApi.class);
    when(tempCredApi.generateTemporaryPathCredentials(any())).thenReturn(cred1).thenReturn(cred2);

    AwsVendedTokenProvider provider = new TestAwsVendedTokenProvider(conf, tempCredApi);
    provider.setRenewalLeadTimeMillis(1000L);

    // Use the cred1 for the 1st access.
    assertCredentials(provider.resolveCredentials(), cred1);

    // Use the cred1 for the 2nd access, since it's valid.
    assertCredentials(provider.resolveCredentials(), cred1);

    // Sleep to renew.
    Thread.sleep(1000L);

    // Use the cred2 for the 3rd access, since cred1 it's expired.
    assertCredentials(provider.resolveCredentials(), cred2);

    // Use the cred3 for the 4th access, since cred2 is valid.
    assertCredentials(provider.resolveCredentials(), cred2);
  }

  @Test
  public void testPathTemporaryCredentialsRenewWithInitialCredentials() throws Exception {
    Configuration conf = newPathBasedConf();

    // Use the generated credential to initialize the provider.
    TemporaryCredentials cred0 =
        newAwsTempCredentials(
            "accessKeyId0",
            "secretAccessKey0",
            "sessionToken0",
            System.currentTimeMillis() + 2000L);
    setInitialCredentials(conf, cred0);

    // Mock the path-based temporary credentials' generation.
    TemporaryCredentialsApi tempCredApi = mock(TemporaryCredentialsApi.class);
    TemporaryCredentials cred1 =
        newAwsTempCredentials(
            "accessKeyId1",
            "secretAccessKey1",
            "sessionToken1",
            System.currentTimeMillis() + 3000L);
    TemporaryCredentials cred2 =
        newAwsTempCredentials(
            "accessKeyId2",
            "secretAccessKey2",
            "sessionToken2",
            System.currentTimeMillis() + 4000L);
    when(tempCredApi.generateTemporaryPathCredentials(any())).thenReturn(cred1).thenReturn(cred2);

    // Initialize the credential provider.
    AwsVendedTokenProvider provider = new TestAwsVendedTokenProvider(conf, tempCredApi);
    provider.setRenewalLeadTimeMillis(1000L);

    // cred0 is valid.
    assertCredentials(provider.resolveCredentials(), cred0);

    // cred0 is still valid.
    assertCredentials(provider.resolveCredentials(), cred0);

    Thread.sleep(1000L);

    // cred0 is invalid while cred1 is valid.
    assertCredentials(provider.resolveCredentials(), cred1);

    // cred1 is still valid.
    assertCredentials(provider.resolveCredentials(), cred1);

    Thread.sleep(1000L);

    // cred1 is expired, while cred2 is valid.
    assertCredentials(provider.resolveCredentials(), cred2);

    // cred2 is still valid.
    assertCredentials(provider.resolveCredentials(), cred2);
  }

  private static Configuration newTableBasedConf() {
    Configuration conf = new Configuration();
    conf.set(UCHadoopConf.UC_URI_KEY, "http://localhost:8080");
    conf.set(UCHadoopConf.UC_TOKEN_KEY, "unity-catalog-token");

    // For table-based temporary requests.
    conf.set(UCHadoopConf.UC_CREDENTIALS_TYPE_KEY, UCHadoopConf.UC_CREDENTIALS_TYPE_TABLE_VALUE);
    conf.set(UCHadoopConf.UC_TABLE_ID_KEY, "test");
    conf.set(UCHadoopConf.UC_TABLE_OPERATION_KEY, TableOperation.READ.getValue());

    return conf;
  }

  private static Configuration newPathBasedConf() {
    Configuration conf = new Configuration();
    conf.set(UCHadoopConf.UC_URI_KEY, "http://localhost:8080");
    conf.set(UCHadoopConf.UC_TOKEN_KEY, "unity-catalog-token");

    // For path-based temporary requests.
    conf.set(UCHadoopConf.UC_CREDENTIALS_TYPE_KEY, UCHadoopConf.UC_CREDENTIALS_TYPE_PATH_VALUE);
    conf.set(UCHadoopConf.UC_PATH_KEY, "test");
    conf.set(UCHadoopConf.UC_PATH_OPERATION_KEY, PathOperation.PATH_READ.getValue());

    return conf;
  }

  private static void setInitialCredentials(Configuration conf, TemporaryCredentials cred) {
    conf.set(UCHadoopConf.S3A_INIT_ACCESS_KEY, cred.getAwsTempCredentials().getAccessKeyId());
    conf.set(UCHadoopConf.S3A_INIT_SECRET_KEY, cred.getAwsTempCredentials().getSecretAccessKey());
    conf.set(UCHadoopConf.S3A_INIT_SESSION_TOKEN, cred.getAwsTempCredentials().getSessionToken());
    conf.setLong(UCHadoopConf.S3A_INIT_CRED_EXPIRED_TIME, cred.getExpirationTime());
  }

  private static void assertCredentials(
      software.amazon.awssdk.auth.credentials.AwsCredentials actual,
      TemporaryCredentials expected) {
    assertThat(actual).isInstanceOf(AwsSessionCredentials.class);
    AwsSessionCredentials actualSessionCred = (AwsSessionCredentials) actual;

    assertThat(expected.getAwsTempCredentials()).isNotNull();
    AwsCredentials expectedAwsCred = expected.getAwsTempCredentials();

    assertThat(actualSessionCred.accessKeyId()).isEqualTo(expectedAwsCred.getAccessKeyId());
    assertThat(actualSessionCred.secretAccessKey()).isEqualTo(expectedAwsCred.getSecretAccessKey());
    assertThat(actualSessionCred.sessionToken()).isEqualTo(expectedAwsCred.getSessionToken());
  }

  private static TemporaryCredentials newAwsTempCredentials(
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

    TestAwsVendedTokenProvider(Configuration conf, TemporaryCredentialsApi tempCredApi) {
      super(conf);
      this.tempCredApi = tempCredApi;
    }

    @Override
    protected TemporaryCredentialsApi temporaryCredentialsApi() {
      return tempCredApi;
    }
  }
}
