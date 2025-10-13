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
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.AWSCredentialProviderList;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.fs.s3a.auth.CredentialProviderListFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;

public class AwsVendedTokenProviderTest {

  @BeforeEach
  public void before() {
    GenericCredentialProvider.globalCache.invalidateAll();
  }

  @AfterEach
  public void after() {
    GenericCredentialProvider.globalCache.invalidateAll();
  }

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
    Configuration conf = newTableBasedConf("unity-catalog-table");
    conf.set(Constants.AWS_CREDENTIALS_PROVIDER, AwsVendedTokenProvider.class.getName());

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

    TemporaryCredentials cred1 = newAwsTempCred("1", System.currentTimeMillis() + 2000L);
    TemporaryCredentials cred2 = newAwsTempCred("2", System.currentTimeMillis() + 3000L);

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
    TemporaryCredentials cred0 = newAwsTempCred("0", System.currentTimeMillis() + 2000L);
    setInitialCredentials(conf, cred0);

    // Mock the path-based temporary credentials' generation.
    TemporaryCredentialsApi tempCredApi = mock(TemporaryCredentialsApi.class);
    TemporaryCredentials cred1 = newAwsTempCred("1", System.currentTimeMillis() + 3000L);
    TemporaryCredentials cred2 = newAwsTempCred("2", System.currentTimeMillis() + 4000L);

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
    TemporaryCredentials cred1 = newAwsTempCred("1", expirationTime1);
    TemporaryCredentials cred2 = newAwsTempCred("2", expirationTime2);

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
    TemporaryCredentials cred0 = newAwsTempCred("0", System.currentTimeMillis() + 2000L);
    setInitialCredentials(conf, cred0);

    // Mock the path-based temporary credentials' generation.
    TemporaryCredentialsApi tempCredApi = mock(TemporaryCredentialsApi.class);
    TemporaryCredentials cred1 = newAwsTempCred("1", System.currentTimeMillis() + 3000L);
    TemporaryCredentials cred2 = newAwsTempCred("2", System.currentTimeMillis() + 4000L);
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

  @Test
  public void testGlobalCredCache() throws Exception {
    Configuration confA = newTableBasedConf("tableA");
    Configuration confB = newTableBasedConf("tableB");
    Configuration confC = newPathBasedConf("pathC");
    Configuration confD = newPathBasedConf("pathD");

    TemporaryCredentialsApi tempCredApi = mock(TemporaryCredentialsApi.class);
    // For TableA's 1st renewal
    TemporaryCredentials cred1 = newAwsTempCred("1", System.currentTimeMillis() + 2000L);
    // For TableB's 1st renewal
    TemporaryCredentials cred2 = newAwsTempCred("2", System.currentTimeMillis() + 2000L);
    // For PathC's 1st renewal
    TemporaryCredentials cred3 = newAwsTempCred("3", System.currentTimeMillis() + 2000L);
    // For PathD's 1st renewal
    TemporaryCredentials cred4 = newAwsTempCred("4", System.currentTimeMillis() + 2000L);
    // For TableA's 2nd renewal
    TemporaryCredentials cred5 = newAwsTempCred("5", System.currentTimeMillis() + 3000L);
    // For TableB's 2nd renewal
    TemporaryCredentials cred6 = newAwsTempCred("6", System.currentTimeMillis() + 3000L);
    // For PathC's 2nd renewal
    TemporaryCredentials cred7 = newAwsTempCred("7", System.currentTimeMillis() + 3000L);
    // For PathD's 2nd renewal
    TemporaryCredentials cred8 = newAwsTempCred("8", System.currentTimeMillis() + 3000L);
    when(tempCredApi.generateTemporaryTableCredentials(any()))
        .thenReturn(cred1)
        .thenReturn(cred2)
        .thenReturn(cred5)
        .thenReturn(cred6);
    when(tempCredApi.generateTemporaryPathCredentials(any()))
        .thenReturn(cred3)
        .thenReturn(cred4)
        .thenReturn(cred7)
        .thenReturn(cred8);

    AwsVendedTokenProvider providerA = new TestAwsVendedTokenProvider(confA, tempCredApi);
    providerA.setRenewalLeadTimeMillis(1000L);

    AwsVendedTokenProvider providerB = new TestAwsVendedTokenProvider(confB, tempCredApi);
    providerB.setRenewalLeadTimeMillis(1000L);

    AwsVendedTokenProvider providerC = new TestAwsVendedTokenProvider(confC, tempCredApi);
    providerC.setRenewalLeadTimeMillis(1000L);

    AwsVendedTokenProvider providerD = new TestAwsVendedTokenProvider(confD, tempCredApi);
    providerD.setRenewalLeadTimeMillis(1000L);

    // TableA: 1st access.
    assertCredentials(providerA.resolveCredentials(), cred1);
    assertGlobalCache(1, cred1);

    // TableB: 1st access.
    assertCredentials(providerB.resolveCredentials(), cred2);
    assertGlobalCache(2, cred1, cred2);

    // PathC: 1st access.
    assertCredentials(providerC.resolveCredentials(), cred3);
    assertGlobalCache(3, cred1, cred2, cred3);

    // PathD: 1st access.
    assertCredentials(providerD.resolveCredentials(), cred4);
    assertGlobalCache(4, cred1, cred2, cred3, cred4);

    // TableA: 2nd access.
    assertCredentials(providerA.resolveCredentials(), cred1);
    assertGlobalCache(4, cred1, cred2, cred3, cred4);

    // TableB: 2nd access.
    assertCredentials(providerB.resolveCredentials(), cred2);
    assertGlobalCache(4, cred1, cred2, cred3, cred4);

    // PathC: 2nd access.
    assertCredentials(providerC.resolveCredentials(), cred3);
    assertGlobalCache(4, cred1, cred2, cred3, cred4);

    // PathD: 2nd access.
    assertCredentials(providerC.resolveCredentials(), cred3);
    assertGlobalCache(4, cred1, cred2, cred3, cred4);

    Thread.sleep(1000L);

    // TableA: 3rd access. renew cred1 to cred5.
    assertCredentials(providerA.resolveCredentials(), cred5);
    assertGlobalCache(4, cred5, cred2, cred3, cred4);

    // TableB: 3rd access. renew cred2 to cred6.
    assertCredentials(providerB.resolveCredentials(), cred6);
    assertGlobalCache(4, cred5, cred6, cred3, cred4);

    // PathC: 3rd access. renew cred3 to cred7.
    assertCredentials(providerC.resolveCredentials(), cred7);
    assertGlobalCache(4, cred5, cred6, cred7, cred4);

    // PathD: 3rd access. renew cred4 to cred8.
    assertCredentials(providerD.resolveCredentials(), cred8);
    assertGlobalCache(4, cred5, cred6, cred7, cred8);
  }

  private static Configuration newTableBasedConf(String tableId) {
    Configuration conf = new Configuration();
    conf.set(UCHadoopConf.UC_URI_KEY, "http://localhost:8080");
    conf.set(UCHadoopConf.UC_TOKEN_KEY, "unity-catalog-token");
    conf.set(UCHadoopConf.UC_CREDENTIALS_UID_KEY, UUID.randomUUID().toString());

    // For table-based temporary requests.
    conf.set(UCHadoopConf.UC_CREDENTIALS_TYPE_KEY, UCHadoopConf.UC_CREDENTIALS_TYPE_TABLE_VALUE);
    conf.set(UCHadoopConf.UC_TABLE_ID_KEY, tableId);
    conf.set(UCHadoopConf.UC_TABLE_OPERATION_KEY, TableOperation.READ.getValue());

    return conf;
  }

  private static Configuration newTableBasedConf() {
    return newTableBasedConf("testTableId");
  }

  private static Configuration newPathBasedConf(String path) {
    Configuration conf = new Configuration();
    conf.set(UCHadoopConf.UC_URI_KEY, "http://localhost:8080");
    conf.set(UCHadoopConf.UC_TOKEN_KEY, "unity-catalog-token");
    conf.set(UCHadoopConf.UC_CREDENTIALS_UID_KEY, UUID.randomUUID().toString());

    // For path-based temporary requests.
    conf.set(UCHadoopConf.UC_CREDENTIALS_TYPE_KEY, UCHadoopConf.UC_CREDENTIALS_TYPE_PATH_VALUE);
    conf.set(UCHadoopConf.UC_PATH_KEY, path);
    conf.set(UCHadoopConf.UC_PATH_OPERATION_KEY, PathOperation.PATH_READ.getValue());

    return conf;
  }

  private static Configuration newPathBasedConf() {
    return newPathBasedConf("path");
  }

  private static void setInitialCredentials(Configuration conf, TemporaryCredentials cred) {
    conf.set(UCHadoopConf.S3A_INIT_ACCESS_KEY, cred.getAwsTempCredentials().getAccessKeyId());
    conf.set(UCHadoopConf.S3A_INIT_SECRET_KEY, cred.getAwsTempCredentials().getSecretAccessKey());
    conf.set(UCHadoopConf.S3A_INIT_SESSION_TOKEN, cred.getAwsTempCredentials().getSessionToken());
    conf.setLong(UCHadoopConf.S3A_INIT_CRED_EXPIRED_TIME, cred.getExpirationTime());
  }

  private static void assertGlobalCache(int expectedSize, TemporaryCredentials... creds) {
    assertThat(expectedSize).isEqualTo(creds.length);
    assertThat(AwsVendedTokenProvider.globalCache.size()).isEqualTo(expectedSize);
    for (TemporaryCredentials cred : creds) {
      assertThat(AwsVendedTokenProvider.globalCache.asMap().values())
          .contains(new GenericCredential(cred));
    }
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

  private static TemporaryCredentials newAwsTempCred(String idSuffix, long expirationTimeMs) {
    return newAwsTempCred(
        "accessKeyId" + idSuffix,
        "secretAccessKey" + idSuffix,
        "sessionToken" + idSuffix,
        expirationTimeMs);
  }

  private static TemporaryCredentials newAwsTempCred(
      String accessKeyId, String secretAccessKey, String sessionToken, long expirationTimeMs) {
    io.unitycatalog.client.model.AwsCredentials awsCred =
        new io.unitycatalog.client.model.AwsCredentials();
    awsCred.setAccessKeyId(accessKeyId);
    awsCred.setSecretAccessKey(secretAccessKey);
    awsCred.setSessionToken(sessionToken);

    TemporaryCredentials tempCred = new TemporaryCredentials();

    tempCred.setAwsTempCredentials(awsCred);
    tempCred.setExpirationTime(expirationTimeMs);

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
