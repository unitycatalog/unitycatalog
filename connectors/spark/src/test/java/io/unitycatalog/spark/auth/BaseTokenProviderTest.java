package io.unitycatalog.spark.auth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.unitycatalog.client.api.TemporaryCredentialsApi;
import io.unitycatalog.client.model.PathOperation;
import io.unitycatalog.client.model.TableOperation;
import io.unitycatalog.client.model.TemporaryCredentials;
import io.unitycatalog.spark.UCHadoopConf;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public abstract class BaseTokenProviderTest<T extends GenericCredentialProvider> {

  /** Use the {@link Configuration} and the mocked api to create a new provider. */
  protected abstract T createTestProvider(Configuration conf, TemporaryCredentialsApi mockApi);

  /** New a testing temporary credentials, using the id and expiration time. */
  protected abstract TemporaryCredentials newTempCred(String id, long expirationMillis);

  /** Set the credentials into the hadoop conf, as the initialized credential. */
  protected abstract void setInitialCred(Configuration conf, TemporaryCredentials cred);

  /** Use the provider to resolve the last credential, and assert it's the expected one. */
  protected abstract void assertCred(T provider, TemporaryCredentials expected);

  @BeforeEach
  public void before() {
    GenericCredentialProvider.globalCache.invalidateAll();
  }

  @AfterEach
  public void after() {
    GenericCredentialProvider.globalCache.invalidateAll();
  }

  @Test
  public void testTableTemporaryCredentialsRenew() throws Exception {
    Configuration conf = newTableBasedConf();

    TemporaryCredentials cred1 = newTempCred("1", System.currentTimeMillis() + 2000L);
    TemporaryCredentials cred2 = newTempCred("2", System.currentTimeMillis() + 3000L);

    // Mock the table-based temporary credentials' generation.
    TemporaryCredentialsApi tempCredApi = mock(TemporaryCredentialsApi.class);
    when(tempCredApi.generateTemporaryTableCredentials(any())).thenReturn(cred1).thenReturn(cred2);

    T provider = createTestProvider(conf, tempCredApi);
    provider.setRenewalLeadTimeMillis(1000L);

    // Use the cred1 for the 1st access.
    assertCred(provider, cred1);

    // Use the cred1 for the 2nd access, since it's valid.
    assertCred(provider, cred1);

    // Sleep to renew, cred2 will be valid.
    Thread.sleep(1000L);

    // Use the cred2 for the 3rd access, since cred1 it's expired.
    assertCred(provider, cred2);

    // Use the cred3 for the 4th access, since cred2 is valid.
    assertCred(provider, cred2);
  }

  @Test
  public void testTableTemporaryCredentialsRenewWithInitialCredentials() throws Exception {
    Configuration conf = newTableBasedConf();

    // Use the generated credential to initialize the provider.
    TemporaryCredentials cred0 = newTempCred("0", System.currentTimeMillis() + 2000L);
    setInitialCred(conf, cred0);

    // Mock the path-based temporary credentials' generation.
    TemporaryCredentialsApi tempCredApi = mock(TemporaryCredentialsApi.class);
    TemporaryCredentials cred1 = newTempCred("1", System.currentTimeMillis() + 3000L);
    TemporaryCredentials cred2 = newTempCred("2", System.currentTimeMillis() + 4000L);

    when(tempCredApi.generateTemporaryTableCredentials(any())).thenReturn(cred1).thenReturn(cred2);

    // Initialize the credential provider.
    T provider = createTestProvider(conf, tempCredApi);
    provider.setRenewalLeadTimeMillis(1000L);

    // cred0 is valid.
    assertCred(provider, cred0);

    // cred0 is still valid.
    assertCred(provider, cred0);

    Thread.sleep(1000L);

    // cred0 is invalid while cred1 is valid.
    assertCred(provider, cred1);

    // cred1 is still valid.
    assertCred(provider, cred1);

    Thread.sleep(1000L);

    // cred1 is expired, while cred2 is valid.
    assertCred(provider, cred2);

    // cred2 is still valid.
    assertCred(provider, cred2);
  }

  @Test
  public void testPathTemporaryCredentialsRenew() throws Exception {
    Configuration conf = newPathBasedConf();

    long expirationTime1 = System.currentTimeMillis() + 2000L;
    long expirationTime2 = System.currentTimeMillis() + 3000L;
    TemporaryCredentials cred1 = newTempCred("1", expirationTime1);
    TemporaryCredentials cred2 = newTempCred("2", expirationTime2);

    // Mock the path-based temporary credentials' generation.
    TemporaryCredentialsApi tempCredApi = mock(TemporaryCredentialsApi.class);
    when(tempCredApi.generateTemporaryPathCredentials(any())).thenReturn(cred1).thenReturn(cred2);

    T provider = createTestProvider(conf, tempCredApi);
    provider.setRenewalLeadTimeMillis(1000L);

    // Use the cred1 for the 1st access.
    assertCred(provider, cred1);

    // Use the cred1 for the 2nd access, since it's valid.
    assertCred(provider, cred1);

    // Sleep to renew.
    Thread.sleep(1000L);

    // Use the cred2 for the 3rd access, since cred1 it's expired.
    assertCred(provider, cred2);

    // Use the cred3 for the 4th access, since cred2 is valid.
    assertCred(provider, cred2);
  }

  @Test
  public void testPathTemporaryCredentialsRenewWithInitialCredentials() throws Exception {
    Configuration conf = newPathBasedConf();

    // Use the generated credential to initialize the provider.
    TemporaryCredentials cred0 = newTempCred("0", System.currentTimeMillis() + 2000L);
    setInitialCred(conf, cred0);

    // Mock the path-based temporary credentials' generation.
    TemporaryCredentialsApi tempCredApi = mock(TemporaryCredentialsApi.class);
    TemporaryCredentials cred1 = newTempCred("1", System.currentTimeMillis() + 3000L);
    TemporaryCredentials cred2 = newTempCred("2", System.currentTimeMillis() + 4000L);
    when(tempCredApi.generateTemporaryPathCredentials(any())).thenReturn(cred1).thenReturn(cred2);

    // Initialize the credential provider.
    T provider = createTestProvider(conf, tempCredApi);
    provider.setRenewalLeadTimeMillis(1000L);

    // cred0 is valid.
    assertCred(provider, cred0);

    // cred0 is still valid.
    assertCred(provider, cred0);

    Thread.sleep(1000L);

    // cred0 is invalid while cred1 is valid.
    assertCred(provider, cred1);

    // cred1 is still valid.
    assertCred(provider, cred1);

    Thread.sleep(1000L);

    // cred1 is expired, while cred2 is valid.
    assertCred(provider, cred2);

    // cred2 is still valid.
    assertCred(provider, cred2);
  }

  @Test
  public void testGlobalCredCache() throws Exception {
    Configuration confA = newTableBasedConf("tableA");
    Configuration confB = newTableBasedConf("tableB");
    Configuration confC = newPathBasedConf("pathC");
    Configuration confD = newPathBasedConf("pathD");

    TemporaryCredentialsApi tempCredApi = mock(TemporaryCredentialsApi.class);
    // For TableA's 1st renewal
    TemporaryCredentials cred1 = newTempCred("1", System.currentTimeMillis() + 2000L);
    // For TableB's 1st renewal
    TemporaryCredentials cred2 = newTempCred("2", System.currentTimeMillis() + 2000L);
    // For PathC's 1st renewal
    TemporaryCredentials cred3 = newTempCred("3", System.currentTimeMillis() + 2000L);
    // For PathD's 1st renewal
    TemporaryCredentials cred4 = newTempCred("4", System.currentTimeMillis() + 2000L);
    // For TableA's 2nd renewal
    TemporaryCredentials cred5 = newTempCred("5", System.currentTimeMillis() + 3000L);
    // For TableB's 2nd renewal
    TemporaryCredentials cred6 = newTempCred("6", System.currentTimeMillis() + 3000L);
    // For PathC's 2nd renewal
    TemporaryCredentials cred7 = newTempCred("7", System.currentTimeMillis() + 3000L);
    // For PathD's 2nd renewal
    TemporaryCredentials cred8 = newTempCred("8", System.currentTimeMillis() + 3000L);
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

    T providerA = createTestProvider(confA, tempCredApi);
    providerA.setRenewalLeadTimeMillis(1000L);

    T providerB = createTestProvider(confB, tempCredApi);
    providerB.setRenewalLeadTimeMillis(1000L);

    T providerC = createTestProvider(confC, tempCredApi);
    providerC.setRenewalLeadTimeMillis(1000L);

    T providerD = createTestProvider(confD, tempCredApi);
    providerD.setRenewalLeadTimeMillis(1000L);

    // TableA: 1st access.
    assertCred(providerA, cred1);
    assertGlobalCache(1, cred1);

    // TableB: 1st access.
    assertCred(providerB, cred2);
    assertGlobalCache(2, cred1, cred2);

    // PathC: 1st access.
    assertCred(providerC, cred3);
    assertGlobalCache(3, cred1, cred2, cred3);

    // PathD: 1st access.
    assertCred(providerD, cred4);
    assertGlobalCache(4, cred1, cred2, cred3, cred4);

    // TableA: 2nd access.
    assertCred(providerA, cred1);
    assertGlobalCache(4, cred1, cred2, cred3, cred4);

    // TableB: 2nd access.
    assertCred(providerB, cred2);
    assertGlobalCache(4, cred1, cred2, cred3, cred4);

    // PathC: 2nd access.
    assertCred(providerC, cred3);
    assertGlobalCache(4, cred1, cred2, cred3, cred4);

    // PathD: 2nd access.
    assertCred(providerC, cred3);
    assertGlobalCache(4, cred1, cred2, cred3, cred4);

    Thread.sleep(1000L);

    // TableA: 3rd access. renew cred1 to cred5.
    assertCred(providerA, cred5);
    assertGlobalCache(4, cred5, cred2, cred3, cred4);

    // TableB: 3rd access. renew cred2 to cred6.
    assertCred(providerB, cred6);
    assertGlobalCache(4, cred5, cred6, cred3, cred4);

    // PathC: 3rd access. renew cred3 to cred7.
    assertCred(providerC, cred7);
    assertGlobalCache(4, cred5, cred6, cred7, cred4);

    // PathD: 3rd access. renew cred4 to cred8.
    assertCred(providerD, cred8);
    assertGlobalCache(4, cred5, cred6, cred7, cred8);
  }

  private static void assertGlobalCache(int expectedSize, TemporaryCredentials... creds) {
    assertThat(expectedSize).isEqualTo(creds.length);
    assertThat(GenericCredentialProvider.globalCache.size()).isEqualTo(expectedSize);
    for (TemporaryCredentials cred : creds) {
      assertThat(GenericCredentialProvider.globalCache.asMap().values())
          .contains(new GenericCredential(cred));
    }
  }

  public static Configuration newTableBasedConf(String tableId) {
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

  public static Configuration newTableBasedConf() {
    return newTableBasedConf("testTableId");
  }

  public static Configuration newPathBasedConf(String path) {
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

  public static Configuration newPathBasedConf() {
    return newPathBasedConf("path");
  }
}
