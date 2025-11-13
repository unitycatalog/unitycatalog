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
import io.unitycatalog.spark.utils.Clock;
import java.time.Duration;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public abstract class BaseTokenProviderTest<T extends GenericCredentialProvider> {
  private String clockName;
  private Clock clock;

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
    clockName = UUID.randomUUID().toString();
    clock = Clock.getManualClock(clockName);
    GenericCredentialProvider.globalCache.invalidateAll();
  }

  @AfterEach
  public void after() {
    Clock.removeManualClock(clockName);
    clock = null;
    clockName = null;
    GenericCredentialProvider.globalCache.invalidateAll();
  }

  @Test
  public void testTableTemporaryCredentialsRenew() throws Exception {
    Configuration conf = newTableBasedConf();
    conf.set(UCHadoopConf.UC_TEST_CLOCK_NAME, clockName);
    conf.setLong(UCHadoopConf.UC_RENEWAL_LEAD_TIME_KEY, 1000L);

    TemporaryCredentials cred1 = newTempCred("1", clock.now().toEpochMilli() + 2000L);
    TemporaryCredentials cred2 = newTempCred("2", clock.now().toEpochMilli() + 3000L);

    // Mock the table-based temporary credentials' generation.
    TemporaryCredentialsApi tempCredApi = mock(TemporaryCredentialsApi.class);
    when(tempCredApi.generateTemporaryTableCredentials(any())).thenReturn(cred1).thenReturn(cred2);

    T provider = createTestProvider(conf, tempCredApi);

    // Use the cred1 for the 1st access.
    assertCred(provider, cred1);

    // Use the cred1 for the 2nd access, since it's valid.
    assertCred(provider, cred1);

    // Advance the clock to trigger renewal, cred2 will be valid.
    clock.sleep(Duration.ofMillis(1000));

    // Use the cred2 for the 3rd access, since renewal happened.
    assertCred(provider, cred2);

    // Use the cred2 for the 4th access.
    assertCred(provider, cred2);
  }

  @Test
  public void testTableTemporaryCredentialsRenewWithInitialCredentials() throws Exception {
    Configuration conf = newTableBasedConf();
    conf.set(UCHadoopConf.UC_TEST_CLOCK_NAME, clockName);
    conf.setLong(UCHadoopConf.UC_RENEWAL_LEAD_TIME_KEY, 1000L);

    // Use the generated credential to initialize the provider.
    TemporaryCredentials cred0 = newTempCred("0", clock.now().toEpochMilli() + 2000L);
    setInitialCred(conf, cred0);

    // Mock the path-based temporary credentials' generation.
    TemporaryCredentialsApi tempCredApi = mock(TemporaryCredentialsApi.class);
    TemporaryCredentials cred1 = newTempCred("1", clock.now().toEpochMilli() + 3000L);
    TemporaryCredentials cred2 = newTempCred("2", clock.now().toEpochMilli() + 4000L);

    when(tempCredApi.generateTemporaryTableCredentials(any())).thenReturn(cred1).thenReturn(cred2);

    // Initialize the credential provider.
    T provider = createTestProvider(conf, tempCredApi);

    // cred0 is valid.
    assertCred(provider, cred0);

    // cred0 is still valid.
    assertCred(provider, cred0);

    clock.sleep(Duration.ofMillis(1000));

    // cred0 is invalid while cred1 is valid.
    assertCred(provider, cred1);

    // cred1 is still valid.
    assertCred(provider, cred1);

    clock.sleep(Duration.ofMillis(1000));

    // cred1 is expired, while cred2 is valid.
    assertCred(provider, cred2);

    // cred2 is still valid.
    assertCred(provider, cred2);
  }

  @Test
  public void testPathTemporaryCredentialsRenew() throws Exception {
    Configuration conf = newPathBasedConf();
    conf.set(UCHadoopConf.UC_TEST_CLOCK_NAME, clockName);
    conf.setLong(UCHadoopConf.UC_RENEWAL_LEAD_TIME_KEY, 1000L);

    TemporaryCredentials cred1 = newTempCred("1", clock.now().toEpochMilli() + 2000L);
    TemporaryCredentials cred2 = newTempCred("2", clock.now().toEpochMilli() + 3000L);

    // Mock the path-based temporary credentials' generation.
    TemporaryCredentialsApi tempCredApi = mock(TemporaryCredentialsApi.class);
    when(tempCredApi.generateTemporaryPathCredentials(any())).thenReturn(cred1).thenReturn(cred2);

    T provider = createTestProvider(conf, tempCredApi);

    // Use the cred1 for the 1st access.
    assertCred(provider, cred1);

    // Use the cred1 for the 2nd access, since it's valid.
    assertCred(provider, cred1);

    // Advance the clock to renew.
    clock.sleep(Duration.ofMillis(1000));

    // Use the cred2 for the 3rd access, since cred1 it's expired.
    assertCred(provider, cred2);

    // Use the cred2 for the 4th access.
    assertCred(provider, cred2);
  }

  @Test
  public void testPathTemporaryCredentialsRenewWithInitialCredentials() throws Exception {
    Configuration conf = newPathBasedConf();
    conf.set(UCHadoopConf.UC_TEST_CLOCK_NAME, clockName);
    conf.setLong(UCHadoopConf.UC_RENEWAL_LEAD_TIME_KEY, 1000L);

    // Use the generated credential to initialize the provider.
    TemporaryCredentials cred0 = newTempCred("0", clock.now().toEpochMilli() + 2000L);
    setInitialCred(conf, cred0);

    // Mock the path-based temporary credentials' generation.
    TemporaryCredentialsApi tempCredApi = mock(TemporaryCredentialsApi.class);
    TemporaryCredentials cred1 = newTempCred("1", clock.now().toEpochMilli() + 3000L);
    TemporaryCredentials cred2 = newTempCred("2", clock.now().toEpochMilli() + 4000L);
    when(tempCredApi.generateTemporaryPathCredentials(any())).thenReturn(cred1).thenReturn(cred2);

    // Initialize the credential provider.
    T provider = createTestProvider(conf, tempCredApi);

    // cred0 is valid.
    assertCred(provider, cred0);

    // cred0 is still valid.
    assertCred(provider, cred0);

    clock.sleep(Duration.ofMillis(1000));

    // cred0 is invalid while cred1 is valid.
    assertCred(provider, cred1);

    // cred1 is still valid.
    assertCred(provider, cred1);

    clock.sleep(Duration.ofMillis(1000));

    // cred1 is expired, while cred2 is valid.
    assertCred(provider, cred2);

    // cred2 is still valid.
    assertCred(provider, cred2);
  }

  @Test
  public void testGlobalCredCache() throws Exception {
    Configuration tableAconf = newTableBasedConf("tableA");
    tableAconf.set(UCHadoopConf.UC_TEST_CLOCK_NAME, clockName);
    tableAconf.setLong(UCHadoopConf.UC_RENEWAL_LEAD_TIME_KEY, 1000L);

    Configuration tableBconf = newTableBasedConf("tableB");
    tableBconf.set(UCHadoopConf.UC_TEST_CLOCK_NAME, clockName);
    tableBconf.setLong(UCHadoopConf.UC_RENEWAL_LEAD_TIME_KEY, 1000L);

    Configuration pathAconf = newPathBasedConf("pathA");
    pathAconf.set(UCHadoopConf.UC_TEST_CLOCK_NAME, clockName);
    pathAconf.setLong(UCHadoopConf.UC_RENEWAL_LEAD_TIME_KEY, 1000L);

    Configuration pathBconf = newPathBasedConf("pathB");
    pathBconf.set(UCHadoopConf.UC_TEST_CLOCK_NAME, clockName);
    pathBconf.setLong(UCHadoopConf.UC_RENEWAL_LEAD_TIME_KEY, 1000L);

    TemporaryCredentialsApi tempCredApi = mock(TemporaryCredentialsApi.class);
    // Mock the temporary table credential API.
    // For TableA's 1st renewal
    TemporaryCredentials tableACred1 = newTempCred("table_A1", clock.now().toEpochMilli() + 2000L);
    // For TableB's 1st renewal
    TemporaryCredentials tableBCred1 = newTempCred("table_B1", clock.now().toEpochMilli() + 2000L);
    // For TableA's 2nd renewal
    TemporaryCredentials tableACred2 = newTempCred("table_A2", clock.now().toEpochMilli() + 3000L);
    // For TableB's 2nd renewal
    TemporaryCredentials tableBCred2 = newTempCred("table_B2", clock.now().toEpochMilli() + 3000L);
    when(tempCredApi.generateTemporaryTableCredentials(any()))
        .thenReturn(tableACred1)
        .thenReturn(tableBCred1)
        .thenReturn(tableACred2)
        .thenReturn(tableBCred2);

    // Mock the temporary path credential API.
    // For PathA's 1st renewal
    TemporaryCredentials pathACred1 = newTempCred("path_A1", clock.now().toEpochMilli() + 2000L);
    // For PathB's 1st renewal
    TemporaryCredentials pathBCred1 = newTempCred("path_B1", clock.now().toEpochMilli() + 2000L);
    // For PathA's 2nd renewal
    TemporaryCredentials pathACred2 = newTempCred("path_A2", clock.now().toEpochMilli() + 3000L);
    // For PathB's 2nd renewal
    TemporaryCredentials pathBCred2 = newTempCred("path_B2", clock.now().toEpochMilli() + 3000L);
    when(tempCredApi.generateTemporaryPathCredentials(any()))
        .thenReturn(pathACred1)
        .thenReturn(pathBCred1)
        .thenReturn(pathACred2)
        .thenReturn(pathBCred2);

    T providerTableA = createTestProvider(tableAconf, tempCredApi);

    T providerTableB = createTestProvider(tableBconf, tempCredApi);

    T providerPathA = createTestProvider(pathAconf, tempCredApi);

    T providerPathB = createTestProvider(pathBconf, tempCredApi);

    // TableA: 1st access.
    assertCred(providerTableA, tableACred1);
    assertGlobalCache(1, tableACred1);

    // TableB: 1st access.
    assertCred(providerTableB, tableBCred1);
    assertGlobalCache(2, tableACred1, tableBCred1);

    // PathA: 1st access.
    assertCred(providerPathA, pathACred1);
    assertGlobalCache(3, tableACred1, tableBCred1, pathACred1);

    // PathB: 1st access.
    assertCred(providerPathB, pathBCred1);
    assertGlobalCache(4, tableACred1, tableBCred1, pathACred1, pathBCred1);

    // TableA: 2nd access.
    assertCred(providerTableA, tableACred1);
    assertGlobalCache(4, tableACred1, tableBCred1, pathACred1, pathBCred1);

    // TableB: 2nd access.
    assertCred(providerTableB, tableBCred1);
    assertGlobalCache(4, tableACred1, tableBCred1, pathACred1, pathBCred1);

    // PathA: 2nd access.
    assertCred(providerPathA, pathACred1);
    assertGlobalCache(4, tableACred1, tableBCred1, pathACred1, pathBCred1);

    // PathB: 2nd access.
    assertCred(providerPathA, pathACred1);
    assertGlobalCache(4, tableACred1, tableBCred1, pathACred1, pathBCred1);

    clock.sleep(Duration.ofMillis(1000));

    // TableA: 3rd access. renew tableACred1 to tableACred2.
    assertCred(providerTableA, tableACred2);
    assertGlobalCache(4, tableACred2, tableBCred1, pathACred1, pathBCred1);

    // TableB: 3rd access. renew tableBCred1 to tableBCred2.
    assertCred(providerTableB, tableBCred2);
    assertGlobalCache(4, tableACred2, tableBCred2, pathACred1, pathBCred1);

    // PathA: 3rd access. renew pathACred1 to pathACred2.
    assertCred(providerPathA, pathACred2);
    assertGlobalCache(4, tableACred2, tableBCred2, pathACred2, pathBCred1);

    // PathB: 3rd access. renew pathBCred1 to pathBCred2.
    assertCred(providerPathB, pathBCred2);
    assertGlobalCache(4, tableACred2, tableBCred2, pathACred2, pathBCred2);
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
