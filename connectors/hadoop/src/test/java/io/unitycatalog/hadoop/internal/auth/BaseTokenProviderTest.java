package io.unitycatalog.hadoop.internal.auth;

import static io.unitycatalog.hadoop.internal.id.CredIdTest.EMPTY_CRED_CONTEXT_ID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.unitycatalog.client.api.TemporaryCredentialsApi;
import io.unitycatalog.client.internal.Clock;
import io.unitycatalog.client.model.PathOperation;
import io.unitycatalog.client.model.TableOperation;
import io.unitycatalog.client.model.TemporaryCredentials;
import io.unitycatalog.hadoop.internal.CredentialUtil;
import io.unitycatalog.hadoop.internal.UCHadoopConfConstants;
import io.unitycatalog.hadoop.internal.id.CredId;
import io.unitycatalog.hadoop.internal.id.PathCredId;
import io.unitycatalog.hadoop.internal.id.TableCredId;
import io.unitycatalog.hadoop.internal.util.MapIdGenerator;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.OngoingStubbing;

public abstract class BaseTokenProviderTest<T extends GenericCredentialProvider> {
  private String clockName;
  private Clock clock;

  /**
   * Builds a standard UC fetcher from a (table- or path-based) conf, dispatching to the concrete
   * {@link CredId} subtype expected by {@link GenericCredentialFetcher#forUc}.
   */
  static GenericCredentialFetcher ucFetcher(Configuration conf, TemporaryCredentialsApi api) {
    CredId credId = CredId.create(conf);
    if (credId instanceof PathCredId) {
      return GenericCredentialFetcher.forUc((PathCredId) credId, api);
    }
    return GenericCredentialFetcher.forUc((TableCredId) credId, api);
  }

  /** Use the {@link Configuration} and the mocked api to create a new provider. */
  protected abstract T createTestProvider(Configuration conf, TemporaryCredentialsApi mockApi);

  /** Uses the given fetcher to create a provider for multi-credential response tests. */
  protected abstract T createTestProvider(Configuration conf, GenericCredentialFetcher fetcher);

  /** New a testing temporary credentials, using the id and expiration time. */
  protected abstract TemporaryCredentials newTempCred(String id, long expirationMillis);

  /** Creates a cloud-specific generic credential scoped to {@code location}. */
  protected abstract GenericCredential newGenericCred(
      String id, long expirationMillis, String location);

  /** Returns a cloud-specific URI under the test storage root. */
  protected abstract String location(String path);

  /** Set the credentials into the hadoop conf, as the initialized credential. */
  protected abstract void setInitialCred(Configuration conf, TemporaryCredentials cred);

  /** Use the provider to resolve the last credential, and assert it's the expected one. */
  protected abstract void assertCred(T provider, TemporaryCredentials expected);

  @BeforeEach
  public void before() {
    clockName = UUID.randomUUID().toString();
    clock = Clock.getManualClock(clockName);
    GenericCredentialProvider.globalCache.clear();
  }

  @AfterEach
  public void after() {
    Clock.removeManualClock(clockName);
    clock = null;
    clockName = null;
    GenericCredentialProvider.globalCache.clear();
  }

  @Test
  public void selectsCredentialCoveringLocationByLongestPrefix() {
    GenericCredential bucket = newGenericCred("bucket", Long.MAX_VALUE, location(""));
    GenericCredential table = newGenericCred("table", Long.MAX_VALUE, location("/t"));
    GenericCredential child = newGenericCred("child", Long.MAX_VALUE, location("/t/child"));

    T provider = provider(location("/t/child/file"), bucket, table, child);

    assertThat(provider.accessCredentials()).isSameAs(child);
  }

  @Test
  public void selectedCredentialIsSharedAcrossProvidersWithoutRefetch() throws Exception {
    GenericCredential firstSelected = newGenericCred("first", Long.MAX_VALUE, location("/t"));
    GenericCredential secondSelected = newGenericCred("second", Long.MAX_VALUE, location("/t"));
    GenericCredentialFetcher secondFetcher =
        fetcherReturning(
            List.of(newGenericCred("bucket", Long.MAX_VALUE, location("")), secondSelected));

    T first =
        provider(
            location("/t/file"),
            newGenericCred("bucket", Long.MAX_VALUE, location("")),
            firstSelected);
    T second = createTestProvider(providerConf(location("/t/file")), secondFetcher);

    assertThat(first.accessCredentials()).isSameAs(firstSelected);
    assertThat(GenericCredentialProvider.globalCache.values()).containsExactly(firstSelected);

    assertThat(second.accessCredentials()).isSameAs(firstSelected);
    verify(secondFetcher, never()).createCredentials();
  }

  @Test
  public void renewalReselectsFromTheRefetchedList() throws Exception {
    long soon = clock.now().toEpochMilli() + 2000L;
    GenericCredential first = newGenericCred("first", soon, location("/t"));
    GenericCredential second = newGenericCred("second", Long.MAX_VALUE, location("/t"));
    GenericCredentialFetcher fetcher =
        fetcherReturning(
            List.of(newGenericCred("bucket", Long.MAX_VALUE, location("")), first),
            List.of(newGenericCred("bucket", Long.MAX_VALUE, location("")), second));
    Configuration conf = providerConf(location("/t/file"));
    conf.setLong(UCHadoopConfConstants.UC_RENEWAL_LEAD_TIME_KEY, 1000L);
    T provider = createTestProvider(conf, fetcher);

    assertThat(provider.accessCredentials()).isSameAs(first);

    clock.sleep(Duration.ofMillis(1500));
    assertThat(provider.accessCredentials()).isSameAs(second);
    verify(fetcher, times(2)).createCredentials();
  }

  @Test
  public void singleCredentialIsUsedWithoutMatchingLocation() {
    GenericCredential only = newGenericCred("only", Long.MAX_VALUE, location("/other"));

    T provider = provider(location("/t/file"), only);

    assertThat(provider.accessCredentials()).isSameAs(only);
  }

  @Test
  public void throwsWhenNoCredentialIsVended() {
    T provider = provider(location("/t"));

    assertThatThrownBy(provider::accessCredentials)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("No vended credential was returned.");
  }

  @Test
  public void throwsWhenMultipleCredentialsButNoLocation() {
    T provider =
        provider(
            null,
            newGenericCred("a", Long.MAX_VALUE, location("/a")),
            newGenericCred("b", Long.MAX_VALUE, location("/b")));

    assertThatThrownBy(provider::accessCredentials)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Multiple credentials were vended but no location");
  }

  @Test
  public void throwsWhenNoCredentialCoversLocation() {
    T provider =
        provider(
            location("/t"),
            newGenericCred("other", Long.MAX_VALUE, location("/other")),
            newGenericCred("sibling", Long.MAX_VALUE, location("/sibling")));

    assertThatThrownBy(provider::accessCredentials)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("No vended credential covers location");
  }

  @Test
  public void selectsCredentialWhenCacheDisabledAndBypassesSharedCache() {
    GenericCredential table = newGenericCred("table", Long.MAX_VALUE, location("/t"));

    Configuration conf = providerConf(location("/t/file"));
    conf.setBoolean(UCHadoopConfConstants.UC_CREDENTIAL_CACHE_ENABLED_KEY, false);
    T provider =
        createTestProvider(
            conf,
            fetcherReturning(
                List.of(newGenericCred("bucket", Long.MAX_VALUE, location("")), table)));

    assertThat(provider.accessCredentials()).isSameAs(table);
    assertThat(GenericCredentialProvider.globalCache.values()).isEmpty();
  }

  @Test
  public void testTableTemporaryCredentialsRenew() throws Exception {
    Configuration conf = newTableBasedConf();
    conf.set(UCHadoopConfConstants.UC_TEST_CLOCK_NAME, clockName);
    conf.setLong(UCHadoopConfConstants.UC_RENEWAL_LEAD_TIME_KEY, 1000L);

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
  public void initialCredentialReadsPrefixFromConf() {
    Configuration conf = newTableBasedConf();
    TemporaryCredentials credential = newTempCred("initial", Long.MAX_VALUE);
    setInitialCred(conf, credential);
    T provider = createTestProvider(conf, mock(TemporaryCredentialsApi.class));

    assertThat(provider.initGenericCredential(conf).prefix()).isNull();

    conf.set(UCHadoopConfConstants.UC_CREDENTIAL_PREFIX_KEY, "");
    assertThat(provider.initGenericCredential(conf).prefix()).isEmpty();

    conf.set(UCHadoopConfConstants.UC_CREDENTIAL_PREFIX_KEY, "test-prefix");
    assertThat(provider.initGenericCredential(conf).prefix()).isEqualTo("test-prefix");
  }

  @Test
  public void testTableTemporaryCredentialsRenewWithInitialCredentials() throws Exception {
    Configuration conf = newTableBasedConf();
    conf.set(UCHadoopConfConstants.UC_TEST_CLOCK_NAME, clockName);
    conf.setLong(UCHadoopConfConstants.UC_RENEWAL_LEAD_TIME_KEY, 1000L);

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
    conf.set(UCHadoopConfConstants.UC_TEST_CLOCK_NAME, clockName);
    conf.setLong(UCHadoopConfConstants.UC_RENEWAL_LEAD_TIME_KEY, 1000L);

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
    conf.set(UCHadoopConfConstants.UC_TEST_CLOCK_NAME, clockName);
    conf.setLong(UCHadoopConfConstants.UC_RENEWAL_LEAD_TIME_KEY, 1000L);

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
    tableAconf.set(UCHadoopConfConstants.UC_TEST_CLOCK_NAME, clockName);
    tableAconf.setLong(UCHadoopConfConstants.UC_RENEWAL_LEAD_TIME_KEY, 1000L);

    Configuration tableBconf = newTableBasedConf("tableB");
    tableBconf.set(UCHadoopConfConstants.UC_TEST_CLOCK_NAME, clockName);
    tableBconf.setLong(UCHadoopConfConstants.UC_RENEWAL_LEAD_TIME_KEY, 1000L);

    Configuration pathAconf = newPathBasedConf("pathA");
    pathAconf.set(UCHadoopConfConstants.UC_TEST_CLOCK_NAME, clockName);
    pathAconf.setLong(UCHadoopConfConstants.UC_RENEWAL_LEAD_TIME_KEY, 1000L);

    Configuration pathBconf = newPathBasedConf("pathB");
    pathBconf.set(UCHadoopConfConstants.UC_TEST_CLOCK_NAME, clockName);
    pathBconf.setLong(UCHadoopConfConstants.UC_RENEWAL_LEAD_TIME_KEY, 1000L);

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

  @Test
  public void sameTableDifferentCredContextUsesSeparateGlobalCacheEntries() throws Exception {
    String contextA = MapIdGenerator.generateId(Map.of("type", "static", "token", "tenant-a"));
    String contextB = MapIdGenerator.generateId(Map.of("type", "static", "token", "tenant-b"));

    Configuration confA = newTableBasedConf("shared-table");
    confA.set(UCHadoopConfConstants.UC_CRED_CONTEXT_ID_KEY, contextA);
    confA.set(UCHadoopConfConstants.UC_TEST_CLOCK_NAME, clockName);
    confA.setLong(UCHadoopConfConstants.UC_RENEWAL_LEAD_TIME_KEY, 1000L);

    Configuration confB = newTableBasedConf("shared-table");
    confB.set(UCHadoopConfConstants.UC_CRED_CONTEXT_ID_KEY, contextB);
    confB.set(UCHadoopConfConstants.UC_TEST_CLOCK_NAME, clockName);
    confB.setLong(UCHadoopConfConstants.UC_RENEWAL_LEAD_TIME_KEY, 1000L);

    TemporaryCredentials credA = newTempCred("tenantA", clock.now().toEpochMilli() + 2000L);
    TemporaryCredentials credB = newTempCred("tenantB", clock.now().toEpochMilli() + 2000L);

    TemporaryCredentialsApi tempCredApi = mock(TemporaryCredentialsApi.class);
    when(tempCredApi.generateTemporaryTableCredentials(any())).thenReturn(credA).thenReturn(credB);

    T providerA = createTestProvider(confA, tempCredApi);
    T providerB = createTestProvider(confB, tempCredApi);

    assertCred(providerA, credA);
    assertCred(providerB, credB);
    assertGlobalCache(2, credA, credB);

    assertCred(providerA, credA);
    assertCred(providerB, credB);
    assertGlobalCache(2, credA, credB);
  }

  @Test
  public void sameScopeDifferentPrefixUsesSeparateGlobalCacheEntries() throws Exception {
    Configuration confA = newTableBasedConf("shared-table");
    confA.set(UCHadoopConfConstants.UC_CREDENTIAL_PREFIX_KEY, location("/a"));

    Configuration confB = newTableBasedConf("shared-table");
    confB.set(UCHadoopConfConstants.UC_CREDENTIAL_PREFIX_KEY, location("/b"));

    TemporaryCredentials credA = newTempCred("locationA", Long.MAX_VALUE);
    TemporaryCredentials credB = newTempCred("locationB", Long.MAX_VALUE);

    TemporaryCredentialsApi tempCredApi = mock(TemporaryCredentialsApi.class);
    when(tempCredApi.generateTemporaryTableCredentials(any())).thenReturn(credA).thenReturn(credB);

    T providerA = createTestProvider(confA, tempCredApi);
    T providerB = createTestProvider(confB, tempCredApi);

    assertCred(providerA, credA);
    assertCred(providerB, credB);
    assertGlobalCache(2, credA, credB);
  }

  @Test
  public void sameScopeSamePrefixReusesGlobalCacheEntry() throws Exception {
    Configuration conf = newTableBasedConf("shared-table");
    conf.set(UCHadoopConfConstants.UC_CREDENTIAL_PREFIX_KEY, location("/a"));

    TemporaryCredentials cred = newTempCred("locationA", Long.MAX_VALUE);
    TemporaryCredentialsApi tempCredApi = mock(TemporaryCredentialsApi.class);
    when(tempCredApi.generateTemporaryTableCredentials(any())).thenReturn(cred);

    T providerA = createTestProvider(conf, tempCredApi);
    T providerB = createTestProvider(conf, tempCredApi);

    assertCred(providerA, cred);
    assertCred(providerB, cred);
    assertGlobalCache(1, cred);
    verify(tempCredApi, times(1)).generateTemporaryTableCredentials(any());
  }

  @Test
  public void sameScopeNullPrefixReusesGlobalCacheEntry() throws Exception {
    Configuration confA = newTableBasedConf("shared-table");
    Configuration confB = newTableBasedConf("shared-table");

    TemporaryCredentials cred = newTempCred("locationA", Long.MAX_VALUE);
    TemporaryCredentialsApi tempCredApi = mock(TemporaryCredentialsApi.class);
    when(tempCredApi.generateTemporaryTableCredentials(any())).thenReturn(cred);

    T providerA = createTestProvider(confA, tempCredApi);
    T providerB = createTestProvider(confB, tempCredApi);

    assertCred(providerA, cred);
    assertCred(providerB, cred);
    assertGlobalCache(1, cred);
    verify(tempCredApi, times(1)).generateTemporaryTableCredentials(any());
  }

  @Test
  public void differentScopeSamePrefixUsesSeparateGlobalCacheEntries() throws Exception {
    Configuration confA = newTableBasedConf("table-a");
    confA.set(UCHadoopConfConstants.UC_CREDENTIAL_PREFIX_KEY, location("/shared"));

    Configuration confB = newTableBasedConf("table-b");
    confB.set(UCHadoopConfConstants.UC_CREDENTIAL_PREFIX_KEY, location("/shared"));

    TemporaryCredentials credA = newTempCred("tableA", Long.MAX_VALUE);
    TemporaryCredentials credB = newTempCred("tableB", Long.MAX_VALUE);
    TemporaryCredentialsApi tempCredApi = mock(TemporaryCredentialsApi.class);
    when(tempCredApi.generateTemporaryTableCredentials(any())).thenReturn(credA).thenReturn(credB);

    T providerA = createTestProvider(confA, tempCredApi);
    T providerB = createTestProvider(confB, tempCredApi);

    assertCred(providerA, credA);
    assertCred(providerB, credB);
    assertGlobalCache(2, credA, credB);
    verify(tempCredApi, times(2)).generateTemporaryTableCredentials(any());
  }

  private T provider(String location, GenericCredential... credentials) {
    return createTestProvider(providerConf(location), fetcherReturning(List.of(credentials)));
  }

  private Configuration providerConf(String location) {
    Configuration conf = newTableBasedConf("tid");
    conf.set(UCHadoopConfConstants.UC_TEST_CLOCK_NAME, clockName);
    if (location != null) {
      conf.set(UCHadoopConfConstants.UC_CREDENTIAL_PREFIX_KEY, location);
    }
    return conf;
  }

  @SafeVarargs
  private static GenericCredentialFetcher fetcherReturning(List<GenericCredential>... responses) {
    GenericCredentialFetcher fetcher = mock(GenericCredentialFetcher.class);
    try {
      OngoingStubbing<List<GenericCredential>> stub = when(fetcher.createCredentials());
      for (List<GenericCredential> response : responses) {
        stub = stub.thenReturn(response);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return fetcher;
  }

  private static void assertGlobalCache(int expectedSize, TemporaryCredentials... creds) {
    assertThat(expectedSize).isEqualTo(creds.length);
    assertThat(GenericCredentialProvider.globalCache.size()).isEqualTo(expectedSize);
    for (TemporaryCredentials cred : creds) {
      assertThat(GenericCredentialProvider.globalCache.values())
          .contains(CredentialUtil.toGenericCredential(cred));
    }
  }

  public static Configuration newTableBasedConf(String tableId) {
    Configuration conf = new Configuration();
    conf.set(UCHadoopConfConstants.UC_CRED_CONTEXT_ID_KEY, EMPTY_CRED_CONTEXT_ID);
    conf.set(UCHadoopConfConstants.UC_URI_KEY, "http://localhost:8080");
    conf.set(UCHadoopConfConstants.UC_AUTH_TYPE, "static");
    conf.set(UCHadoopConfConstants.UC_AUTH_TOKEN_KEY, "unity-catalog-token");

    // For table-based temporary requests.
    conf.set(
        UCHadoopConfConstants.UC_CREDENTIALS_TYPE_KEY,
        UCHadoopConfConstants.UC_CREDENTIALS_TYPE_TABLE_VALUE);
    conf.set(UCHadoopConfConstants.UC_TABLE_ID_KEY, tableId);
    conf.set(UCHadoopConfConstants.UC_TABLE_OPERATION_KEY, TableOperation.READ.getValue());

    return conf;
  }

  public static Configuration newTableBasedConf() {
    return newTableBasedConf("testTableId");
  }

  public static Configuration newPathBasedConf(String path) {
    Configuration conf = new Configuration();
    conf.set(UCHadoopConfConstants.UC_CRED_CONTEXT_ID_KEY, EMPTY_CRED_CONTEXT_ID);
    conf.set(UCHadoopConfConstants.UC_URI_KEY, "http://localhost:8080");
    conf.set(UCHadoopConfConstants.UC_AUTH_TYPE, "static");
    conf.set(UCHadoopConfConstants.UC_AUTH_TOKEN_KEY, "unity-catalog-token");

    // For path-based temporary requests.
    conf.set(
        UCHadoopConfConstants.UC_CREDENTIALS_TYPE_KEY,
        UCHadoopConfConstants.UC_CREDENTIALS_TYPE_PATH_VALUE);
    conf.set(UCHadoopConfConstants.UC_PATH_KEY, path);
    conf.set(UCHadoopConfConstants.UC_PATH_OPERATION_KEY, PathOperation.PATH_READ.getValue());

    return conf;
  }

  public static Configuration newPathBasedConf() {
    return newPathBasedConf("path");
  }
}
