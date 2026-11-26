package io.unitycatalog.hadoop.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.unitycatalog.client.auth.TokenProvider;
import io.unitycatalog.client.internal.Clock;
import io.unitycatalog.hadoop.UCCredentialHadoopConfs;
import io.unitycatalog.hadoop.internal.auth.AwsCredential;
import io.unitycatalog.hadoop.internal.auth.GenericCredential;
import io.unitycatalog.hadoop.internal.auth.GenericCredentialFetcher;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Covers the {@link CredPropsUtil} encoder side credential cache: reuse across queries with the
 * same credential identity, separation by credential identity, cache-disable, and clock-driven
 * expiry. The cache is cloud-agnostic, so these run once against S3; the per-cloud property matrix
 * lives in {@link CredPropsBaseTest} and its subclasses.
 */
class CredPropsCacheTest {

  private final String clockName = UUID.randomUUID().toString();
  private final Clock clock = Clock.getManualClock(clockName);

  @BeforeEach
  void clearCredentialCache() {
    CredPropsUtil.initialCredCache.clear();
  }

  @AfterEach
  void resetFactory() {
    CredPropsUtil.genericCredFetcherFactory = GenericCredentialFetcher::create;
    CredPropsUtil.initialCredCache.clear();
    Clock.removeManualClock(clockName);
  }

  @Test
  void sameCredIdReusesCachedCredentialAcrossQueries() throws Exception {
    AtomicInteger fetches = new AtomicInteger();
    CredPropsUtil.genericCredFetcherFactory =
        (apiClient, credId) -> {
          fetches.incrementAndGet();
          return mockGenericCredentialFetcher(s3Creds());
        };

    Map<String, String> first = createTableCredProps(new Configuration(false));
    Map<String, String> second = createTableCredProps(new Configuration(false));

    assertThat(fetches.get()).isEqualTo(1);
    assertThat(first).isEqualTo(second).containsEntry("fs.s3a.access.key", "ak");
  }

  @Test
  void differentCredIdsAreCachedSeparately() throws Exception {
    AtomicInteger fetches = new AtomicInteger();
    CredPropsUtil.genericCredFetcherFactory =
        (apiClient, credId) -> {
          fetches.incrementAndGet();
          return mockGenericCredentialFetcher(s3Creds());
        };

    createTableCredProps(new Configuration(false), "tidA");
    createTableCredProps(new Configuration(false), "tidB");

    assertThat(fetches.get()).isEqualTo(2);
  }

  @Test
  void sameTableDifferentAuthConfigsAreCachedSeparately() throws Exception {
    AtomicInteger fetches = new AtomicInteger();
    CredPropsUtil.genericCredFetcherFactory =
        (apiClient, credId) -> {
          fetches.incrementAndGet();
          return mockGenericCredentialFetcher(
              s3CredsExpiringAt(
                  String.valueOf(fetches.get()), System.currentTimeMillis() + 60_000));
        };

    TokenProvider tenantA = TokenProvider.create(Map.of("type", "static", "token", "tenant-a"));
    TokenProvider tenantB = TokenProvider.create(Map.of("type", "static", "token", "tenant-b"));

    createTableCredProps(new Configuration(false), "shared-table", tenantA);
    createTableCredProps(new Configuration(false), "shared-table", tenantB);

    assertThat(fetches.get()).isEqualTo(2);
  }

  /**
   * The credential-context id folds in {@code catalogUri}, so the same resource accessed via two
   * different catalogs with the same auth must not share a cached credential. Covers every {@link
   * io.unitycatalog.hadoop.internal.id.CredId} variant produced by the {@code create*CredProps}
   * entry points.
   */
  @ParameterizedTest
  @EnumSource(EntryPoint.class)
  void sameResourceDifferentCatalogUriCachedSeparately(EntryPoint entryPoint) throws Exception {
    AtomicInteger fetches = new AtomicInteger();
    CredPropsUtil.genericCredFetcherFactory =
        (apiClient, credId) -> {
          fetches.incrementAndGet();
          return mockGenericCredentialFetcher(s3Creds());
        };

    entryPoint.call(false, new Configuration(false), "s3", "http://uc-a");
    entryPoint.call(false, new Configuration(false), "s3", "http://uc-b");

    assertThat(fetches.get()).isEqualTo(2);
  }

  @Test
  void cacheDisabledFetchesForEveryQuery() throws Exception {
    AtomicInteger fetches = new AtomicInteger();
    CredPropsUtil.genericCredFetcherFactory =
        (apiClient, credId) -> {
          fetches.incrementAndGet();
          return mockGenericCredentialFetcher(s3Creds());
        };
    Configuration conf = new Configuration(false);
    conf.setBoolean(UCHadoopConfConstants.UC_CREDENTIAL_CACHE_ENABLED_KEY, false);

    createTableCredProps(conf);
    createTableCredProps(conf);

    assertThat(fetches.get()).isEqualTo(2);
  }

  /**
   * A cached credential is reused only while at least half of the lifetime it had left when
   * received remains. Without renewal it is written as fixed keys that nothing refreshes, so a
   * query never starts on one that is about to expire.
   */
  @ParameterizedTest(name = "{0} renew={1}")
  @MethodSource("entryPointsByRenewal")
  void cachedCredentialIsReusedUntilHalfItsLifetimeRemains(EntryPoint entryPoint, boolean renew)
      throws Exception {
    AtomicInteger fetches = installCountingFetcher(Duration.ofHours(1), Duration.ZERO);
    Configuration conf = clockConf(30_000L);
    String tokenKey = renew ? "fs.s3a.init.session.token" : "fs.s3a.session.token";

    entryPoint.call(renew, conf, "s3", "http://uc");
    clock.sleep(Duration.ofMinutes(30).minusMillis(1));
    assertThat(entryPoint.call(renew, conf, "s3", "http://uc")).containsEntry(tokenKey, "st1");
    assertThat(fetches.get()).isEqualTo(1);

    clock.sleep(Duration.ofMillis(1));
    assertThat(entryPoint.call(renew, conf, "s3", "http://uc")).containsEntry(tokenKey, "st2");
    assertThat(fetches.get()).isEqualTo(2);
  }

  /**
   * A credential received with less lifetime left, such as one the server served from its own
   * cache, is reused for half of what it had left, and never within the renewal lead time.
   */
  @ParameterizedTest(name = "lifetimeLeft={0} leadTime={1}ms reusedFor={2}")
  @CsvSource({"PT10M, 30000, PT5M", "PT40S, 30000, PT10S"})
  void cachedCredentialIsReusedUntilItsThreshold(
      Duration lifetimeLeft, long leadTimeMillis, Duration reusedFor) throws Exception {
    AtomicInteger fetches = installCountingFetcher(lifetimeLeft, Duration.ZERO);
    Configuration conf = clockConf(leadTimeMillis);

    EntryPoint.TABLE.call(false, conf, "s3", "http://uc");
    clock.sleep(reusedFor.minusMillis(1));
    assertThat(EntryPoint.TABLE.call(false, conf, "s3", "http://uc"))
        .containsEntry("fs.s3a.session.token", "st1");
    assertThat(fetches.get()).isEqualTo(1);

    clock.sleep(Duration.ofMillis(1));
    assertThat(EntryPoint.TABLE.call(false, conf, "s3", "http://uc"))
        .containsEntry("fs.s3a.session.token", "st2");
    assertThat(fetches.get()).isEqualTo(2);
  }

  /**
   * The lifetime is measured from when the fetch returns: a credential vended with 10 minutes left
   * that takes 2 minutes to arrive has 8 left, so it is reused for 4 minutes after it arrives.
   */
  @Test
  void thresholdIsMeasuredFromWhenTheCredentialIsReceived() throws Exception {
    AtomicInteger fetches = installCountingFetcher(Duration.ofMinutes(10), Duration.ofMinutes(2));
    Configuration conf = clockConf(30_000L);

    EntryPoint.TABLE.call(false, conf, "s3", "http://uc");
    clock.sleep(Duration.ofMinutes(4).minusMillis(1));
    assertThat(EntryPoint.TABLE.call(false, conf, "s3", "http://uc"))
        .containsEntry("fs.s3a.session.token", "st1");
    assertThat(fetches.get()).isEqualTo(1);

    clock.sleep(Duration.ofMillis(1));
    assertThat(EntryPoint.TABLE.call(false, conf, "s3", "http://uc"))
        .containsEntry("fs.s3a.session.token", "st2");
    assertThat(fetches.get()).isEqualTo(2);
  }

  // ---- test fixtures -------------------------------------------------------------------------

  /** A {@code create*CredProps} entry point, called for one fixed resource and auth. */
  private enum EntryPoint {
    TABLE(
        (renew, conf, scheme, catalogUri) ->
            CredPropsUtil.createTableCredProps(
                renew,
                false,
                conf,
                scheme,
                null,
                catalogUri,
                tokenProvider(),
                "shared-table",
                UCCredentialHadoopConfs.TableOperation.READ_WRITE,
                Map.of())),
    PATH(
        (renew, conf, scheme, catalogUri) ->
            CredPropsUtil.createPathCredProps(
                renew,
                false,
                conf,
                scheme,
                null,
                catalogUri,
                tokenProvider(),
                scheme + "://bucket/shared",
                UCCredentialHadoopConfs.PathOperation.PATH_READ_WRITE,
                Map.of())),
    DELTA_TABLE(
        (renew, conf, scheme, catalogUri) ->
            CredPropsUtil.createDeltaTableCredProps(
                renew,
                false,
                conf,
                scheme,
                null,
                catalogUri,
                tokenProvider(),
                UCDeltaTableIdentifier.of("cat", "sch", "tbl"),
                scheme + "://bucket/tbl",
                UCCredentialHadoopConfs.TableOperation.READ_WRITE,
                Map.of())),
    DELTA_STAGING_TABLE(
        (renew, conf, scheme, catalogUri) ->
            CredPropsUtil.createDeltaStagingTableCredProps(
                renew,
                false,
                conf,
                scheme,
                null,
                catalogUri,
                tokenProvider(),
                "staging-uuid",
                scheme + "://bucket/staging",
                Map.of()));

    private final CredPropsCall call;

    EntryPoint(CredPropsCall call) {
      this.call = call;
    }

    Map<String, String> call(
        boolean renewCredEnabled, Configuration conf, String scheme, String catalogUri)
        throws Exception {
      return call.call(renewCredEnabled, conf, scheme, catalogUri);
    }
  }

  @FunctionalInterface
  private interface CredPropsCall {
    Map<String, String> call(
        boolean renewCredEnabled, Configuration conf, String scheme, String catalogUri)
        throws Exception;
  }

  private static Stream<Arguments> entryPointsByRenewal() {
    return Arrays.stream(EntryPoint.values())
        .flatMap(
            entryPoint ->
                Stream.of(Arguments.of(entryPoint, false), Arguments.of(entryPoint, true)));
  }

  /**
   * Installs a fetcher whose n-th fetch vends an S3 credential with session token {@code "st<n>"}.
   * The credential expires {@code lifetime} after the fetch starts, and the fetch takes {@code
   * fetchTime} on the test clock.
   *
   * @return the number of fetches so far
   */
  private AtomicInteger installCountingFetcher(Duration lifetime, Duration fetchTime) {
    AtomicInteger fetches = new AtomicInteger();
    CredPropsUtil.genericCredFetcherFactory =
        (apiClient, credId) -> {
          long expirationMillis = clock.now().plus(lifetime).toEpochMilli();
          ((Clock.ManualClock) clock).sleep(fetchTime);
          return mockGenericCredentialFetcher(
              s3CredsExpiringAt(String.valueOf(fetches.incrementAndGet()), expirationMillis));
        };
    return fetches;
  }

  private Configuration clockConf(long leadTimeMillis) {
    Configuration conf = new Configuration(false);
    conf.set(UCHadoopConfConstants.UC_TEST_CLOCK_NAME, clockName);
    conf.setLong(UCHadoopConfConstants.UC_RENEWAL_LEAD_TIME_KEY, leadTimeMillis);
    return conf;
  }

  private static Map<String, String> createTableCredProps(Configuration conf) throws Exception {
    return createTableCredProps(conf, "tid");
  }

  private static Map<String, String> createTableCredProps(Configuration conf, String tableId)
      throws Exception {
    return createTableCredProps(conf, tableId, tokenProvider());
  }

  private static Map<String, String> createTableCredProps(
      Configuration conf, String tableId, TokenProvider tokenProvider) throws Exception {
    return CredPropsUtil.createTableCredProps(
        false,
        false,
        conf,
        "s3",
        null,
        "http://uc",
        tokenProvider,
        tableId,
        UCCredentialHadoopConfs.TableOperation.READ_WRITE,
        Map.of());
  }

  private static TokenProvider tokenProvider() {
    return TokenProvider.create(Map.of("type", "static", "token", "tok"));
  }

  private static GenericCredential s3Creds() {
    return new AwsCredential("ak", "sk", "st", null, null);
  }

  private static GenericCredential s3CredsExpiringAt(String id, long expirationMillis) {
    return new AwsCredential("ak" + id, "sk" + id, "st" + id, expirationMillis, null);
  }

  private static GenericCredentialFetcher mockGenericCredentialFetcher(GenericCredential creds) {
    GenericCredentialFetcher api = mock(GenericCredentialFetcher.class);
    try {
      when(api.createCredentials()).thenReturn(Collections.singletonList(creds));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return api;
  }
}
