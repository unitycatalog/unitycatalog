package io.unitycatalog.hadoop.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.unitycatalog.client.auth.TokenProvider;
import io.unitycatalog.client.internal.Clock;
import io.unitycatalog.hadoop.UCCredentialHadoopConfs;
import io.unitycatalog.hadoop.internal.auth.AwsCredential;
import io.unitycatalog.hadoop.internal.auth.AzureCredential;
import io.unitycatalog.hadoop.internal.auth.GcsCredential;
import io.unitycatalog.hadoop.internal.auth.GenericCredential;
import io.unitycatalog.hadoop.internal.auth.GenericCredentialFetcher;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
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
 * expiry with and without renewal. The cache is cloud-agnostic, so most run once against S3; reuse
 * without renewal also runs against each cloud's fixed token key. The per-cloud property matrix
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
   * Without renewal the cached credential is written as fixed keys that nothing renews, so it is
   * reused only while at least half of the lifetime it was vended with remains.
   */
  @ParameterizedTest(name = "{0} {1}")
  @MethodSource("entryPointsByCloud")
  void fixedCredentialIsReplacedOnceHalfItsLifetimeHasPassed(EntryPoint entryPoint, Cloud cloud)
      throws Exception {
    AtomicInteger fetches = installCountingFetcher(cloud, Duration.ofHours(1).toMillis());
    Configuration conf = clockConf(30_000L);

    assertThat(entryPoint.call(false, conf, cloud.scheme, "http://uc"))
        .containsEntry(cloud.fixedTokenKey, "token1");
    clock.sleep(Duration.ofMinutes(10));
    assertThat(entryPoint.call(false, conf, cloud.scheme, "http://uc"))
        .containsEntry(cloud.fixedTokenKey, "token1");
    assertThat(fetches.get()).isEqualTo(1);

    clock.sleep(Duration.ofMinutes(49).plusSeconds(29));
    assertThat(entryPoint.call(false, conf, cloud.scheme, "http://uc"))
        .containsEntry(cloud.fixedTokenKey, "token2");
    assertThat(fetches.get()).isEqualTo(2);
  }

  /**
   * Without renewal a cached credential is replaced once less than half its lifetime, or less than
   * the renewal lead time, remains.
   */
  @ParameterizedTest(name = "lifetime={0}ms leadTime={1}ms reusedFor={2}ms")
  @CsvSource({"3600000, 30000, 1800000", "40000, 30000, 10000"})
  void fixedCredentialIsReusedUntilItsThreshold(
      long lifetimeMillis, long leadTimeMillis, long reusedForMillis) throws Exception {
    AtomicInteger fetches = installCountingFetcher(Cloud.S3, lifetimeMillis);
    Configuration conf = clockConf(leadTimeMillis);

    EntryPoint.TABLE.call(false, conf, "s3", "http://uc");
    clock.sleep(Duration.ofMillis(reusedForMillis - 1));
    assertThat(EntryPoint.TABLE.call(false, conf, "s3", "http://uc"))
        .containsEntry("fs.s3a.session.token", "token1");
    assertThat(fetches.get()).isEqualTo(1);

    clock.sleep(Duration.ofMillis(1));
    assertThat(EntryPoint.TABLE.call(false, conf, "s3", "http://uc"))
        .containsEntry("fs.s3a.session.token", "token2");
    assertThat(fetches.get()).isEqualTo(2);
  }

  /**
   * With renewal the vended provider renews the credential, so it is reused until the lead time.
   */
  @ParameterizedTest
  @EnumSource(EntryPoint.class)
  void renewableCredentialIsReusedUntilTheLeadTime(EntryPoint entryPoint) throws Exception {
    AtomicInteger fetches = installCountingFetcher(Cloud.S3, Duration.ofHours(1).toMillis());
    Configuration conf = clockConf(30_000L);

    entryPoint.call(true, conf, "s3", "http://uc");
    clock.sleep(Duration.ofMinutes(59).plusSeconds(29));
    entryPoint.call(true, conf, "s3", "http://uc");
    assertThat(fetches.get()).isEqualTo(1);

    clock.sleep(Duration.ofSeconds(1));
    entryPoint.call(true, conf, "s3", "http://uc");
    assertThat(fetches.get()).isEqualTo(2);
  }

  /**
   * A load without renewal does not take a credential cached for a load with renewal once half its
   * lifetime has passed, and the credential it fetches instead does not replace that one.
   */
  @Test
  void renewableAndFixedCredentialsAreCachedSeparately() throws Exception {
    AtomicInteger fetches = installCountingFetcher(Cloud.S3, Duration.ofHours(1).toMillis());
    Configuration conf = clockConf(30_000L);

    EntryPoint.TABLE.call(true, conf, "s3", "http://uc");
    clock.sleep(Duration.ofMinutes(40));
    assertThat(EntryPoint.TABLE.call(false, conf, "s3", "http://uc"))
        .containsEntry("fs.s3a.session.token", "token2");
    assertThat(EntryPoint.TABLE.call(true, conf, "s3", "http://uc"))
        .containsEntry(UCHadoopConfConstants.S3A_INIT_SESSION_TOKEN, "token1");
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

  /** A cloud's vended credential and the key that serves its token when renewal is off. */
  private enum Cloud {
    S3(
        "s3",
        "fs.s3a.session.token",
        (token, exp) -> new AwsCredential("ak", "sk", token, exp, null)),
    ABFS(
        "abfss", "fs.azure.sas.fixed.token", (token, exp) -> new AzureCredential(token, exp, null)),
    GCS(
        "gs",
        "fs.gs.auth.access.token.credential",
        (token, exp) -> new GcsCredential(token, exp, null));

    private final String scheme;
    private final String fixedTokenKey;
    private final BiFunction<String, Long, GenericCredential> credential;

    Cloud(
        String scheme,
        String fixedTokenKey,
        BiFunction<String, Long, GenericCredential> credential) {
      this.scheme = scheme;
      this.fixedTokenKey = fixedTokenKey;
      this.credential = credential;
    }
  }

  private static Stream<Arguments> entryPointsByCloud() {
    return Arrays.stream(EntryPoint.values())
        .flatMap(entryPoint -> Arrays.stream(Cloud.values()).map(c -> Arguments.of(entryPoint, c)));
  }

  /**
   * Installs a fetcher that vends {@code cloud}'s credential with token {@code "token<n>"} on the
   * n-th fetch, expiring {@code lifetimeMillis} after it is vended.
   *
   * @return the number of fetches so far
   */
  private AtomicInteger installCountingFetcher(Cloud cloud, long lifetimeMillis) {
    AtomicInteger fetches = new AtomicInteger();
    CredPropsUtil.genericCredFetcherFactory =
        (apiClient, credId) ->
            mockGenericCredentialFetcher(
                cloud.credential.apply(
                    "token" + fetches.incrementAndGet(),
                    clock.now().toEpochMilli() + lifetimeMillis));
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
