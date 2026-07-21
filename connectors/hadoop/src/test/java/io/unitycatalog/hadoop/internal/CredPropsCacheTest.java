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
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Covers the {@link CredPropsUtil} encoder side credential cache: reuse across queries with the
 * same credential identity, separation by credential identity, cache-disable, and clock-driven
 * expiry. The cache is cloud-agnostic, so these run once against S3; the per-cloud property matrix
 * lives in {@link CredPropsBaseTest} and its subclasses.
 */
class CredPropsCacheTest {

  @BeforeEach
  void clearCredentialCache() {
    CredPropsUtil.initialCredCache.clear();
  }

  @AfterEach
  void resetFactory() {
    CredPropsUtil.genericCredFetcherFactory = GenericCredentialFetcher::create;
    CredPropsUtil.initialCredCache.clear();
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
  @ParameterizedTest(name = "{0}")
  @MethodSource("credIdVariants")
  void sameResourceDifferentCatalogUriCachedSeparately(String variant, CredPropsFetch fetch)
      throws Exception {
    AtomicInteger fetches = new AtomicInteger();
    CredPropsUtil.genericCredFetcherFactory =
        (apiClient, credId) -> {
          fetches.incrementAndGet();
          return mockGenericCredentialFetcher(s3Creds());
        };

    fetch.fetch("http://uc-a");
    fetch.fetch("http://uc-b");

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

  @Test
  void expiredCachedCredentialIsRefetched() throws Exception {
    String clockName = UUID.randomUUID().toString();
    Clock clock = Clock.getManualClock(clockName);
    try {
      GenericCredential cred1 = s3CredsExpiringAt("1", clock.now().toEpochMilli() + 2000L);
      GenericCredential cred2 = s3CredsExpiringAt("2", clock.now().toEpochMilli() + 20000L);
      AtomicInteger fetches = new AtomicInteger();
      CredPropsUtil.genericCredFetcherFactory =
          (apiClient, credId) ->
              mockGenericCredentialFetcher(fetches.getAndIncrement() == 0 ? cred1 : cred2);

      Configuration conf = new Configuration(false);
      conf.set(UCHadoopConfConstants.UC_TEST_CLOCK_NAME, clockName);
      conf.setLong(UCHadoopConfConstants.UC_RENEWAL_LEAD_TIME_KEY, 1000L);

      // 1st query fetches cred1 and caches it.
      assertThat(createTableCredProps(conf)).containsEntry("fs.s3a.session.token", "st1");
      // 2nd query reuses cred1 while it is still valid.
      assertThat(createTableCredProps(conf)).containsEntry("fs.s3a.session.token", "st1");
      assertThat(fetches.get()).isEqualTo(1);

      // Advance the clock so cred1 is within the renewal lead time; the next query refetches cred2.
      clock.sleep(Duration.ofMillis(1500));
      assertThat(createTableCredProps(conf)).containsEntry("fs.s3a.session.token", "st2");
      assertThat(fetches.get()).isEqualTo(2);
    } finally {
      Clock.removeManualClock(clockName);
    }
  }

  // ---- test fixtures -------------------------------------------------------------------------

  /** Fetches credential props for one fixed resource + auth against the given catalog. */
  @FunctionalInterface
  private interface CredPropsFetch {
    void fetch(String catalogUri) throws Exception;
  }

  private static Stream<Arguments> credIdVariants() {
    return Stream.of(
        Arguments.of(
            "TableCredId",
            (CredPropsFetch)
                catalogUri ->
                    createTableCredProps(
                        new Configuration(false), catalogUri, "shared-table", tokenProvider())),
        Arguments.of(
            "PathCredId",
            (CredPropsFetch)
                catalogUri ->
                    CredPropsUtil.createPathCredProps(
                        false,
                        false,
                        new Configuration(false),
                        "s3",
                        null,
                        catalogUri,
                        tokenProvider(),
                        "s3://bucket/shared",
                        UCCredentialHadoopConfs.PathOperation.PATH_READ_WRITE,
                        Map.of())),
        Arguments.of(
            "DeltaTableCredId",
            (CredPropsFetch)
                catalogUri ->
                    CredPropsUtil.createDeltaTableCredProps(
                        false,
                        false,
                        new Configuration(false),
                        "s3",
                        null,
                        catalogUri,
                        tokenProvider(),
                        UCDeltaTableIdentifier.of("cat", "sch", "tbl"),
                        "s3://bucket/tbl",
                        UCCredentialHadoopConfs.TableOperation.READ_WRITE,
                        Map.of())),
        Arguments.of(
            "DeltaStagingTableCredId",
            (CredPropsFetch)
                catalogUri ->
                    CredPropsUtil.createDeltaStagingTableCredProps(
                        false,
                        false,
                        new Configuration(false),
                        "s3",
                        null,
                        catalogUri,
                        tokenProvider(),
                        "staging-uuid",
                        "s3://bucket/staging",
                        Map.of())));
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
    return createTableCredProps(conf, "http://uc", tableId, tokenProvider);
  }

  private static Map<String, String> createTableCredProps(
      Configuration conf, String catalogUri, String tableId, TokenProvider tokenProvider)
      throws Exception {
    return CredPropsUtil.createTableCredProps(
        false,
        false,
        conf,
        "s3",
        null,
        catalogUri,
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
      when(api.createCredential()).thenReturn(creds);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return api;
  }
}
