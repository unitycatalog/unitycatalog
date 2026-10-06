package io.unitycatalog.server.service.credential;

import static io.unitycatalog.server.service.credential.CredentialContext.READ_ONLY;
import static io.unitycatalog.server.service.credential.CredentialContext.READ_WRITE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.unitycatalog.server.model.AwsCredentials;
import io.unitycatalog.server.model.AwsIamRoleResponse;
import io.unitycatalog.server.model.AzureUserDelegationSAS;
import io.unitycatalog.server.model.GcpOauthToken;
import io.unitycatalog.server.model.TemporaryCredentials;
import io.unitycatalog.server.persist.dao.CredentialDAO;
import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.ServerProperties;
import java.time.Clock;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

public class CachingCloudCredentialVendorTest {

  private static final NormalizedURL LOC = NormalizedURL.from("s3://bucket/tableA");
  private static final long T0 = 1_000_000_000_000L;
  private static final long ONE_HOUR_MS = 3_600_000L;

  private CloudCredentialVendor delegate;
  private Clock clock;

  @BeforeEach
  void setUp() {
    delegate = mock(CloudCredentialVendor.class);
    clock = mock(Clock.class);
    tick(T0);
  }

  private void tick(long epochMs) {
    when(clock.instant()).thenReturn(Instant.ofEpochMilli(epochMs));
  }

  /** A cache over {@link #delegate} with the given property overrides. */
  private CachingCloudCredentialVendor cache(String... kv) {
    Properties p = new Properties();
    for (int i = 0; i < kv.length; i += 2) {
      p.setProperty(kv[i], kv[i + 1]);
    }
    return new CachingCloudCredentialVendor(delegate, new ServerProperties(p), clock);
  }

  /** An AWS credential with the given access key, expiring at {@code expiryEpochMs} (or never). */
  private static TemporaryCredentials creds(String accessKey, Long expiryEpochMs) {
    return new TemporaryCredentials()
        .awsTempCredentials(
            new AwsCredentials().accessKeyId(accessKey).secretAccessKey("SK").sessionToken("TK"))
        .expirationTime(expiryEpochMs);
  }

  private static CredentialContext ctx(String roleArn) {
    return ctx(roleArn, null, READ_ONLY);
  }

  private static CredentialContext ctx(
      String roleArn, String externalId, Set<CredentialContext.Privilege> privileges) {
    CredentialDAO dao = mock(CredentialDAO.class);
    when(dao.getAwsIamRoleResponse())
        .thenReturn(new AwsIamRoleResponse().roleArn(roleArn).externalId(externalId));
    return CredentialContext.create(LOC, privileges, Optional.of(dao));
  }

  private static String accessKey(TemporaryCredentials credentials) {
    return credentials.getAwsTempCredentials().getAccessKeyId();
  }

  @Test
  void secondCallForSameBindingIsServedFromTheCache() {
    when(delegate.vendCredential(any())).thenReturn(creds("AK", T0 + ONE_HOUR_MS));
    CachingCloudCredentialVendor cache = cache();

    cache.vendCredential(ctx("arn:role/A"));
    TemporaryCredentials hit = cache.vendCredential(ctx("arn:role/A"));

    // The hit equals the vended AWS credential exactly, with no other cloud's fields added.
    assertEquals(creds("AK", T0 + ONE_HOUR_MS), hit);
    verify(delegate, times(1)).vendCredential(any());
  }

  static Stream<Arguments> changedBindings() {
    CredentialContext original = ctx("arn:role/A", "external-A", READ_ONLY);
    return Stream.of(
        Arguments.of("role", original, ctx("arn:role/B", "external-A", READ_ONLY)),
        Arguments.of("external ID", original, ctx("arn:role/A", "external-B", READ_ONLY)),
        Arguments.of("no external ID", original, ctx("arn:role/A", null, READ_ONLY)),
        // A READ_ONLY credential must never serve a READ_WRITE request, nor the reverse.
        Arguments.of("privileges", original, ctx("arn:role/A", "external-A", READ_WRITE)),
        Arguments.of(
            "location sharing a prefix",
            perBucket("s3://bucket/data"),
            perBucket("s3://bucket/data2")),
        Arguments.of("scheme", perBucket("s3://bucket/data"), perBucket("gs://bucket/data")),
        // Only single-location contexts exist today; AWS and GCP scope a vend to every location.
        Arguments.of(
            "an additional location",
            perBucket("s3://bucket/data"),
            perBucket("s3://bucket/data", "s3://bucket/other")),
        Arguments.of(
            "per-bucket config instead of a credential", original, perBucket(LOC.toString())));
  }

  /** A READ_ONLY context for {@code location} with no credential DAO (per-bucket config). */
  private static CredentialContext perBucket(String location) {
    return CredentialContext.create(NormalizedURL.from(location), READ_ONLY, Optional.empty());
  }

  /** Like {@link #perBucket(String)}, but covering several locations in one context. */
  private static CredentialContext perBucket(String first, String... more) {
    CredentialContext single = perBucket(first);
    return CredentialContext.builder()
        .storageScheme(single.getStorageScheme())
        .storageBase(single.getStorageBase())
        .privileges(single.getPrivileges())
        .locations(
            Stream.concat(Stream.of(first), Stream.of(more)).map(NormalizedURL::from).toList())
        .credentialDAO(Optional.empty())
        .build();
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("changedBindings")
  void changedBindingIsCachedSeparately(
      String changed, CredentialContext original, CredentialContext updated) {
    when(delegate.vendCredential(original)).thenReturn(creds("AK_A", T0 + ONE_HOUR_MS));
    when(delegate.vendCredential(updated)).thenReturn(creds("AK_B", T0 + ONE_HOUR_MS));
    CachingCloudCredentialVendor cache = cache();

    assertEquals("AK_A", accessKey(cache.vendCredential(original)));
    assertEquals("AK_B", accessKey(cache.vendCredential(updated)));
    assertEquals("AK_A", accessKey(cache.vendCredential(original)));
    assertEquals("AK_B", accessKey(cache.vendCredential(updated)));
    verify(delegate, times(1)).vendCredential(original);
    verify(delegate, times(1)).vendCredential(updated);
  }

  @ParameterizedTest
  @ValueSource(strings = {"gs://bucket/tableG", "abfs://container@account.dfs.core.windows.net/t"})
  void perBucketBindingIsCachedPerLocation(String location) {
    // Per-bucket config vends have no credential DAO, so the role fields in the key are null.
    NormalizedURL url = NormalizedURL.from(location);
    NormalizedURL other = NormalizedURL.from(location + "/other");
    when(delegate.vendCredential(any())).thenReturn(creds("AK", T0 + ONE_HOUR_MS));
    CachingCloudCredentialVendor cache = cache();

    cache.vendCredential(CredentialContext.create(url, READ_ONLY, Optional.empty()));
    cache.vendCredential(CredentialContext.create(url, READ_ONLY, Optional.empty()));
    verify(delegate, times(1)).vendCredential(any());

    cache.vendCredential(CredentialContext.create(other, READ_ONLY, Optional.empty()));
    verify(delegate, times(2)).vendCredential(any());
  }

  @Test
  void credentialWithinRenewalLeadIsReplacedAndTheReplacementIsReused() {
    // The first credential expires at T0+120s; with a 60s lead its renewal window opens at T0+60s.
    when(delegate.vendCredential(any()))
        .thenReturn(creds("AK_OLD", T0 + 120_000L))
        .thenReturn(creds("AK_NEW", T0 + ONE_HOUR_MS));
    CachingCloudCredentialVendor cache =
        cache("server.storage-credential-cache.renewal-lead-time", "PT1M");

    assertEquals("AK_OLD", accessKey(cache.vendCredential(ctx("arn:role/A"))));
    tick(T0 + 59_000L); // before the renewal window: served from the cache
    assertEquals("AK_OLD", accessKey(cache.vendCredential(ctx("arn:role/A"))));
    verify(delegate, times(1)).vendCredential(any());

    tick(T0 + 61_000L); // inside the renewal window: vend and cache the replacement
    TemporaryCredentials renewed = cache.vendCredential(ctx("arn:role/A"));
    assertEquals("AK_NEW", accessKey(renewed));
    assertEquals(T0 + ONE_HOUR_MS, renewed.getExpirationTime());

    tick(T0 + 62_000L); // the replacement is served without a third vend
    assertEquals("AK_NEW", accessKey(cache.vendCredential(ctx("arn:role/A"))));
    verify(delegate, times(2)).vendCredential(any());
  }

  @ParameterizedTest(name = "now = T0 + {0}ms -> served from cache: {1}")
  @CsvSource({
    // credential expires at T0 + 120s with a 60s renewal lead: stale from T0 + 60s
    "59999, true",
    "60000, false",
  })
  void renewalLeadBoundary(long elapsedMs, boolean servedFromCache) {
    when(delegate.vendCredential(any())).thenReturn(creds("AK", T0 + 120_000L));
    CachingCloudCredentialVendor cache =
        cache("server.storage-credential-cache.renewal-lead-time", "PT1M");

    cache.vendCredential(ctx("arn:role/A"));
    tick(T0 + elapsedMs);
    cache.vendCredential(ctx("arn:role/A"));

    verify(delegate, times(servedFromCache ? 1 : 2)).vendCredential(any());
  }

  @ParameterizedTest(name = "now = T0 + {0}ms -> served from cache: {1}")
  @CsvSource({
    // max age 5 minutes: stale from T0 + 300s, although the credential is valid for an hour
    "299999, true",
    "300000, false",
  })
  void maxAgeBoundary(long elapsedMs, boolean servedFromCache) {
    when(delegate.vendCredential(any())).thenReturn(creds("AK", T0 + ONE_HOUR_MS));
    CachingCloudCredentialVendor cache = cache("server.storage-credential-cache.max-age", "PT5M");

    cache.vendCredential(ctx("arn:role/A"));
    tick(T0 + elapsedMs);
    cache.vendCredential(ctx("arn:role/A"));

    verify(delegate, times(servedFromCache ? 1 : 2)).vendCredential(any());
  }

  @ParameterizedTest
  @NullSource
  @ValueSource(longs = T0 + ONE_HOUR_MS)
  void maxAgeCapsReuse(Long credentialExpiry) {
    // Applies both to a static credential with no expiry and to one valid for an hour.
    when(delegate.vendCredential(any())).thenReturn(creds("AK", credentialExpiry));
    CachingCloudCredentialVendor cache = cache("server.storage-credential-cache.max-age", "PT5M");

    cache.vendCredential(ctx("arn:role/A"));
    tick(T0 + 299_000L);
    cache.vendCredential(ctx("arn:role/A"));
    verify(delegate, times(1)).vendCredential(any());

    tick(T0 + 301_000L);
    cache.vendCredential(ctx("arn:role/A"));
    verify(delegate, times(2)).vendCredential(any());
  }

  @Test
  void credentialAlreadyInsideRenewalLeadIsNotCached() {
    // Expires in 30s, inside the 60s lead: it is returned but every request re-vends.
    when(delegate.vendCredential(any())).thenReturn(creds("AK", T0 + 30_000L));
    CachingCloudCredentialVendor cache =
        cache("server.storage-credential-cache.renewal-lead-time", "PT1M");

    assertEquals("AK", accessKey(cache.vendCredential(ctx("arn:role/A"))));
    cache.vendCredential(ctx("arn:role/A"));

    verify(delegate, times(2)).vendCredential(any());
  }

  @Test
  void failedVendIsNotCachedAndTheNextRequestVends() {
    when(delegate.vendCredential(any()))
        .thenThrow(new IllegalStateException("cloud unavailable"))
        .thenReturn(creds("AK", T0 + ONE_HOUR_MS));
    CachingCloudCredentialVendor cache = cache();

    assertThrows(IllegalStateException.class, () -> cache.vendCredential(ctx("arn:role/A")));
    assertEquals("AK", accessKey(cache.vendCredential(ctx("arn:role/A"))));
    assertEquals("AK", accessKey(cache.vendCredential(ctx("arn:role/A"))));

    verify(delegate, times(2)).vendCredential(any());
  }

  @Test
  void staleCredentialIsNotServedWhenItsRenewalFails() {
    when(delegate.vendCredential(any()))
        .thenReturn(creds("AK_OLD", T0 + 120_000L))
        .thenThrow(new IllegalStateException("cloud unavailable"))
        .thenReturn(creds("AK_NEW", T0 + ONE_HOUR_MS));
    CachingCloudCredentialVendor cache =
        cache("server.storage-credential-cache.renewal-lead-time", "PT1M");

    cache.vendCredential(ctx("arn:role/A"));
    tick(T0 + 61_000L); // AK_OLD is inside its renewal window

    assertThrows(IllegalStateException.class, () -> cache.vendCredential(ctx("arn:role/A")));
    assertEquals("AK_NEW", accessKey(cache.vendCredential(ctx("arn:role/A"))));
    verify(delegate, times(3)).vendCredential(any());
  }

  @Test
  void cachedCredentialIsACompleteCopyThatCallersCannotChange() {
    TemporaryCredentials vended =
        creds("AK", T0 + ONE_HOUR_MS)
            .azureUserDelegationSas(new AzureUserDelegationSAS().sasToken("SAS"))
            .gcpOauthToken(new GcpOauthToken().oauthToken("OAUTH"))
            .url(LOC.toString());
    TemporaryCredentials expected =
        creds("AK", T0 + ONE_HOUR_MS)
            .azureUserDelegationSas(new AzureUserDelegationSAS().sasToken("SAS"))
            .gcpOauthToken(new GcpOauthToken().oauthToken("OAUTH"))
            .url(LOC.toString());
    when(delegate.vendCredential(any())).thenReturn(vended);
    CachingCloudCredentialVendor cache = cache();

    cache.vendCredential(ctx("arn:role/A")).getAwsTempCredentials().setAccessKeyId("CHANGED");
    vended.getAzureUserDelegationSas().setSasToken("CHANGED_BY_DELEGATE");

    TemporaryCredentials hit = cache.vendCredential(ctx("arn:role/A"));
    assertEquals(expected, hit);
    hit.getGcpOauthToken().setOauthToken("CHANGED_BY_CALLER");
    assertEquals(expected, cache.vendCredential(ctx("arn:role/A")));
    verify(delegate, times(1)).vendCredential(any());
  }

  @Test
  void changingTheCallersPrivilegeSetDoesNotChangeTheCachedKey() {
    when(delegate.vendCredential(any())).thenReturn(creds("AK", T0 + ONE_HOUR_MS));
    CachingCloudCredentialVendor cache = cache();
    Set<CredentialContext.Privilege> privileges = new HashSet<>(READ_ONLY);

    cache.vendCredential(CredentialContext.create(LOC, privileges, Optional.empty()));
    privileges.add(CredentialContext.Privilege.UPDATE);
    cache.vendCredential(CredentialContext.create(LOC, READ_ONLY, Optional.empty()));

    verify(delegate, times(1)).vendCredential(any());
  }

  @Test
  void maxSizeBoundsTheNumberOfCachedCredentials() {
    when(delegate.vendCredential(any())).thenReturn(creds("AK", T0 + ONE_HOUR_MS));
    CachingCloudCredentialVendor cache = cache("server.storage-credential-cache.max-size", "1");

    cache.vendCredential(ctx("arn:role/A"));
    cache.vendCredential(ctx("arn:role/B"));
    cache.vendCredential(ctx("arn:role/A"));
    cache.vendCredential(ctx("arn:role/B"));

    // An unbounded cache would vend twice; with one slot at least one repeat is a miss.
    verify(delegate, atLeast(3)).vendCredential(any());
  }

  @Test
  void concurrentMissesVendIndependentlyWithoutHoldingALock() throws Exception {
    // Each vend waits until all of them have started, which only succeeds if no lock is held
    // across the cloud call. Every caller gets the vended credential.
    int callers = 4;
    CountDownLatch allVending = new CountDownLatch(callers);
    when(delegate.vendCredential(any()))
        .thenAnswer(
            invocation -> {
              allVending.countDown();
              assertThat(allVending.await(10, TimeUnit.SECONDS)).isTrue();
              return creds("AK", T0 + ONE_HOUR_MS);
            });
    CachingCloudCredentialVendor cache = cache();
    CredentialContext context = ctx("arn:role/A");

    ExecutorService pool = Executors.newFixedThreadPool(callers);
    try {
      List<Future<TemporaryCredentials>> results =
          IntStream.range(0, callers)
              .mapToObj(i -> pool.submit(() -> cache.vendCredential(context)))
              .toList();
      for (Future<TemporaryCredentials> result : results) {
        assertEquals("AK", accessKey(result.get(10, TimeUnit.SECONDS)));
      }
    } finally {
      pool.shutdownNow();
    }
    verify(delegate, times(callers)).vendCredential(any());
  }

  @Test
  void concurrentHitsOnOneCredentialEachGetAnIndependentCopy() throws Exception {
    when(delegate.vendCredential(any())).thenReturn(creds("AK", T0 + ONE_HOUR_MS));
    CachingCloudCredentialVendor cache = cache();
    CredentialContext context = ctx("arn:role/A");
    cache.vendCredential(context); // warm the cache before the threads start

    // Holds for every interleaving: each read equals the original even though every reader
    // mutates what it got back, and no read causes a second vend.
    runConcurrently(
        8,
        () -> {
          for (int i = 0; i < 200; i++) {
            TemporaryCredentials hit = cache.vendCredential(context);
            assertEquals(creds("AK", T0 + ONE_HOUR_MS), hit);
            hit.getAwsTempCredentials().setAccessKeyId("CHANGED_BY_CALLER");
          }
        });
    verify(delegate, times(1)).vendCredential(any());
  }

  @Test
  void concurrentRequestsDuringRenewalAreNeverServedTheStaleCredential() throws Exception {
    // The first vend returns AK_OLD; every later vend returns AK_NEW.
    AtomicInteger vends = new AtomicInteger();
    when(delegate.vendCredential(any()))
        .thenAnswer(
            invocation ->
                vends.getAndIncrement() == 0
                    ? creds("AK_OLD", T0 + 120_000L)
                    : creds("AK_NEW", T0 + ONE_HOUR_MS));
    CachingCloudCredentialVendor cache =
        cache("server.storage-credential-cache.renewal-lead-time", "PT1M");
    CredentialContext context = ctx("arn:role/A");
    cache.vendCredential(context);
    tick(T0 + 61_000L); // AK_OLD is now inside its renewal window

    int callers = 8;
    runConcurrently(
        callers, () -> assertEquals("AK_NEW", accessKey(cache.vendCredential(context))));

    // Concurrent misses may each vend, so the exact count depends on scheduling; the range does
    // not.
    assertThat(vends.get()).isBetween(2, callers + 1);
  }

  /**
   * Runs {@code body} on {@code threads} threads released together, and rethrows the first failure.
   * Uses no timing: the timeout only stops a hung run.
   */
  private static void runConcurrently(int threads, ThrowingRunnable body) throws Exception {
    CountDownLatch start = new CountDownLatch(1);
    ExecutorService pool = Executors.newFixedThreadPool(threads);
    try {
      List<Future<Void>> results =
          IntStream.range(0, threads)
              .mapToObj(
                  i ->
                      pool.submit(
                          () -> {
                            start.await();
                            body.run();
                            return (Void) null;
                          }))
              .toList();
      start.countDown();
      for (Future<Void> result : results) {
        result.get(10, TimeUnit.SECONDS);
      }
    } finally {
      pool.shutdownNow();
    }
  }

  @FunctionalInterface
  private interface ThrowingRunnable {
    void run() throws Exception;
  }
}
