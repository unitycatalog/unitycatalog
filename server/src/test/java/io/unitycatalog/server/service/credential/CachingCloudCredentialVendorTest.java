package io.unitycatalog.server.service.credential;

import static io.unitycatalog.server.service.credential.CredentialContext.READ_ONLY;
import static io.unitycatalog.server.service.credential.CredentialContext.READ_WRITE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.auth.oauth2.AccessToken;
import io.unitycatalog.server.model.AwsCredentials;
import io.unitycatalog.server.model.AwsIamRoleResponse;
import io.unitycatalog.server.model.AzureUserDelegationSAS;
import io.unitycatalog.server.model.GcpOauthToken;
import io.unitycatalog.server.model.TemporaryCredentials;
import io.unitycatalog.server.persist.dao.CredentialDAO;
import io.unitycatalog.server.service.credential.aws.AwsCredentialVendor;
import io.unitycatalog.server.service.credential.azure.AzureCredential;
import io.unitycatalog.server.service.credential.azure.AzureCredentialVendor;
import io.unitycatalog.server.service.credential.gcp.GcpCredentialVendor;
import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.ServerProperties;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
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
import software.amazon.awssdk.services.sts.model.Credentials;

public class CachingCloudCredentialVendorTest {

  private static final NormalizedURL LOC = NormalizedURL.from("s3://bucket/tableA");
  private static final long T0 = 1_000_000_000_000L;
  private static final long ONE_HOUR_MS = 3_600_000L;

  private AwsCredentialVendor aws;
  private AzureCredentialVendor azure;
  private GcpCredentialVendor gcp;
  private Clock clock;

  @BeforeEach
  void setUp() {
    aws = mock(AwsCredentialVendor.class);
    azure = mock(AzureCredentialVendor.class);
    gcp = mock(GcpCredentialVendor.class);
    clock = mock(Clock.class);
    tick(T0);
  }

  private void tick(long epochMs) {
    when(clock.instant()).thenReturn(Instant.ofEpochMilli(epochMs));
  }

  /** A cache over the mocked cloud vendors with the given property overrides. */
  private CachingCloudCredentialVendor cache(String... kv) {
    Properties p = new Properties();
    for (int i = 0; i < kv.length; i += 2) {
      p.setProperty(kv[i], kv[i + 1]);
    }
    return new CachingCloudCredentialVendor(
        aws,
        azure,
        gcp,
        new ServerProperties(p),
        clock,
        CachingCloudCredentialVendor.DEFAULT_EXPIRY_MARGIN);
  }

  /** AWS session credentials as STS returns them; a {@code null} expiry is a static credential. */
  private static Credentials awsCreds(String accessKey, Long expiryEpochMs) {
    return Credentials.builder()
        .accessKeyId(accessKey)
        .secretAccessKey("SK")
        .sessionToken("TK")
        .expiration(expiryEpochMs == null ? null : Instant.ofEpochMilli(expiryEpochMs))
        .build();
  }

  /** Every cloud vends a new, distinct credential on each call, valid for an hour. */
  private void vendDistinctCredentials() {
    AtomicInteger next = new AtomicInteger();
    when(aws.vendAwsCredentials(any()))
        .thenAnswer(i -> awsCreds("AK" + next.incrementAndGet(), T0 + ONE_HOUR_MS));
    when(azure.vendAzureCredential(any()))
        .thenAnswer(
            i ->
                AzureCredential.builder()
                    .sasToken("SAS" + next.incrementAndGet())
                    .expirationTimeInEpochMillis(T0 + ONE_HOUR_MS)
                    .build());
    when(gcp.vendGcpCredential(any()))
        .thenAnswer(
            i -> new AccessToken("TOKEN" + next.incrementAndGet(), new Date(T0 + ONE_HOUR_MS)));
  }

  /** The number of cloud vends across all three clouds. */
  private int vends() {
    return Stream.of(aws, azure, gcp)
        .mapToInt(vendor -> mockingDetails(vendor).getInvocations().size())
        .sum();
  }

  /** What {@link #awsCreds} becomes once vended: an AWS credential with the given access key. */
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
    when(aws.vendAwsCredentials(any())).thenReturn(awsCreds("AK", T0 + ONE_HOUR_MS));
    CachingCloudCredentialVendor cache = cache();

    cache.vendCredential(ctx("arn:role/A"));
    TemporaryCredentials hit = cache.vendCredential(ctx("arn:role/A"));

    // The hit equals the vended AWS credential exactly, with no other cloud's fields added.
    assertEquals(creds("AK", T0 + ONE_HOUR_MS), hit);
    verify(aws, times(1)).vendAwsCredentials(any());
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
    vendDistinctCredentials();
    CachingCloudCredentialVendor cache = cache();

    TemporaryCredentials first = cache.vendCredential(original);
    TemporaryCredentials second = cache.vendCredential(updated);
    assertNotEquals(first, second);
    assertEquals(first, cache.vendCredential(original));
    assertEquals(second, cache.vendCredential(updated));
    assertEquals(2, vends());
  }

  @ParameterizedTest
  @ValueSource(strings = {"gs://bucket/tableG", "abfs://container@account.dfs.core.windows.net/t"})
  void perBucketBindingIsCachedPerLocation(String location) {
    // Per-bucket config vends have no credential DAO, so the role fields in the key are null.
    NormalizedURL url = NormalizedURL.from(location);
    NormalizedURL other = NormalizedURL.from(location + "/other");
    vendDistinctCredentials();
    CachingCloudCredentialVendor cache = cache();

    cache.vendCredential(CredentialContext.create(url, READ_ONLY, Optional.empty()));
    cache.vendCredential(CredentialContext.create(url, READ_ONLY, Optional.empty()));
    assertEquals(1, vends());

    cache.vendCredential(CredentialContext.create(other, READ_ONLY, Optional.empty()));
    assertEquals(2, vends());
  }

  @Test
  void credentialNearItsExpiryIsReplacedAndTheReplacementIsReused() {
    // The first credential expires at T0+120s; with the 60s margin, reuse stops at T0+60s.
    when(aws.vendAwsCredentials(any()))
        .thenReturn(awsCreds("AK_OLD", T0 + 120_000L))
        .thenReturn(awsCreds("AK_NEW", T0 + ONE_HOUR_MS));
    CachingCloudCredentialVendor cache = cache();

    assertEquals("AK_OLD", accessKey(cache.vendCredential(ctx("arn:role/A"))));
    tick(T0 + 59_000L); // before the margin: served from the cache
    assertEquals("AK_OLD", accessKey(cache.vendCredential(ctx("arn:role/A"))));
    verify(aws, times(1)).vendAwsCredentials(any());

    tick(T0 + 61_000L); // inside the margin: vend and cache the replacement
    TemporaryCredentials renewed = cache.vendCredential(ctx("arn:role/A"));
    assertEquals("AK_NEW", accessKey(renewed));
    assertEquals(T0 + ONE_HOUR_MS, renewed.getExpirationTime());

    tick(T0 + 62_000L); // the replacement is served without a third vend
    assertEquals("AK_NEW", accessKey(cache.vendCredential(ctx("arn:role/A"))));
    verify(aws, times(2)).vendAwsCredentials(any());
  }

  @ParameterizedTest(
      name = "max age {0}, lifetime {1}ms, now = T0 + {2}ms -> served from cache: {3}")
  @CsvSource({
    // a 2-minute credential: the margin ends reuse at T0 + 60s, before the 5-minute max age
    "PT5M, 120000, 59999, true",
    "PT5M, 120000, 60000, false",
    // max age as long as a 1-hour credential: the margin ends reuse 1 minute before expiry
    "PT1H, 3600000, 3539999, true",
    "PT1H, 3600000, 3540000, false",
  })
  void expiryMarginBoundary(
      String maxAge, long lifetimeMs, long elapsedMs, boolean servedFromCache) {
    when(aws.vendAwsCredentials(any())).thenReturn(awsCreds("AK", T0 + lifetimeMs));
    CachingCloudCredentialVendor cache = cache("server.storage-credential-cache.max-age", maxAge);

    cache.vendCredential(ctx("arn:role/A"));
    tick(T0 + elapsedMs);
    cache.vendCredential(ctx("arn:role/A"));

    verify(aws, times(servedFromCache ? 1 : 2)).vendAwsCredentials(any());
  }

  @ParameterizedTest(name = "now = T0 + {0}ms -> served from cache: {1}")
  @CsvSource({
    // max age 5 minutes: stale from T0 + 300s, although the credential is valid for an hour
    "299999, true",
    "300000, false",
  })
  void maxAgeBoundary(long elapsedMs, boolean servedFromCache) {
    when(aws.vendAwsCredentials(any())).thenReturn(awsCreds("AK", T0 + ONE_HOUR_MS));
    CachingCloudCredentialVendor cache = cache("server.storage-credential-cache.max-age", "PT5M");

    cache.vendCredential(ctx("arn:role/A"));
    tick(T0 + elapsedMs);
    cache.vendCredential(ctx("arn:role/A"));

    verify(aws, times(servedFromCache ? 1 : 2)).vendAwsCredentials(any());
  }

  @ParameterizedTest
  @NullSource
  @ValueSource(longs = T0 + ONE_HOUR_MS)
  void maxAgeCapsReuse(Long credentialExpiry) {
    // Applies both to a static credential with no expiry and to one valid for an hour.
    when(aws.vendAwsCredentials(any())).thenReturn(awsCreds("AK", credentialExpiry));
    CachingCloudCredentialVendor cache = cache("server.storage-credential-cache.max-age", "PT5M");

    cache.vendCredential(ctx("arn:role/A"));
    tick(T0 + 299_000L);
    cache.vendCredential(ctx("arn:role/A"));
    verify(aws, times(1)).vendAwsCredentials(any());

    tick(T0 + 301_000L);
    cache.vendCredential(ctx("arn:role/A"));
    verify(aws, times(2)).vendAwsCredentials(any());
  }

  @ParameterizedTest(name = "lifetime {0}ms -> cached: {1}")
  @CsvSource({
    // A credential that lives no longer than the 60s margin is returned, but never cached.
    "30000, false",
    "60000, false",
    "60001, true",
  })
  void credentialLivingNoLongerThanTheMarginIsNotCached(long lifetimeMs, boolean cached) {
    when(aws.vendAwsCredentials(any())).thenReturn(awsCreds("AK", T0 + lifetimeMs));
    CachingCloudCredentialVendor cache = cache();

    assertEquals("AK", accessKey(cache.vendCredential(ctx("arn:role/A"))));
    cache.vendCredential(ctx("arn:role/A"));

    verify(aws, times(cached ? 1 : 2)).vendAwsCredentials(any());
  }

  @Test
  void customExpiryMarginEndsReuseThatLongBeforeExpiry() {
    // A 10s margin on a credential expiring at T0 + 120s: reuse ends at T0 + 110s.
    when(aws.vendAwsCredentials(any())).thenReturn(awsCreds("AK", T0 + 120_000L));
    CachingCloudCredentialVendor cache =
        new CachingCloudCredentialVendor(
            aws, azure, gcp, new ServerProperties(new Properties()), clock, Duration.ofSeconds(10));

    cache.vendCredential(ctx("arn:role/A"));
    tick(T0 + 109_999L);
    cache.vendCredential(ctx("arn:role/A"));
    verify(aws, times(1)).vendAwsCredentials(any());

    tick(T0 + 110_000L);
    cache.vendCredential(ctx("arn:role/A"));
    verify(aws, times(2)).vendAwsCredentials(any());
  }

  /**
   * Simulates a concurrent miss without threads: while the outer vend is in flight, a nested
   * request for the same key vends and caches a 1-hour credential. The outer vend then finishes
   * with a credential that expires at {@code T0 + outerLifetimeMs}.
   */
  private CachingCloudCredentialVendor cacheWithConcurrentMiss(long outerLifetimeMs) {
    CachingCloudCredentialVendor[] cache = new CachingCloudCredentialVendor[1];
    when(aws.vendAwsCredentials(any()))
        .thenAnswer(
            invocation -> {
              cache[0].vendCredential(ctx("arn:role/A"));
              return awsCreds("AK_OUTER", T0 + outerLifetimeMs);
            })
        .thenReturn(awsCreds("AK_NESTED", T0 + ONE_HOUR_MS))
        .thenReturn(awsCreds("AK_FRESH", T0 + ONE_HOUR_MS));
    cache[0] = cache();
    cache[0].vendCredential(ctx("arn:role/A"));
    return cache[0];
  }

  @Test
  void credentialPastItsDeadlineDoesNotReplaceAConcurrentlyCachedOne() {
    // The outer credential expires within the 60s margin, so the nested one stays cached.
    CachingCloudCredentialVendor cache = cacheWithConcurrentMiss(30_000L);

    assertEquals("AK_NESTED", accessKey(cache.vendCredential(ctx("arn:role/A"))));
    verify(aws, times(2)).vendAwsCredentials(any());
  }

  @Test
  void replacingAnEntryAppliesTheReplacementsOwnDeadline() {
    // The outer credential expires at T0 + 180s, so it replaces the nested one and is served
    // only until T0 + 120s, not until the nested entry's T0 + 5m.
    CachingCloudCredentialVendor cache = cacheWithConcurrentMiss(180_000L);

    tick(T0 + 119_999L);
    assertEquals("AK_OUTER", accessKey(cache.vendCredential(ctx("arn:role/A"))));
    tick(T0 + 120_000L);
    assertEquals("AK_FRESH", accessKey(cache.vendCredential(ctx("arn:role/A"))));
    verify(aws, times(3)).vendAwsCredentials(any());
  }

  @Test
  void maxAgeCountsFromBeforeTheCloudCall() {
    // The first vend takes 2 minutes. Max age counts from its start, so reuse ends at T0 + 5m,
    // not at T0 + 7m.
    when(aws.vendAwsCredentials(any()))
        .thenAnswer(
            invocation -> {
              tick(T0 + 120_000L);
              return awsCreds("AK", T0 + ONE_HOUR_MS);
            })
        .thenReturn(awsCreds("AK", T0 + ONE_HOUR_MS));
    CachingCloudCredentialVendor cache = cache("server.storage-credential-cache.max-age", "PT5M");

    cache.vendCredential(ctx("arn:role/A"));
    tick(T0 + 299_999L);
    cache.vendCredential(ctx("arn:role/A"));
    verify(aws, times(1)).vendAwsCredentials(any());

    tick(T0 + 300_000L);
    cache.vendCredential(ctx("arn:role/A"));
    verify(aws, times(2)).vendAwsCredentials(any());
  }

  @Test
  void failedVendIsNotCachedAndTheNextRequestVends() {
    when(aws.vendAwsCredentials(any()))
        .thenThrow(new IllegalStateException("cloud unavailable"))
        .thenReturn(awsCreds("AK", T0 + ONE_HOUR_MS));
    CachingCloudCredentialVendor cache = cache();

    assertThrows(IllegalStateException.class, () -> cache.vendCredential(ctx("arn:role/A")));
    assertEquals("AK", accessKey(cache.vendCredential(ctx("arn:role/A"))));
    assertEquals("AK", accessKey(cache.vendCredential(ctx("arn:role/A"))));

    verify(aws, times(2)).vendAwsCredentials(any());
  }

  @Test
  void staleCredentialIsNotServedWhenItsRenewalFails() {
    when(aws.vendAwsCredentials(any()))
        .thenReturn(awsCreds("AK_OLD", T0 + 120_000L))
        .thenThrow(new IllegalStateException("cloud unavailable"))
        .thenReturn(awsCreds("AK_NEW", T0 + ONE_HOUR_MS));
    CachingCloudCredentialVendor cache = cache();

    cache.vendCredential(ctx("arn:role/A"));
    tick(T0 + 61_000L); // AK_OLD is inside the margin before its expiry

    assertThrows(IllegalStateException.class, () -> cache.vendCredential(ctx("arn:role/A")));
    assertEquals("AK_NEW", accessKey(cache.vendCredential(ctx("arn:role/A"))));
    verify(aws, times(3)).vendAwsCredentials(any());
  }

  static Stream<Arguments> vendedPerCloud() {
    return Stream.of(
        Arguments.of("s3://bucket/t", creds("AK1", T0 + ONE_HOUR_MS)),
        Arguments.of(
            "gs://bucket/t",
            new TemporaryCredentials()
                .gcpOauthToken(new GcpOauthToken().oauthToken("TOKEN1"))
                .expirationTime(T0 + ONE_HOUR_MS)),
        Arguments.of(
            "abfs://container@account.dfs.core.windows.net/t",
            new TemporaryCredentials()
                .azureUserDelegationSas(new AzureUserDelegationSAS().sasToken("SAS1"))
                .expirationTime(T0 + ONE_HOUR_MS)));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("vendedPerCloud")
  void cachedCredentialIsACompleteCopyThatCallersCannotChange(
      String location, TemporaryCredentials expected) {
    vendDistinctCredentials();
    CachingCloudCredentialVendor cache = cache();
    CredentialContext context = perBucket(location);

    TemporaryCredentials vended = cache.vendCredential(context);
    assertEquals(expected, vended);
    scramble(vended); // the caller changes the credential it was vended
    TemporaryCredentials hit = cache.vendCredential(context);
    assertEquals(expected, hit);
    scramble(hit); // and the one it was served from the cache
    assertEquals(expected, cache.vendCredential(context));
    assertEquals(1, vends());
  }

  /** Changes every field a caller can reach, including the nested cloud credential. */
  private static void scramble(TemporaryCredentials credentials) {
    credentials.setExpirationTime(0L);
    if (credentials.getAwsTempCredentials() != null) {
      credentials.getAwsTempCredentials().setAccessKeyId("CHANGED");
    }
    if (credentials.getAzureUserDelegationSas() != null) {
      credentials.getAzureUserDelegationSas().setSasToken("CHANGED");
    }
    if (credentials.getGcpOauthToken() != null) {
      credentials.getGcpOauthToken().setOauthToken("CHANGED");
    }
  }

  @Test
  void changingTheCallersPrivilegeSetDoesNotChangeTheCachedKey() {
    when(aws.vendAwsCredentials(any())).thenReturn(awsCreds("AK", T0 + ONE_HOUR_MS));
    CachingCloudCredentialVendor cache = cache();
    Set<CredentialContext.Privilege> privileges = new HashSet<>(READ_ONLY);

    cache.vendCredential(CredentialContext.create(LOC, privileges, Optional.empty()));
    privileges.add(CredentialContext.Privilege.UPDATE);
    cache.vendCredential(CredentialContext.create(LOC, READ_ONLY, Optional.empty()));

    verify(aws, times(1)).vendAwsCredentials(any());
  }

  @Test
  void maxSizeBoundsTheNumberOfCachedCredentials() {
    when(aws.vendAwsCredentials(any())).thenReturn(awsCreds("AK", T0 + ONE_HOUR_MS));
    CachingCloudCredentialVendor cache = cache("server.storage-credential-cache.max-size", "1");

    cache.vendCredential(ctx("arn:role/A"));
    cache.vendCredential(ctx("arn:role/B"));
    cache.vendCredential(ctx("arn:role/A"));
    cache.vendCredential(ctx("arn:role/B"));

    // An unbounded cache would vend twice; with one slot at least one repeat is a miss.
    verify(aws, atLeast(3)).vendAwsCredentials(any());
  }

  @Test
  void concurrentMissesVendIndependentlyWithoutHoldingALock() throws Exception {
    // Each vend waits until all of them have started, which only succeeds if no lock is held
    // across the cloud call. Every caller gets the vended credential.
    int callers = 4;
    CountDownLatch allVending = new CountDownLatch(callers);
    when(aws.vendAwsCredentials(any()))
        .thenAnswer(
            invocation -> {
              allVending.countDown();
              assertThat(allVending.await(10, TimeUnit.SECONDS)).isTrue();
              return awsCreds("AK", T0 + ONE_HOUR_MS);
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
    verify(aws, times(callers)).vendAwsCredentials(any());
  }

  @Test
  void concurrentHitsOnOneCredentialEachGetAnIndependentCopy() throws Exception {
    when(aws.vendAwsCredentials(any())).thenReturn(awsCreds("AK", T0 + ONE_HOUR_MS));
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
    verify(aws, times(1)).vendAwsCredentials(any());
  }

  @Test
  void concurrentRequestsDuringRenewalAreNeverServedTheStaleCredential() throws Exception {
    // The first vend returns AK_OLD; every later vend returns AK_NEW.
    AtomicInteger vends = new AtomicInteger();
    when(aws.vendAwsCredentials(any()))
        .thenAnswer(
            invocation ->
                vends.getAndIncrement() == 0
                    ? awsCreds("AK_OLD", T0 + 120_000L)
                    : awsCreds("AK_NEW", T0 + ONE_HOUR_MS));
    CachingCloudCredentialVendor cache = cache();
    CredentialContext context = ctx("arn:role/A");
    cache.vendCredential(context);
    tick(T0 + 61_000L); // AK_OLD is now inside the margin before its expiry

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
