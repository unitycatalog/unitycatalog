package io.unitycatalog.server.service.credential.cache;

import static io.unitycatalog.server.service.credential.CredentialContext.READ_ONLY;
import static io.unitycatalog.server.service.credential.CredentialContext.READ_WRITE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.unitycatalog.server.model.AwsCredentials;
import io.unitycatalog.server.model.AwsIamRoleResponse;
import io.unitycatalog.server.model.TemporaryCredentials;
import io.unitycatalog.server.persist.dao.CredentialDAO;
import io.unitycatalog.server.service.credential.CloudCredentialVendor;
import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.ServerProperties;
import io.unitycatalog.server.utils.cache.Cache;
import java.time.Clock;
import java.time.Instant;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

public class StorageCredentialCacheTest {

  private static final NormalizedURL LOC = NormalizedURL.from("s3://bucket/tableA");
  private static final long T0 = 1_000_000_000_000L; // fixed "now" for the injected clock

  private static ServerProperties props(String... kv) {
    Properties p = new Properties();
    for (int i = 0; i < kv.length; i += 2) {
      p.setProperty(kv[i], kv[i + 1]);
    }
    return new ServerProperties(p);
  }

  /** A clock pinned to {@code epochMs}; advance a test by re-stubbing {@link #tick}. */
  private static Clock clockAt(long epochMs) {
    Clock clock = mock(Clock.class);
    when(clock.instant()).thenReturn(Instant.ofEpochMilli(epochMs));
    return clock;
  }

  private static void tick(Clock clock, long epochMs) {
    when(clock.instant()).thenReturn(Instant.ofEpochMilli(epochMs));
  }

  /** A cloud vend result expiring at {@code expiryEpochMs} (null = static credential, no T1). */
  private static TemporaryCredentials creds(Long expiryEpochMs) {
    TemporaryCredentials c =
        new TemporaryCredentials()
            .awsTempCredentials(
                new AwsCredentials().accessKeyId("AK").secretAccessKey("SK").sessionToken("TK"));
    if (expiryEpochMs != null) {
      c.expirationTime(expiryEpochMs);
    }
    return c;
  }

  /** A READ_ONLY context for LOC with an AWS credential DAO carrying the given role ARN. */
  private static CredentialContext ctx(String roleArn) {
    return ctx(roleArn, READ_ONLY);
  }

  /** A context for LOC with the given role ARN and privilege set. */
  private static CredentialContext ctx(
      String roleArn, Set<CredentialContext.Privilege> privileges) {
    return ctx(roleArn, null, privileges);
  }

  private static CredentialContext ctx(
      String roleArn, String externalId, Set<CredentialContext.Privilege> privileges) {
    CredentialDAO dao = mock(CredentialDAO.class);
    when(dao.getAwsIamRoleResponse())
        .thenReturn(new AwsIamRoleResponse().roleArn(roleArn).externalId(externalId));
    return CredentialContext.create(LOC, privileges, Optional.of(dao));
  }

  @Test
  void secondCallForSameBindingSkipsTheVend() {
    CloudCredentialVendor vendor = mock(CloudCredentialVendor.class);
    when(vendor.vendCredential(any())).thenReturn(creds(T0 + 3_600_000L)); // expires 1h out → fresh
    StorageCredentialCache cache = new StorageCredentialCache(vendor, props(), clockAt(T0));

    cache.get(ctx("arn:role/A"));
    cache.get(ctx("arn:role/A"));

    verify(vendor, times(1)).vendCredential(any()); // vended once, served from cache once
  }

  @Test
  void differentRoleForSameLocationReVends() {
    CloudCredentialVendor vendor = mock(CloudCredentialVendor.class);
    when(vendor.vendCredential(any())).thenReturn(creds(T0 + 3_600_000L));
    StorageCredentialCache cache = new StorageCredentialCache(vendor, props(), clockAt(T0));

    cache.get(ctx("arn:role/A"));
    cache.get(ctx("arn:role/B")); // rebind to a different role → different key → miss

    verify(vendor, times(2)).vendCredential(any());
  }

  @ParameterizedTest
  @NullSource
  @ValueSource(strings = "external-B")
  void differentExternalIdForSameRoleReVends(String updatedExternalId) {
    CredentialContext original = ctx("arn:role/A", "external-A", READ_ONLY);
    CredentialContext updated = ctx("arn:role/A", updatedExternalId, READ_ONLY);
    TemporaryCredentials first = creds(T0 + 3_600_000L);
    first.getAwsTempCredentials().setAccessKeyId("AK_A");
    TemporaryCredentials second = creds(T0 + 3_600_000L);
    second.getAwsTempCredentials().setAccessKeyId("AK_B");
    CloudCredentialVendor vendor = mock(CloudCredentialVendor.class);
    when(vendor.vendCredential(original)).thenReturn(first);
    when(vendor.vendCredential(updated)).thenReturn(second);
    StorageCredentialCache cache = new StorageCredentialCache(vendor, props(), clockAt(T0));

    assertEquals("AK_A", cache.get(original).getAwsTempCredentials().getAccessKeyId());
    assertEquals("AK_A", cache.get(original).getAwsTempCredentials().getAccessKeyId());
    assertEquals("AK_B", cache.get(updated).getAwsTempCredentials().getAccessKeyId());
    assertEquals("AK_B", cache.get(updated).getAwsTempCredentials().getAccessKeyId());
    assertEquals("AK_A", cache.get(original).getAwsTempCredentials().getAccessKeyId());
    verify(vendor, times(1)).vendCredential(original);
    verify(vendor, times(1)).vendCredential(updated);
  }

  @Test
  void differentPrivilegesForSameBindingReVends() {
    // Privileges are part of the key and must match exactly: a READ_ONLY credential must never be
    // served for a READ_WRITE request (insufficient access), nor vice versa (privilege escalation).
    CloudCredentialVendor vendor = mock(CloudCredentialVendor.class);
    when(vendor.vendCredential(any())).thenReturn(creds(T0 + 3_600_000L));
    StorageCredentialCache cache = new StorageCredentialCache(vendor, props(), clockAt(T0));

    cache.get(ctx("arn:role/A", READ_ONLY));
    cache.get(ctx("arn:role/A", READ_WRITE)); // exact privilege set differs → miss

    verify(vendor, times(2)).vendCredential(any());
  }

  @Test
  void disabledBypassesCacheAndVendsEveryCall() {
    CloudCredentialVendor vendor = mock(CloudCredentialVendor.class);
    when(vendor.vendCredential(any())).thenReturn(creds(T0 + 3_600_000L));
    StorageCredentialCache cache =
        new StorageCredentialCache(
            vendor, props("server.storage-credential-cache.enabled", "false"));

    cache.get(ctx("arn:role/A"));
    cache.get(ctx("arn:role/A"));

    verify(vendor, times(2)).vendCredential(any());
  }

  @Test
  void credentialWithinRenewalLeadIsRefetched() {
    // Credential expires at T0+120s; lead=60s → renewal window opens at T0+60s.
    CloudCredentialVendor vendor = mock(CloudCredentialVendor.class);
    when(vendor.vendCredential(any())).thenReturn(creds(T0 + 120_000L));
    Clock clock = clockAt(T0);
    StorageCredentialCache cache =
        new StorageCredentialCache(
            vendor,
            props(
                "server.storage-credential-cache.renewal-lead-time", "PT1M",
                "server.storage-credential-cache.max-age", "PT5M"),
            clock);

    cache.get(ctx("arn:role/A")); // vend 1
    tick(clock, T0 + 59_000L); // 1s before the lead window → still fresh
    cache.get(ctx("arn:role/A"));
    verify(vendor, times(1)).vendCredential(any()); // served from cache

    tick(clock, T0 + 61_000L); // 1s inside the lead window → refetch
    cache.get(ctx("arn:role/A"));
    verify(vendor, times(2)).vendCredential(any());
  }

  @Test
  void gcsPerBucketBindingIsCached() {
    // GCS has no DB credential (static per-bucket config) → null roleArn. The key is (location,
    // GS, privileges); it must still cache and set the url correctly.
    NormalizedURL gcs = NormalizedURL.from("gs://bucket/tableG");
    CloudCredentialVendor vendor = mock(CloudCredentialVendor.class);
    when(vendor.vendCredential(any())).thenReturn(creds(T0 + 3_600_000L));
    StorageCredentialCache cache = new StorageCredentialCache(vendor, props(), clockAt(T0));

    TemporaryCredentials first =
        cache.get(CredentialContext.create(gcs, READ_ONLY, Optional.empty()));
    cache.get(CredentialContext.create(gcs, READ_ONLY, Optional.empty()));

    assertEquals(gcs.toString(), first.getUrl());
    verify(vendor, times(1)).vendCredential(any()); // null-roleArn key still caches
  }

  @Test
  void azurePerBucketBindingIsCached() {
    // ABFS per-bucket config → null roleArn; scheme is derived as ABFS from the location.
    NormalizedURL abfs = NormalizedURL.from("abfs://container@account.dfs.core.windows.net/tableA");
    CloudCredentialVendor vendor = mock(CloudCredentialVendor.class);
    when(vendor.vendCredential(any())).thenReturn(creds(T0 + 3_600_000L));
    StorageCredentialCache cache = new StorageCredentialCache(vendor, props(), clockAt(T0));

    TemporaryCredentials first =
        cache.get(CredentialContext.create(abfs, READ_ONLY, Optional.empty()));
    cache.get(CredentialContext.create(abfs, READ_ONLY, Optional.empty()));

    assertEquals(abfs.toString(), first.getUrl());
    verify(vendor, times(1)).vendCredential(any());
  }

  @Test
  void differentLocationsForSamePerBucketSchemeReVend() {
    // Two GCS locations (both null-roleArn) are distinct keys → two vends, no cross-serving.
    CloudCredentialVendor vendor = mock(CloudCredentialVendor.class);
    when(vendor.vendCredential(any())).thenReturn(creds(T0 + 3_600_000L));
    StorageCredentialCache cache = new StorageCredentialCache(vendor, props(), clockAt(T0));

    NormalizedURL a = NormalizedURL.from("gs://bucket/a");
    NormalizedURL b = NormalizedURL.from("gs://bucket/b");
    cache.get(CredentialContext.create(a, READ_ONLY, Optional.empty()));
    cache.get(CredentialContext.create(b, READ_ONLY, Optional.empty()));

    verify(vendor, times(2)).vendCredential(any());
  }

  @Test
  void returnedCredentialHasUrlSet() {
    CloudCredentialVendor vendor = mock(CloudCredentialVendor.class);
    when(vendor.vendCredential(any())).thenReturn(creds(T0 + 3_600_000L));
    StorageCredentialCache cache = new StorageCredentialCache(vendor, props(), clockAt(T0));

    TemporaryCredentials out = cache.get(ctx("arn:role/A"));
    assertEquals(LOC.toString(), out.getUrl());
  }

  @Test
  void staticCredentialWithoutExpiryIsCachedUntilMaxAge() {
    // A static credential has no own expiry (T1 == null), so freshness is bounded solely by the
    // cache max-age (T2 = vendedAt + max-age): served from cache until T2, then re-vended.
    CloudCredentialVendor vendor = mock(CloudCredentialVendor.class);
    when(vendor.vendCredential(any())).thenReturn(creds(null)); // no expirationTime
    Clock clock = clockAt(T0);
    StorageCredentialCache cache =
        new StorageCredentialCache(
            vendor, props("server.storage-credential-cache.max-age", "PT5M"), clock);

    cache.get(ctx("arn:role/A")); // vend 1; T2 = T0 + 5m
    tick(clock, T0 + 299_000L); // 1s before T2 → served from cache
    cache.get(ctx("arn:role/A"));
    verify(vendor, times(1)).vendCredential(any());

    tick(clock, T0 + 301_000L); // 1s past T2 → re-vend
    cache.get(ctx("arn:role/A"));
    verify(vendor, times(2)).vendCredential(any());
  }

  // ---- test-double backends (loaded reflectively by fqcn, like s3.credentialGenerator) ----

  /** Records the injected context and counts traffic; typed-context constructor path. */
  public static class RecordingStore implements Cache<CredentialCacheKey, CachedCredential> {
    static volatile CredentialCacheStoreContext lastContext;
    static final java.util.concurrent.atomic.AtomicInteger getCount =
        new java.util.concurrent.atomic.AtomicInteger();
    static final java.util.concurrent.atomic.AtomicInteger putCount =
        new java.util.concurrent.atomic.AtomicInteger();
    private final java.util.Map<CredentialCacheKey, CachedCredential> map =
        new java.util.concurrent.ConcurrentHashMap<>();

    public RecordingStore(CredentialCacheStoreContext ctx) {
      lastContext = ctx;
    }

    static void reset() {
      lastContext = null;
      getCount.set(0);
      putCount.set(0);
    }

    public java.util.Optional<CachedCredential> getIfPresent(CredentialCacheKey k) {
      getCount.incrementAndGet();
      return java.util.Optional.ofNullable(map.get(k));
    }

    public void put(CredentialCacheKey k, CachedCredential v) {
      putCount.incrementAndGet();
      map.put(k, v);
    }

    public void invalidate(CredentialCacheKey k) {
      map.remove(k);
    }
  }

  /** No-arg constructor only — exercises the reflective fallback. */
  public static class NoArgStore implements Cache<CredentialCacheKey, CachedCredential> {
    static volatile boolean constructed;

    public NoArgStore() {
      constructed = true;
    }

    public java.util.Optional<CachedCredential> getIfPresent(CredentialCacheKey k) {
      return java.util.Optional.empty();
    }

    public void put(CredentialCacheKey k, CachedCredential v) {}

    public void invalidate(CredentialCacheKey k) {}
  }

  /** Context-constructor always throws — exercises the fail-closed path on construction. */
  public static class ThrowingCtorStore implements Cache<CredentialCacheKey, CachedCredential> {
    public ThrowingCtorStore(CredentialCacheStoreContext ctx) {
      throw new RuntimeException("bad endpoint");
    }

    public java.util.Optional<CachedCredential> getIfPresent(CredentialCacheKey k) {
      return java.util.Optional.empty();
    }

    public void put(CredentialCacheKey k, CachedCredential v) {}

    public void invalidate(CredentialCacheKey k) {}
  }

  /** Has both constructors; the context-constructor should be preferred. */
  public static class BothCtorsStore implements Cache<CredentialCacheKey, CachedCredential> {
    static volatile boolean usedContextCtor;

    public BothCtorsStore() {
      usedContextCtor = false;
    }

    public BothCtorsStore(CredentialCacheStoreContext ctx) {
      usedContextCtor = true;
    }

    public java.util.Optional<CachedCredential> getIfPresent(CredentialCacheKey k) {
      return java.util.Optional.empty();
    }

    public void put(CredentialCacheKey k, CachedCredential v) {}

    public void invalidate(CredentialCacheKey k) {}
  }

  /** Always throws — exercises the facade's FailSafe guarantee. */
  public static class ThrowingStore implements Cache<CredentialCacheKey, CachedCredential> {
    public ThrowingStore(CredentialCacheStoreContext ctx) {}

    public java.util.Optional<CachedCredential> getIfPresent(CredentialCacheKey k) {
      throw new RuntimeException("backend down");
    }

    public void put(CredentialCacheKey k, CachedCredential v) {
      throw new RuntimeException("backend down");
    }

    public void invalidate(CredentialCacheKey k) {}
  }

  /** Implements Cache but has neither a context ctor nor a no-arg ctor -> reflection fails. */
  public static class NoUsableCtorStore implements Cache<CredentialCacheKey, CachedCredential> {
    public NoUsableCtorStore(String unrelated) {}

    public java.util.Optional<CachedCredential> getIfPresent(CredentialCacheKey k) {
      return java.util.Optional.empty();
    }

    public void put(CredentialCacheKey k, CachedCredential v) {}

    public void invalidate(CredentialCacheKey k) {}
  }

  private static String backend(Class<?> c) {
    return c.getName();
  }

  @org.junit.jupiter.api.BeforeEach
  void resetBackends() {
    RecordingStore.reset();
    NoArgStore.constructed = false;
    BothCtorsStore.usedContextCtor = false;
  }

  @Test
  void customBackendReceivesTrafficAndContext() {
    CloudCredentialVendor vendor = mock(CloudCredentialVendor.class);
    when(vendor.vendCredential(any())).thenReturn(creds(T0 + 3_600_000L));
    java.time.Clock clock = clockAt(T0);
    StorageCredentialCache cache =
        new StorageCredentialCache(
            vendor,
            props(
                "server.storage-credential-cache.backend", backend(RecordingStore.class),
                "server.storage-credential-cache.backend.endpoint", "redis://h:6379",
                "server.storage-credential-cache.max-size", "1000"),
            clock);

    cache.get(ctx("arn:role/A")); // miss -> vend + put
    cache.get(ctx("arn:role/A")); // hit -> served from the custom store, no re-vend

    verify(vendor, times(1)).vendCredential(any());
    assertEquals(1, RecordingStore.putCount.get());
    // Context carries the configured max-size and only the backend.* subtree.
    assertEquals(1000, RecordingStore.lastContext.maxSize());
    assertEquals("redis://h:6379", RecordingStore.lastContext.backendProperties().get("endpoint"));
    assertEquals(1, RecordingStore.lastContext.backendProperties().size());
    // The injected clock instance must reach the store unchanged.
    assertSame(clock, RecordingStore.lastContext.clock());
  }

  @Test
  void customBackendNoArgConstructorFallback() {
    CloudCredentialVendor vendor = mock(CloudCredentialVendor.class);
    when(vendor.vendCredential(any())).thenReturn(creds(T0 + 3_600_000L));
    StorageCredentialCache cache =
        new StorageCredentialCache(
            vendor,
            props("server.storage-credential-cache.backend", backend(NoArgStore.class)),
            clockAt(T0));

    cache.get(
        ctx("arn:role/A")); // NoArgStore never hits -> vends every time, but must construct+run
    assertTrue(NoArgStore.constructed);
  }

  @Test
  void throwingBackendDegradesToLiveVend() {
    CloudCredentialVendor vendor = mock(CloudCredentialVendor.class);
    when(vendor.vendCredential(any())).thenReturn(creds(T0 + 3_600_000L));
    StorageCredentialCache cache =
        new StorageCredentialCache(
            vendor,
            props("server.storage-credential-cache.backend", backend(ThrowingStore.class)),
            clockAt(T0));

    // FailSafe turns the store's exception into a miss -> the request still succeeds via a vend.
    assertEquals(LOC.toString(), cache.get(ctx("arn:role/A")).getUrl());
    verify(vendor, times(1)).vendCredential(any());
  }

  @Test
  void unknownBackendClassFailsClosedNamingTheFqcn() {
    CloudCredentialVendor vendor = mock(CloudCredentialVendor.class);
    IllegalStateException ex =
        assertThrows(
            IllegalStateException.class,
            () ->
                new StorageCredentialCache(
                    vendor,
                    props("server.storage-credential-cache.backend", "com.acme.DoesNotExist"),
                    clockAt(T0)));
    assertTrue(ex.getMessage().contains("com.acme.DoesNotExist"));
  }

  @Test
  void nonCacheBackendClassFailsClosed() {
    CloudCredentialVendor vendor = mock(CloudCredentialVendor.class);
    assertThrows(
        IllegalStateException.class,
        () ->
            new StorageCredentialCache(
                vendor,
                props("server.storage-credential-cache.backend", String.class.getName()),
                clockAt(T0)));
  }

  @Test
  void backendWithNoUsableConstructorFailsClosed() {
    CloudCredentialVendor vendor = mock(CloudCredentialVendor.class);
    assertThrows(
        IllegalStateException.class,
        () ->
            new StorageCredentialCache(
                vendor,
                props("server.storage-credential-cache.backend", backend(NoUsableCtorStore.class)),
                clockAt(T0)));
  }

  @Test
  void disabledCacheIgnoresBackend() {
    CloudCredentialVendor vendor = mock(CloudCredentialVendor.class);
    when(vendor.vendCredential(any())).thenReturn(creds(T0 + 3_600_000L));
    StorageCredentialCache cache =
        new StorageCredentialCache(
            vendor,
            props(
                "server.storage-credential-cache.enabled",
                "false",
                "server.storage-credential-cache.backend",
                backend(RecordingStore.class)));

    cache.get(ctx("arn:role/A"));
    cache.get(ctx("arn:role/A"));

    verify(vendor, times(2)).vendCredential(any()); // disabled -> vends every call
    assertNull(RecordingStore.lastContext); // backend never constructed
  }

  @Test
  void backendConstructorThatThrowsFailsClosed() {
    CloudCredentialVendor vendor = mock(CloudCredentialVendor.class);
    IllegalStateException ex =
        assertThrows(
            IllegalStateException.class,
            () ->
                new StorageCredentialCache(
                    vendor,
                    props(
                        "server.storage-credential-cache.backend",
                        backend(ThrowingCtorStore.class)),
                    clockAt(T0)));
    assertTrue(ex.getMessage().contains(ThrowingCtorStore.class.getName()));
  }

  @Test
  void bothCtorsContextCtorPreferred() {
    CloudCredentialVendor vendor = mock(CloudCredentialVendor.class);
    when(vendor.vendCredential(any())).thenReturn(creds(T0 + 3_600_000L));
    new StorageCredentialCache(
        vendor,
        props("server.storage-credential-cache.backend", backend(BothCtorsStore.class)),
        clockAt(T0));
    assertTrue(BothCtorsStore.usedContextCtor);
  }

  @Test
  void staleEntryFromCustomStoreIsRevalidated() {
    // Credential expires at T0+120s; with renewal-lead=1m it is fresh at T0 but stale at T0+121s.
    // The RecordingStore never expires entries on its own, so the stale value comes back from the
    // store — the facade must re-validate it and re-vend rather than trusting the store's answer.
    CloudCredentialVendor vendor = mock(CloudCredentialVendor.class);
    when(vendor.vendCredential(any())).thenReturn(creds(T0 + 120_000L));
    Clock clock = clockAt(T0);
    StorageCredentialCache cache =
        new StorageCredentialCache(
            vendor,
            props(
                "server.storage-credential-cache.backend", backend(RecordingStore.class),
                "server.storage-credential-cache.renewal-lead-time", "PT1M",
                "server.storage-credential-cache.max-age", "PT5M"),
            clock);

    cache.get(ctx("arn:role/A")); // vend 1; stored in RecordingStore
    tick(clock, T0 + 121_000L); // past expiry (T0+120s) + past renewal window (T0+60s)
    cache.get(ctx("arn:role/A")); // RecordingStore returns stale entry -> re-vend

    verify(vendor, times(2)).vendCredential(any());
    assertTrue(RecordingStore.getCount.get() > 0); // store WAS consulted; stale value came from it
  }
}
