package io.unitycatalog.hadoop.internal.auth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

import io.unitycatalog.client.internal.Clock;
import io.unitycatalog.hadoop.internal.auth.CredentialCache.RenewableCredential;
import io.unitycatalog.hadoop.internal.id.CredId;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class CredentialCacheTest {

  private static final long RENEWAL_LEAD_TIME_MILLIS = 60_000L;
  private static final Clock CLOCK = Clock.systemClock();

  @Test
  void validCachedCredentialSkipsFactory() throws Exception {
    CredentialCache cache = new CredentialCache(4);
    CredId credId = new TestCredId("scope-a");
    GenericCredential first = cache.access(credId, () -> validCredential());

    GenericCredential second =
        cache.access(
            credId, () -> fail("factory must not be invoked for a valid cached credential"));

    assertThat(second).isSameAs(first);
  }

  @Test
  void expiredCredentialIsRenewedAndReplaced() throws Exception {
    CredentialCache cache = new CredentialCache(4);
    CredId credId = new TestCredId("scope-a");
    AtomicInteger factoryCalls = new AtomicInteger();

    GenericCredential expired =
        cache.access(
            credId,
            () -> {
              factoryCalls.incrementAndGet();
              return expiredCredential();
            });
    GenericCredential renewed =
        cache.access(
            credId,
            () -> {
              factoryCalls.incrementAndGet();
              return validCredential();
            });

    assertThat(factoryCalls).hasValue(2);
    assertThat(renewed).isNotSameAs(expired);
    // The renewed credential is now cached and reused.
    GenericCredential third =
        cache.access(credId, () -> fail("factory must not be invoked after renewal"));
    assertThat(third).isSameAs(renewed);
  }

  @Test
  void renewalOfOneScopeDoesNotBlockOtherScopes() throws Exception {
    CredentialCache cache = new CredentialCache(4);
    CredId slowId = new TestCredId("scope-slow");
    CredId fastId = new TestCredId("scope-fast");

    CountDownLatch slowRenewalStarted = new CountDownLatch(1);
    CountDownLatch releaseSlowRenewal = new CountDownLatch(1);

    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      Future<GenericCredential> slow =
          executor.submit(
              () ->
                  cache.access(
                      slowId,
                      () -> {
                        slowRenewalStarted.countDown();
                        awaitOrFail(releaseSlowRenewal);
                        return validCredential();
                      }));
      assertThat(slowRenewalStarted.await(5, TimeUnit.SECONDS)).isTrue();

      // While scope-slow's renewal RPC is in flight, an access on a different scope must complete
      // instead of queueing behind the renewal.
      Future<GenericCredential> fast =
          executor.submit(() -> cache.access(fastId, CredentialCacheTest::validCredential));
      assertThat(fast.get(2, TimeUnit.SECONDS)).isNotNull();

      releaseSlowRenewal.countDown();
      assertThat(slow.get(5, TimeUnit.SECONDS)).isNotNull();
    } finally {
      releaseSlowRenewal.countDown();
      executor.shutdownNow();
    }
  }

  private static void awaitOrFail(CountDownLatch latch) {
    try {
      assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      fail("interrupted while waiting for latch");
    }
  }

  private static RenewableCredential validCredential() {
    return renewable(CLOCK.now().toEpochMilli() + 3_600_000L);
  }

  private static RenewableCredential expiredCredential() {
    return renewable(CLOCK.now().toEpochMilli());
  }

  private static RenewableCredential renewable(long expiredTimeMillis) {
    return new RenewableCredential(
        RENEWAL_LEAD_TIME_MILLIS,
        CLOCK,
        GenericCredential.forAws("access-key", "secret-key", "session-token", expiredTimeMillis));
  }

  private static final class TestCredId implements CredId {
    private final String name;

    TestCredId(String name) {
      this.name = name;
    }

    @Override
    public Map<String, String> props() {
      return Collections.emptyMap();
    }

    @Override
    public boolean equals(Object other) {
      return other instanceof TestCredId && ((TestCredId) other).name.equals(name);
    }

    @Override
    public int hashCode() {
      return name.hashCode();
    }
  }
}
