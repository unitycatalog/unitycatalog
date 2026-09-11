package io.unitycatalog.hadoop.internal.auth;

import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.hadoop.internal.auth.CredentialCache.RenewableCredential;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class CredentialCacheTest {

  @Test
  void accessReusesValidCachedCredential() throws Exception {
    CredentialCache<String, String> cache = new CredentialCache<>(2);
    AtomicInteger fetches = new AtomicInteger();

    String first = cache.access("k", () -> newCredential("v1", fetches, false));
    String second = cache.access("k", () -> newCredential("v2", fetches, false));

    assertThat(first).isEqualTo("v1");
    assertThat(second).isEqualTo("v1");
    assertThat(fetches).hasValue(1);
  }

  @Test
  void accessRefetchesWhenReadyToRenew() throws Exception {
    CredentialCache<String, String> cache = new CredentialCache<>(2);
    AtomicInteger fetches = new AtomicInteger();

    assertThat(cache.access("k", () -> newCredential("v1", fetches, true))).isEqualTo("v1");
    assertThat(cache.access("k", () -> newCredential("v2", fetches, false))).isEqualTo("v2");
    assertThat(fetches).hasValue(2);
  }

  @Test
  void accessCoalescesConcurrentFetchesForTheSameKey() throws Exception {
    CredentialCache<String, String> cache = new CredentialCache<>(2);
    CountDownLatch firstFetchStarted = new CountDownLatch(1);
    CountDownLatch releaseFirstFetch = new CountDownLatch(1);
    AtomicInteger fetches = new AtomicInteger();

    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      Future<String> first =
          executor.submit(
              () ->
                  cache.access(
                      "k",
                      () -> {
                        fetches.incrementAndGet();
                        firstFetchStarted.countDown();
                        await(releaseFirstFetch);
                        return new FixedCredential<>("v1", false);
                      }));
      assertThat(firstFetchStarted.await(5, TimeUnit.SECONDS)).isTrue();

      Future<String> second =
          executor.submit(() -> cache.access("k", () -> newCredential("v2", fetches, false)));

      releaseFirstFetch.countDown();
      assertThat(first.get(5, TimeUnit.SECONDS)).isEqualTo("v1");
      assertThat(second.get(5, TimeUnit.SECONDS)).isEqualTo("v1");
      assertThat(fetches).hasValue(1);
    } finally {
      releaseFirstFetch.countDown();
      executor.shutdownNow();
    }
  }

  @Test
  void accessFetchesDistinctKeysInParallel() throws Exception {
    CredentialCache<String, String> cache = new CredentialCache<>(4);
    CountDownLatch slowFetchStarted = new CountDownLatch(1);
    CountDownLatch releaseSlowFetch = new CountDownLatch(1);
    AtomicBoolean fastCompletedWhileSlowHeld = new AtomicBoolean();

    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      Future<String> slow =
          executor.submit(
              () ->
                  cache.access(
                      "slow",
                      () -> {
                        slowFetchStarted.countDown();
                        await(releaseSlowFetch);
                        return new FixedCredential<>("slow-value", false);
                      }));
      assertThat(slowFetchStarted.await(5, TimeUnit.SECONDS)).isTrue();

      Future<String> fast =
          executor.submit(
              () -> {
                String value =
                    cache.access("fast", () -> new FixedCredential<>("fast-value", false));
                fastCompletedWhileSlowHeld.set(releaseSlowFetch.getCount() > 0);
                return value;
              });
      assertThat(fast.get(5, TimeUnit.SECONDS)).isEqualTo("fast-value");
      assertThat(fastCompletedWhileSlowHeld).isTrue();

      releaseSlowFetch.countDown();
      assertThat(slow.get(5, TimeUnit.SECONDS)).isEqualTo("slow-value");
    } finally {
      releaseSlowFetch.countDown();
      executor.shutdownNow();
    }
  }

  private static void await(CountDownLatch latch) {
    try {
      assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    }
  }

  private static RenewableCredential<String> newCredential(
      String value, AtomicInteger fetches, boolean readyToRenew) {
    fetches.incrementAndGet();
    return new FixedCredential<>(value, readyToRenew);
  }

  private static final class FixedCredential<T> extends RenewableCredential<T> {
    private final boolean readyToRenew;

    private FixedCredential(T value, boolean readyToRenew) {
      super(value);
      this.readyToRenew = readyToRenew;
    }

    @Override
    public boolean readyToRenew() {
      return readyToRenew;
    }
  }
}
