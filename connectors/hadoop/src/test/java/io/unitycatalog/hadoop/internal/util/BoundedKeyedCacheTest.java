package io.unitycatalog.hadoop.internal.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class BoundedKeyedCacheTest {

  @Test
  void evictsLeastRecentlyUsedEntry() {
    List<String> evicted = new ArrayList<>();
    BoundedKeyedCache<String, String> cache = new BoundedKeyedCache<>(2, evicted::add);

    cache.put("a", "value-a");
    cache.put("b", "value-b");
    assertThat(cache.getIfPresent("a")).isEqualTo("value-a");

    cache.put("c", "value-c");

    assertThat(cache.getIfPresent("a")).isEqualTo("value-a");
    assertThat(cache.getIfPresent("b")).isNull();
    assertThat(cache.getIfPresent("c")).isEqualTo("value-c");
    assertThat(evicted).containsExactly("value-b");
  }

  @Test
  void getIfPresentUpdatesLruRecency() {
    List<String> evicted = new ArrayList<>();
    BoundedKeyedCache<String, String> cache = new BoundedKeyedCache<>(2, evicted::add);

    cache.put("a", "value-a");
    cache.put("b", "value-b");
    // Reading "a" should make "a" the most-recently-used, so the next put evicts "b" not "a".
    assertThat(cache.getIfPresent("a")).isEqualTo("value-a");
    cache.put("c", "value-c");

    assertThat(cache.getIfPresent("b")).isNull();
    assertThat(cache.getIfPresent("a")).isEqualTo("value-a");
    assertThat(cache.getIfPresent("c")).isEqualTo("value-c");
    assertThat(evicted).containsExactly("value-b");
  }

  @Test
  void putWithDifferentValueFiresEvictionForPrevious() {
    List<String> evicted = new ArrayList<>();
    BoundedKeyedCache<String, String> cache = new BoundedKeyedCache<>(2, evicted::add);

    cache.put("a", "v1");
    cache.put("a", "v2");

    assertThat(cache.getIfPresent("a")).isEqualTo("v2");
    assertThat(cache.size()).isEqualTo(1);
    assertThat(evicted).containsExactly("v1");
  }

  @Test
  void clearFiresListenerForAllEntries() {
    List<String> evicted = new ArrayList<>();
    BoundedKeyedCache<String, String> cache = new BoundedKeyedCache<>(4, evicted::add);

    cache.put("a", "value-a");
    cache.put("b", "value-b");
    cache.clear();

    assertThat(cache.size()).isZero();
    assertThat(evicted).containsExactlyInAnyOrder("value-a", "value-b");
  }

  @Test
  void loaderReturningNullThrows() {
    BoundedKeyedCache<String, String> cache = new BoundedKeyedCache<>(2);

    assertThatThrownBy(() -> cache.getOrLoad("k", () -> null))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("loader returned null");
    assertThat(cache.getIfPresent("k")).isNull();
  }

  @Test
  void constructorWithoutEvictionListenerUsesNoOpListener() {
    BoundedKeyedCache<String, String> cache = new BoundedKeyedCache<>(1);

    cache.put("a", "value-a");
    cache.put("b", "value-b");

    assertThat(cache.getIfPresent("a")).isNull();
    assertThat(cache.getIfPresent("b")).isEqualTo("value-b");
  }

  @Test
  void getOrLoadPropagatesCheckedException() {
    BoundedKeyedCache<String, String> cache = new BoundedKeyedCache<>(2);
    IOException boom = new IOException("loader boom");

    assertThatThrownBy(
            () ->
                cache.getOrLoad(
                    "k",
                    () -> {
                      throw boom;
                    }))
        .isSameAs(boom);
  }

  @Test
  void keyLockReleasedAfterLoaderThrows() throws Exception {
    BoundedKeyedCache<String, String> cache = new BoundedKeyedCache<>(2);

    assertThatThrownBy(
            () ->
                cache.getOrLoad(
                    "k",
                    () -> {
                      throw new RuntimeException("loader boom");
                    }))
        .hasMessageContaining("loader boom");

    // If the per-key lock leaked, this second call would deadlock or block. We use a timeout so
    // the test fails fast rather than hanging if the lock was not released.
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      Future<String> retry = executor.submit(() -> cache.getOrLoad("k", () -> "loaded"));
      assertThat(retry.get(2, TimeUnit.SECONDS)).isEqualTo("loaded");
    } finally {
      executor.shutdownNow();
    }
    assertThat(cache.getIfPresent("k")).isEqualTo("loaded");
  }

  @Test
  void getOrLoadLoadsSameKeyOnlyOnce() throws Exception {
    BoundedKeyedCache<String, String> cache = new BoundedKeyedCache<>(2);
    CountDownLatch firstLoadStarted = new CountDownLatch(1);
    CountDownLatch releaseFirstLoad = new CountDownLatch(1);
    AtomicInteger loadCount = new AtomicInteger();

    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      Future<String> first =
          executor.submit(
              () ->
                  cache.getOrLoad(
                      "key",
                      () -> {
                        loadCount.incrementAndGet();
                        firstLoadStarted.countDown();
                        assertThat(releaseFirstLoad.await(5, TimeUnit.SECONDS)).isTrue();
                        return "value";
                      }));
      assertThat(firstLoadStarted.await(5, TimeUnit.SECONDS)).isTrue();

      Future<String> second = executor.submit(() -> cache.getOrLoad("key", () -> "other"));

      releaseFirstLoad.countDown();
      assertThat(first.get(5, TimeUnit.SECONDS)).isEqualTo("value");
      assertThat(second.get(5, TimeUnit.SECONDS)).isEqualTo("value");
      assertThat(loadCount).hasValue(1);
    } finally {
      releaseFirstLoad.countDown();
      executor.shutdownNow();
    }
  }

  @Test
  void manyThreadsOnSameKeyInvokeLoaderExactlyOnce() throws Exception {
    int threads = 64;
    BoundedKeyedCache<String, String> cache = new BoundedKeyedCache<>(2);
    AtomicInteger loadCount = new AtomicInteger();
    CyclicBarrier startBarrier = new CyclicBarrier(threads);
    String singleton = "value";

    ExecutorService executor = Executors.newFixedThreadPool(threads);
    try {
      List<Future<String>> futures = new ArrayList<>();
      for (int i = 0; i < threads; i++) {
        futures.add(
            executor.submit(
                () -> {
                  startBarrier.await(5, TimeUnit.SECONDS);
                  return cache.getOrLoad(
                      "k",
                      () -> {
                        loadCount.incrementAndGet();
                        return singleton;
                      });
                }));
      }
      for (Future<String> f : futures) {
        assertThat(f.get(10, TimeUnit.SECONDS)).isSameAs(singleton);
      }
      assertThat(loadCount).hasValue(1);
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  void getOrLoadDifferentKeysProgressIndependently() throws Exception {
    BoundedKeyedCache<String, String> cache = new BoundedKeyedCache<>(4);
    CountDownLatch slowLoaderStarted = new CountDownLatch(1);
    CountDownLatch releaseSlowLoader = new CountDownLatch(1);

    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      Future<String> slowKey =
          executor.submit(
              () ->
                  cache.getOrLoad(
                      "slow",
                      () -> {
                        slowLoaderStarted.countDown();
                        assertThat(releaseSlowLoader.await(5, TimeUnit.SECONDS)).isTrue();
                        return "slow-value";
                      }));
      assertThat(slowLoaderStarted.await(5, TimeUnit.SECONDS)).isTrue();

      // While the slow loader on key "slow" is blocked, a load on a different key must complete.
      Future<String> fastKey = executor.submit(() -> cache.getOrLoad("fast", () -> "fast-value"));
      assertThat(fastKey.get(5, TimeUnit.SECONDS)).isEqualTo("fast-value");

      releaseSlowLoader.countDown();
      assertThat(slowKey.get(5, TimeUnit.SECONDS)).isEqualTo("slow-value");
    } finally {
      releaseSlowLoader.countDown();
      executor.shutdownNow();
    }
  }
}
