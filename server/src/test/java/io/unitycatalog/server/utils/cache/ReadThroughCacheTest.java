package io.unitycatalog.server.utils.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiPredicate;
import org.junit.jupiter.api.Test;

public class ReadThroughCacheTest {

  static final class MapCache<K, V> implements Cache<K, V> {
    final Map<K, V> map = new ConcurrentHashMap<>();

    public Optional<V> getIfPresent(K key) {
      return Optional.ofNullable(map.get(key));
    }

    public void put(K key, V value) {
      map.put(key, value);
    }

    public void invalidate(K key) {
      map.remove(key);
    }
  }

  @Test
  void freshHitSkipsLoader() {
    MapCache<String, String> store = new MapCache<>();
    store.put("k", "cached");
    BiPredicate<String, String> alwaysValid = (k, v) -> true;
    ReadThroughCache<String, String> cache = new ReadThroughCache<>(store, alwaysValid);

    AtomicInteger loads = new AtomicInteger();
    String result =
        cache.get(
            "k",
            () -> {
              loads.incrementAndGet();
              return "loaded";
            });

    assertEquals("cached", result);
    assertEquals(0, loads.get());
  }

  @Test
  void invalidCachedValueTriggersLoadAndOverwrite() {
    MapCache<String, String> store = new MapCache<>();
    store.put("k", "stale");
    BiPredicate<String, String> rejectStale = (k, v) -> !v.equals("stale");
    ReadThroughCache<String, String> cache = new ReadThroughCache<>(store, rejectStale);

    String result = cache.get("k", () -> "fresh");

    assertEquals("fresh", result);
    assertEquals(Optional.of("fresh"), store.getIfPresent("k")); // overwritten
  }

  @Test
  void missLoadsStoresAndReturns() {
    MapCache<String, String> store = new MapCache<>();
    ReadThroughCache<String, String> cache = new ReadThroughCache<>(store, (k, v) -> true);

    String result = cache.get("k", () -> "loaded");

    assertEquals("loaded", result);
    assertEquals(Optional.of("loaded"), store.getIfPresent("k"));
  }

  // --- Loader throws: propagates, store not poisoned ---

  @Test
  void loaderThrowsPropagatesAndStoreNotPoisoned() {
    MapCache<String, String> store = new MapCache<>();
    ReadThroughCache<String, String> cache = new ReadThroughCache<>(store, (k, v) -> true);

    assertThrows(
        RuntimeException.class,
        () ->
            cache.get(
                "k",
                () -> {
                  throw new RuntimeException("backend down");
                }));
    assertTrue(store.getIfPresent("k").isEmpty(), "store must not be poisoned after loader throws");
  }

  // --- Loader returns null: IllegalStateException, nothing cached ---

  @Test
  void loaderReturnsNullThrowsIllegalStateAndNothingCached() {
    MapCache<String, String> store = new MapCache<>();
    ReadThroughCache<String, String> cache = new ReadThroughCache<>(store, (k, v) -> true);

    assertThrows(IllegalStateException.class, () -> cache.get("k", () -> null));
    assertTrue(store.getIfPresent("k").isEmpty(), "store must remain empty after null loader");
  }

  // --- stale value in store + loader throws: exception propagates, stale stays ---

  @Test
  void staleHitThenLoaderThrowsPropagatesAndStoreKeepsStale() {
    MapCache<String, String> store = new MapCache<>();
    store.put("k", "stale");
    ReadThroughCache<String, String> cache =
        new ReadThroughCache<>(store, (key, v) -> !v.equals("stale")); // rejects "stale"
    assertThrows(
        RuntimeException.class,
        () ->
            cache.get(
                "k",
                () -> {
                  throw new RuntimeException("backend down");
                }));
    assertEquals(
        Optional.of("stale"),
        store.getIfPresent("k")); // stale still there; not removed, not overwritten
  }

  // --- Null loader guard ---

  @Test
  void nullLoaderThrows() {
    assertThrows(
        NullPointerException.class,
        () -> new ReadThroughCache<>(new MapCache<>(), (k, v) -> true).get("k", null));
  }

  // --- Null constructor arguments ---

  @Test
  void nullCtorArgsThrow() {
    assertThrows(NullPointerException.class, () -> new ReadThroughCache<>(null, (k, v) -> true));
    assertThrows(NullPointerException.class, () -> new ReadThroughCache<>(new MapCache<>(), null));
  }

  // --- Single-flight: a cold-key stampede loads exactly once ---

  @Test
  void concurrentMissesLoadOnce() throws Exception {
    MapCache<String, String> store = new MapCache<>();
    ReadThroughCache<String, String> cache = new ReadThroughCache<>(store, (k, v) -> true);

    int threads = 16;
    var start = new java.util.concurrent.CountDownLatch(1);
    var loads = new AtomicInteger();
    var pool = java.util.concurrent.Executors.newFixedThreadPool(threads);
    try {
      List<java.util.concurrent.Future<String>> futures = new java.util.ArrayList<>();
      for (int i = 0; i < threads; i++) {
        futures.add(
            pool.submit(
                () -> {
                  start.await(); // burst together so misses overlap on the miss path
                  return cache.get(
                      "k",
                      () -> {
                        loads.incrementAndGet();
                        sleepQuietly(50); // hold the stripe so the others queue behind it
                        return "v";
                      });
                }));
      }
      start.countDown();
      for (var f : futures) {
        assertEquals("v", f.get()); // every caller gets the value
      }
      assertEquals(1, loads.get(), "single-flight must load a cold key exactly once");
    } finally {
      pool.shutdownNow();
    }
  }

  private static void sleepQuietly(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
