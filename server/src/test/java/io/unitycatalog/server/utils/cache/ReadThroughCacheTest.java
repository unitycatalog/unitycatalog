package io.unitycatalog.server.utils.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;

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

  @Test
  void concurrentMissesEachReturnValidValueLockFree() throws Exception {
    MapCache<String, String> store = new MapCache<>();
    ReadThroughCache<String, String> cache = new ReadThroughCache<>(store, (k, v) -> true);

    int threads = 16;
    var pool = java.util.concurrent.Executors.newFixedThreadPool(threads);
    try {
      List<java.util.concurrent.Future<String>> futures = new java.util.ArrayList<>();
      for (int i = 0; i < threads; i++) {
        futures.add(pool.submit(() -> cache.get("k", () -> "v")));
      }
      for (var f : futures) {
        assertEquals("v", f.get()); // every caller gets a valid value; last write wins
      }
    } finally {
      pool.shutdownNow();
    }
  }
}
