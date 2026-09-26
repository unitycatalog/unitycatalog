package io.unitycatalog.server.utils.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Optional;
import org.junit.jupiter.api.Test;

public class CaffeineCacheTest {

  // Never-expire clock: value's expiry is far in the future.
  private static long farFuture(String v) {
    return System.currentTimeMillis() + 3_600_000L;
  }

  @Test
  void putThenGetReturnsSameReference() {
    CaffeineCache<String, String> cache = new CaffeineCache<>(10, CaffeineCacheTest::farFuture);
    String value = "v";
    cache.put("k", value);
    Optional<String> got = cache.getIfPresent("k");
    assertTrue(got.isPresent());
    assertSame(value, got.get()); // identity storage, no copy
  }

  @Test
  void missReturnsEmpty() {
    CaffeineCache<String, String> cache = new CaffeineCache<>(10, CaffeineCacheTest::farFuture);
    assertTrue(cache.getIfPresent("absent").isEmpty());
  }

  @Test
  void invalidateRemoves() {
    CaffeineCache<String, String> cache = new CaffeineCache<>(10, CaffeineCacheTest::farFuture);
    cache.put("k", "v");
    cache.invalidate("k");
    assertTrue(cache.getIfPresent("k").isEmpty());
  }

  @Test
  void alreadyExpiredEntryIsNotReturned() {
    // expiry in the past → Caffeine drops it immediately.
    CaffeineCache<String, String> cache =
        new CaffeineCache<>(10, v -> System.currentTimeMillis() - 1);
    cache.put("k", "v");
    assertTrue(cache.getIfPresent("k").isEmpty());
  }

  @Test
  void maximumSizeIsEnforced() {
    CaffeineCache<Integer, Integer> cache = new CaffeineCache<>(2, v -> Long.MAX_VALUE);
    cache.put(1, 1);
    cache.put(2, 2);
    cache.put(3, 3); // exceeds max size
    cache.cleanUp(); // force pending eviction (deterministic for the test)
    long present = 0;
    for (int i = 1; i <= 3; i++) {
      if (cache.getIfPresent(i).isPresent()) present++;
    }
    assertEquals(2, present);
  }
}
