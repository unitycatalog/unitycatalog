package io.unitycatalog.server.utils.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
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

  // --- expireAfterUpdate: re-putting a key resets the TTL from the new value's expiry ---

  @Test
  void expireAfterUpdateOnRePut() {
    // Values are epoch-ms; extractor reads the value directly.
    CaffeineCache<String, Long> cache = new CaffeineCache<>(10, v -> v);
    long farFutureMs = System.currentTimeMillis() + 3_600_000L;
    cache.put("k", farFutureMs); // far-future expiry → should be present
    assertTrue(cache.getIfPresent("k").isPresent());

    long alreadyExpiredMs = System.currentTimeMillis() - 1L; // past expiry
    cache.put("k", alreadyExpiredMs); // re-put with an expired value
    cache.cleanUp();
    assertTrue(cache.getIfPresent("k").isEmpty());
  }

  // --- Per-key expiry independence ---

  @Test
  void perKeyExpiryIsIndependent() {
    CaffeineCache<String, Long> cache = new CaffeineCache<>(10, v -> v);
    long expired = System.currentTimeMillis() - 1L;
    long fresh = System.currentTimeMillis() + 3_600_000L;
    cache.put("a", expired);
    cache.put("b", fresh);
    cache.cleanUp();
    assertTrue(cache.getIfPresent("a").isEmpty(), "expired key must be absent");
    assertTrue(cache.getIfPresent("b").isPresent(), "fresh key must still be present");
  }

  // --- expireAfterRead does not extend the entry's life ---

  @Test
  void expireAfterReadDoesNotExtendLife() throws InterruptedException {
    CaffeineCache<String, Long> cache = new CaffeineCache<>(10, v -> v);
    // 300 ms TTL from now
    long expiry = System.currentTimeMillis() + 300L;
    cache.put("k", expiry);

    // Read several times within the first ~150 ms; entry should still be live
    long readDeadline = System.currentTimeMillis() + 150L;
    while (System.currentTimeMillis() < readDeadline) {
      cache.getIfPresent("k");
      Thread.sleep(20);
    }

    // Sleep past the original 300 ms TTL (total elapsed > 350 ms from put)
    Thread.sleep(200);
    cache.cleanUp();
    assertTrue(cache.getIfPresent("k").isEmpty(), "entry must have expired despite reads");
  }

  // --- Null expiry function guard ---

  @Test
  void nullExpiryFunctionThrows() {
    assertThrows(NullPointerException.class, () -> new CaffeineCache<>(10, null));
  }
}
