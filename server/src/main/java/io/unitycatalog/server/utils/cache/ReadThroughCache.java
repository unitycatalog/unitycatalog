package io.unitycatalog.server.utils.cache;

import java.util.Objects;
import java.util.Optional;
import java.util.function.BiPredicate;
import java.util.function.Supplier;

/**
 * Read-through loading over a {@link Cache}. Returns a cached value only when it is present and the
 * validator accepts it; otherwise it loads synchronously on the caller's thread, stores, and
 * returns the loaded value (no re-read, so a freshly loaded value near its expiry is still returned
 * rather than triggering a reload loop). The loader is supplied per call because a credential vend
 * needs its full context, not just the key.
 *
 * <p>A validated hit is served without locking. On a miss, loads are single-flighted per key: only
 * one thread loads a given key while the others wait and then re-check, so a cold-key stampede
 * triggers a single load instead of one load per caller. Locking uses a fixed set of stripe locks
 * (not a per-key lock map), so it never leaks locks or races on lock cleanup; distinct keys that
 * hash to the same stripe serialize harmlessly.
 */
public class ReadThroughCache<K, V> {
  private static final int STRIPE_COUNT = 64;

  private final Cache<K, V> cache;
  private final BiPredicate<K, V> valid;
  private final Object[] stripes;

  public ReadThroughCache(Cache<K, V> cache, BiPredicate<K, V> valid) {
    this.cache = Objects.requireNonNull(cache, "cache");
    this.valid = Objects.requireNonNull(valid, "valid");
    this.stripes = new Object[STRIPE_COUNT];
    for (int i = 0; i < STRIPE_COUNT; i++) {
      this.stripes[i] = new Object();
    }
  }

  public V get(K key, Supplier<V> loader) {
    Objects.requireNonNull(loader, "loader");
    Optional<V> hit = cache.getIfPresent(key);
    if (hit.isPresent() && valid.test(key, hit.get())) {
      return hit.get();
    }
    // Serialize the miss path per stripe so a cold-key stampede loads once. Re-check under the lock
    // — another thread may have loaded a valid value while we waited.
    synchronized (stripes[Math.floorMod(Objects.hashCode(key), STRIPE_COUNT)]) {
      Optional<V> rehit = cache.getIfPresent(key);
      if (rehit.isPresent() && valid.test(key, rehit.get())) {
        return rehit.get();
      }
      return load(key, loader);
    }
  }

  private V load(K key, Supplier<V> loader) {
    V loaded = loader.get();
    if (loaded == null) {
      throw new IllegalStateException("loader must return a non-null value");
    }
    cache.put(key, loaded);
    return loaded;
  }
}
