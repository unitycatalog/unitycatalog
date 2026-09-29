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
 * <p>Concurrent misses load independently. Each caller receives its own loaded value, and the last
 * write determines the stored value. Every subsequent hit still goes through validation.
 */
public class ReadThroughCache<K, V> {
  private final Cache<K, V> cache;
  private final BiPredicate<K, V> valid;

  public ReadThroughCache(Cache<K, V> cache, BiPredicate<K, V> valid) {
    this.cache = Objects.requireNonNull(cache, "cache");
    this.valid = Objects.requireNonNull(valid, "valid");
  }

  public V get(K key, Supplier<V> loader) {
    Objects.requireNonNull(loader, "loader");
    Optional<V> hit = cache.getIfPresent(key);
    if (hit.isPresent() && valid.test(key, hit.get())) {
      return hit.get();
    }
    return load(key, loader);
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
