package io.unitycatalog.server.utils.cache;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.ToLongFunction;

/**
 * In-memory L1 tier backed by Caffeine. Stores live key/value references (no serialization).
 * Per-entry expiry is derived from the value's own absolute expiry timestamp via {@code
 * expiresAtEpochMs}; a {@code maximumSize} bounds memory. Eviction/expiry here is advisory storage
 * hygiene — the authoritative freshness decision is made by the caller's validator on read.
 */
public class CaffeineCache<K, V> implements Cache<K, V> {
  // Fully qualified to avoid clashing with this package's Cache interface.
  private final com.github.benmanes.caffeine.cache.Cache<K, V> delegate;

  public CaffeineCache(int maxSize, ToLongFunction<V> expiresAtEpochMs) {
    Objects.requireNonNull(expiresAtEpochMs, "expiresAtEpochMs");
    this.delegate =
        Caffeine.newBuilder()
            .maximumSize(maxSize)
            .expireAfter(
                new Expiry<K, V>() {
                  @Override
                  public long expireAfterCreate(K key, V value, long currentTime) {
                    return remainingNanos(value);
                  }

                  @Override
                  public long expireAfterUpdate(
                      K key, V value, long currentTime, long currentDuration) {
                    return remainingNanos(value);
                  }

                  @Override
                  public long expireAfterRead(
                      K key, V value, long currentTime, long currentDuration) {
                    return currentDuration; // reading does not extend life
                  }

                  private long remainingNanos(V value) {
                    long remainingMs =
                        Math.max(
                            0, expiresAtEpochMs.applyAsLong(value) - System.currentTimeMillis());
                    // Cap before the ms->ns conversion. TimeUnit.toNanos already SATURATES to
                    // Long.MAX_VALUE on overflow (it does not wrap), so a far-future/static expiry
                    // yields an effectively-never-expire entry; clamping here makes that guarantee
                    // explicit rather than relying on that subtlety.
                    long safeMs = Math.min(remainingMs, Long.MAX_VALUE / 1_000_000L);
                    return TimeUnit.MILLISECONDS.toNanos(safeMs);
                  }
                })
            .build();
  }

  @Override
  public Optional<V> getIfPresent(K key) {
    return Optional.ofNullable(delegate.getIfPresent(key));
  }

  @Override
  public void put(K key, V value) {
    delegate.put(key, value);
  }

  @Override
  public void invalidate(K key) {
    delegate.invalidate(key);
  }

  /** Forces pending maintenance (eviction). Test-visible; deterministic size assertions need it. */
  void cleanUp() {
    delegate.cleanUp();
  }
}
