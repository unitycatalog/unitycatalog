package io.unitycatalog.server.utils.cache;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import java.time.Clock;
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
    this(maxSize, expiresAtEpochMs, Clock.systemUTC());
  }

  public CaffeineCache(int maxSize, ToLongFunction<V> expiresAtEpochMs, Clock clock) {
    Objects.requireNonNull(expiresAtEpochMs, "expiresAtEpochMs");
    Objects.requireNonNull(clock, "clock");
    // Capture the wall-clock instant at construction. The ticker measures elapsed time since
    // construction, so its magnitude stays small (like the default nanoTime ticker);
    // TimeUnit.toNanos saturates rather than throwing.
    final long baseEpochMilli = clock.instant().toEpochMilli();
    this.delegate =
        Caffeine.newBuilder()
            .maximumSize(maxSize)
            // Drive Caffeine's expiry clock from the injected Clock so that a mocked Clock
            // makes expiry deterministic in tests.
            .ticker(
                () ->
                    TimeUnit.MILLISECONDS.toNanos(clock.instant().toEpochMilli() - baseEpochMilli))
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
                            0L,
                            expiresAtEpochMs.applyAsLong(value) - clock.instant().toEpochMilli());
                    return TimeUnit.MILLISECONDS.toNanos(remainingMs);
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
