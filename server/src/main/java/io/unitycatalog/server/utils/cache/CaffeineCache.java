package io.unitycatalog.server.utils.cache;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
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

  // Cap TTL and ticker well below Long.MAX_VALUE so Caffeine's internal `now + duration` can never
  // overflow — we do NOT rely on Caffeine's own duration clamp. Long.MAX_VALUE/2 nanos ≈ 146 years,
  // which is "effectively never expires" for any real credential (bounded far tighter by
  // maxAge/T2).
  private static final long MAX_NANOS = Long.MAX_VALUE / 2L;
  private static final Duration MAX_DURATION = Duration.ofNanos(MAX_NANOS);

  public CaffeineCache(int maxSize, ToLongFunction<V> expiresAtEpochMs) {
    this(maxSize, expiresAtEpochMs, Clock.systemUTC());
  }

  public CaffeineCache(int maxSize, ToLongFunction<V> expiresAtEpochMs, Clock clock) {
    Objects.requireNonNull(expiresAtEpochMs, "expiresAtEpochMs");
    Objects.requireNonNull(clock, "clock");
    // Capture the wall-clock instant at construction. The ticker only needs to measure ELAPSED
    // time (like the default nanoTime ticker), so we subtract this base to keep its magnitude
    // small and avoid relying on any Caffeine-internal overflow clamping for epoch-magnitude
    // values. remainingNanos uses clock.instant() with the absolute expiry epoch.
    final Instant baseInstant = clock.instant();
    this.delegate =
        Caffeine.newBuilder()
            .maximumSize(maxSize)
            // Drive Caffeine's expiry clock from the injected Clock so that a mocked Clock
            // makes expiry deterministic in tests. The ticker returns elapsed time (capped at
            // MAX_NANOS) from the base captured above; Caffeine's internal `now + duration`
            // is then at most MAX_NANOS + MAX_NANOS = Long.MAX_VALUE — provably safe.
            .ticker(
                () -> {
                  Duration elapsed = Duration.between(baseInstant, clock.instant());
                  if (elapsed.isNegative()) {
                    return 0L; // clock stepped backward (e.g. NTP) — floor at 0, never negative
                  }
                  return elapsed.compareTo(MAX_DURATION) >= 0 ? MAX_NANOS : elapsed.toNanos();
                })
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
                    Instant expiry = Instant.ofEpochMilli(expiresAtEpochMs.applyAsLong(value));
                    Duration remaining = Duration.between(clock.instant(), expiry);
                    if (remaining.isNegative()) {
                      return 0L; // already expired
                    }
                    // Guard BEFORE toNanos: only call toNanos on a value known < MAX_DURATION,
                    // so it cannot overflow — no reliance on Caffeine's own duration clamp.
                    return remaining.compareTo(MAX_DURATION) >= 0 ? MAX_NANOS : remaining.toNanos();
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
