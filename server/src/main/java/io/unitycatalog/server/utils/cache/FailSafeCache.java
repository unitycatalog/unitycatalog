package io.unitycatalog.server.utils.cache;

import java.util.Objects;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wraps a cache tier so its failures are never terminal: a get failure becomes a miss and a
 * put/invalidate failure a logged no-op. Catches {@link Exception} (covers checked exceptions a
 * delegate may raise via {@code @SneakyThrows} and, later, remote-tier I/O) and deliberately lets
 * {@link Error} propagate. An {@link InterruptedException} is not treated as a cache failure: the
 * interrupt flag is restored and the interruption is propagated, rather than being masked as a
 * miss/no-op that would let the caller continue with the interrupt flag set.
 */
public class FailSafeCache<K, V> implements Cache<K, V> {
  private static final Logger LOGGER = LoggerFactory.getLogger(FailSafeCache.class);

  private final Cache<K, V> delegate;

  public FailSafeCache(Cache<K, V> delegate) {
    this.delegate = Objects.requireNonNull(delegate, "delegate");
  }

  @Override
  public Optional<V> getIfPresent(K key) {
    try {
      return delegate.getIfPresent(key);
    } catch (Exception e) {
      rethrowIfInterrupted(e);
      LOGGER.warn(
          "Cache get failed on [{}]; treating as miss", delegate.getClass().getSimpleName(), e);
      return Optional.empty();
    }
  }

  @Override
  public void put(K key, V value) {
    try {
      delegate.put(key, value);
    } catch (Exception e) {
      rethrowIfInterrupted(e);
      LOGGER.warn("Cache put failed on [{}]; ignoring", delegate.getClass().getSimpleName(), e);
    }
  }

  @Override
  public void invalidate(K key) {
    try {
      delegate.invalidate(key);
    } catch (Exception e) {
      rethrowIfInterrupted(e);
      LOGGER.warn(
          "Cache invalidate failed on [{}]; ignoring", delegate.getClass().getSimpleName(), e);
    }
  }

  /**
   * An interruption is not a cache failure. Restore the interrupt flag and propagate it so the
   * caller stops, instead of masking it as a miss/no-op and continuing while interrupted.
   */
  private static void rethrowIfInterrupted(Exception e) {
    if (e instanceof InterruptedException) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted during cache operation", e);
    }
  }
}
