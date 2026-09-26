package io.unitycatalog.server.utils.cache;

import java.util.Objects;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wraps a cache tier so its failures are never terminal: a get failure becomes a miss and a
 * put/invalidate failure a logged no-op. Catches {@link Exception} (covers checked exceptions a
 * delegate may raise via {@code @SneakyThrows} and, later, remote-tier I/O), restores the interrupt
 * flag on {@link InterruptedException}, and deliberately lets {@link Error} propagate.
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
      restoreInterruptIfNeeded(e);
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
      restoreInterruptIfNeeded(e);
      LOGGER.warn("Cache put failed on [{}]; ignoring", delegate.getClass().getSimpleName(), e);
    }
  }

  @Override
  public void invalidate(K key) {
    try {
      delegate.invalidate(key);
    } catch (Exception e) {
      restoreInterruptIfNeeded(e);
      LOGGER.warn(
          "Cache invalidate failed on [{}]; ignoring", delegate.getClass().getSimpleName(), e);
    }
  }

  private static void restoreInterruptIfNeeded(Exception e) {
    if (e instanceof InterruptedException) {
      Thread.currentThread().interrupt();
    }
  }
}
