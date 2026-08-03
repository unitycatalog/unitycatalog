package io.unitycatalog.hadoop.internal.util;

import io.unitycatalog.client.internal.Preconditions;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Predicate;

public final class BoundedKeyedCache<K, V> {
  private final int maxSize;
  private final Consumer<V> evictionListener;
  private final Object cacheLock = new Object();
  private final LinkedHashMap<K, V> cache = new LinkedHashMap<>(16, 0.75f, true);
  private final IdLockMap<K> keyLocks = new IdLockMap<>();

  public BoundedKeyedCache(int maxSize) {
    this(maxSize, null);
  }

  public BoundedKeyedCache(int maxSize, Consumer<V> evictionListener) {
    Preconditions.checkArgument(maxSize > 0, "maxSize must be positive, got %s", maxSize);
    this.maxSize = maxSize;
    this.evictionListener = evictionListener == null ? value -> {} : evictionListener;
  }

  public V getIfPresent(K key) {
    synchronized (cacheLock) {
      return cache.get(key);
    }
  }

  public <E extends Exception> V getOrLoad(K key, CheckedSupplier<V, E> loader) throws E {
    return getOrLoad(key, value -> true, loader);
  }

  /**
   * Returns the cached value for {@code key} when present and accepted by {@code isValid},
   * otherwise loads a fresh value and caches it. A cached-but-rejected value (e.g. an expiring
   * credential) is treated as a miss. Loads are single-flight per key: the thread holding the key
   * lock loads while same-key waiters block for the duration of the load and reuse its result;
   * threads on different keys never block each other. A freshly loaded value is cached and returned
   * even if {@code isValid} would reject it -- the loader's output is trusted. Loaders must not
   * call back into this cache: a loader that loads other keys can form a lock cycle and deadlock.
   */
  public <E extends Exception> V getOrLoad(
      K key, Predicate<V> isValid, CheckedSupplier<V, E> loader) throws E {
    V cached = getIfPresent(key);
    if (cached != null && isValid.test(cached)) {
      return cached;
    }
    try (IdLockMap<K>.IdLock ignored = keyLocks.acquire(key)) {
      V lockedCached = getIfPresent(key);
      if (lockedCached != null && isValid.test(lockedCached)) {
        return lockedCached;
      }
      V loaded = Objects.requireNonNull(loader.get(), "loader returned null");
      put(key, loaded);
      return loaded;
    }
  }

  public void put(K key, V value) {
    Objects.requireNonNull(key, "key cannot be null");
    Objects.requireNonNull(value, "value cannot be null");
    List<V> evicted = new ArrayList<>();
    synchronized (cacheLock) {
      V previous = cache.put(key, value);
      if (previous != null && previous != value) {
        evicted.add(previous);
      }
      while (cache.size() > maxSize) {
        Map.Entry<K, V> eldest = cache.entrySet().iterator().next();
        cache.remove(eldest.getKey());
        evicted.add(eldest.getValue());
      }
    }
    evicted.forEach(evictionListener);
  }

  public void clear() {
    List<V> evicted;
    synchronized (cacheLock) {
      evicted = new ArrayList<>(cache.values());
      cache.clear();
    }
    evicted.forEach(evictionListener);
  }

  public int size() {
    synchronized (cacheLock) {
      return cache.size();
    }
  }

  public List<V> values() {
    synchronized (cacheLock) {
      return new ArrayList<>(cache.values());
    }
  }

  @FunctionalInterface
  public interface CheckedSupplier<T, E extends Exception> {
    T get() throws E;
  }
}
