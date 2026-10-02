package io.unitycatalog.server.utils.cache;

import java.util.Optional;

/**
 * A single cache tier: a keyed store with no loading logic. {@code getIfPresent} is read-only with
 * respect to slower tiers (a composite may promote upward, never downward). Implementations decide
 * their own storage and serialization; the generic core knows nothing credential- or
 * library-specific.
 *
 * @param <K> key type; must have value-based {@code equals}/{@code hashCode}
 * @param <V> value type
 */
public interface Cache<K, V> {
  Optional<V> getIfPresent(K key);

  void put(K key, V value);

  void invalidate(K key);
}
