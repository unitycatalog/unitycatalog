package io.unitycatalog.server.utils.cache;

import java.util.List;
import java.util.Optional;

/**
 * Composes cache tiers ordered fastest → slowest. A read walks the tiers and, on a hit, promotes
 * the value upward to every faster tier (a slow-tier hit warms the fast tier); it never writes
 * downward. Writes and invalidations fan out to all tiers. Works with one tier (L1 only) or many.
 */
public class LayeredCache<K, V> implements Cache<K, V> {
  private final List<Cache<K, V>> layers;

  public LayeredCache(List<Cache<K, V>> layers) {
    this.layers = List.copyOf(layers);
  }

  @Override
  public Optional<V> getIfPresent(K key) {
    for (int i = 0; i < layers.size(); i++) {
      Optional<V> hit = layers.get(i).getIfPresent(key);
      if (hit.isPresent()) {
        for (int j = 0; j < i; j++) {
          layers.get(j).put(key, hit.get());
        }
        return hit;
      }
    }
    return Optional.empty();
  }

  @Override
  public void put(K key, V value) {
    for (Cache<K, V> layer : layers) {
      layer.put(key, value);
    }
  }

  @Override
  public void invalidate(K key) {
    for (Cache<K, V> layer : layers) {
      layer.invalidate(key);
    }
  }
}
