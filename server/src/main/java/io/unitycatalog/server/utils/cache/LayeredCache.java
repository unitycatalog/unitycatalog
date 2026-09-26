package io.unitycatalog.server.utils.cache;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Composes cache tiers ordered fastest → slowest. A read walks the tiers and, on a hit, promotes
 * the value upward to every faster tier (a slow-tier hit warms the fast tier); it never writes
 * downward. Writes and invalidations fan out to all tiers. Works with one tier (L1 only) or many.
 */
public class LayeredCache<K, V> implements Cache<K, V> {
  private final List<Cache<K, V>> layers;

  public LayeredCache(List<Cache<K, V>> layers) {
    Objects.requireNonNull(layers, "layers");
    this.layers = List.copyOf(layers);
    if (this.layers.isEmpty()) {
      throw new IllegalArgumentException("LayeredCache requires at least one layer");
    }
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

  /**
   * Fans out to all tiers. If a tier's invalidate fails (for example, because it is wrapped in a
   * {@link FailSafeCache} that swallows the failure), a stale entry can survive in that tier and be
   * promoted back up on the next read. Correctness in that case relies on a {@link
   * ReadThroughCache} validator rejecting the stale value on read. This scenario is L2-only and
   * cannot occur with the L1-only stack.
   */
  @Override
  public void invalidate(K key) {
    for (Cache<K, V> layer : layers) {
      layer.invalidate(key);
    }
  }
}
