package io.unitycatalog.server.utils.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.Test;

public class LayeredCacheTest {

  /** Minimal map-backed tier for composition tests. */
  static final class MapCache<K, V> implements Cache<K, V> {
    final Map<K, V> map = new ConcurrentHashMap<>();

    public Optional<V> getIfPresent(K key) {
      return Optional.ofNullable(map.get(key));
    }

    public void put(K key, V value) {
      map.put(key, value);
    }

    public void invalidate(K key) {
      map.remove(key);
    }
  }

  @Test
  void hitInLowerLayerPromotesToFasterLayers() {
    MapCache<String, String> l1 = new MapCache<>();
    MapCache<String, String> l2 = new MapCache<>();
    l2.put("k", "v"); // only slow tier has it
    LayeredCache<String, String> cache = new LayeredCache<>(List.of(l1, l2));

    assertEquals(Optional.of("v"), cache.getIfPresent("k"));
    assertEquals(Optional.of("v"), l1.getIfPresent("k")); // promoted upward
  }

  @Test
  void hitInFastLayerDoesNotWriteDownward() {
    MapCache<String, String> l1 = new MapCache<>();
    MapCache<String, String> l2 = new MapCache<>();
    l1.put("k", "v");
    LayeredCache<String, String> cache = new LayeredCache<>(List.of(l1, l2));

    assertEquals(Optional.of("v"), cache.getIfPresent("k"));
    assertTrue(l2.getIfPresent("k").isEmpty()); // never written downward on a read
  }

  @Test
  void putWritesThroughAllLayersAndInvalidateClearsAll() {
    MapCache<String, String> l1 = new MapCache<>();
    MapCache<String, String> l2 = new MapCache<>();
    LayeredCache<String, String> cache = new LayeredCache<>(List.of(l1, l2));

    cache.put("k", "v");
    assertEquals(Optional.of("v"), l1.getIfPresent("k"));
    assertEquals(Optional.of("v"), l2.getIfPresent("k"));

    cache.invalidate("k");
    assertTrue(l1.getIfPresent("k").isEmpty());
    assertTrue(l2.getIfPresent("k").isEmpty());
  }

  @Test
  void missEverywhereReturnsEmpty() {
    LayeredCache<String, String> cache =
        new LayeredCache<>(List.of(new MapCache<>(), new MapCache<>()));
    assertTrue(cache.getIfPresent("absent").isEmpty());
  }

  // --- 3-tier upward promotion ---

  @Test
  void threeTierUpwardPromotion() {
    MapCache<String, String> l1 = new MapCache<>();
    MapCache<String, String> l2 = new MapCache<>();
    MapCache<String, String> l3 = new MapCache<>();
    l3.put("k", "v"); // only slowest tier has the value
    LayeredCache<String, String> cache = new LayeredCache<>(List.of(l1, l2, l3));

    assertEquals(Optional.of("v"), cache.getIfPresent("k"));
    assertEquals(Optional.of("v"), l1.getIfPresent("k"), "value must be promoted to L1");
    assertEquals(Optional.of("v"), l2.getIfPresent("k"), "value must be promoted to L2");
    assertEquals(Optional.of("v"), l3.getIfPresent("k"), "value must remain in L3");
  }

  // --- Empty layer list rejected ---

  @Test
  void emptyLayersRejected() {
    assertThrows(IllegalArgumentException.class, () -> new LayeredCache<>(List.of()));
  }

  // --- Null layer list rejected ---

  @Test
  void nullLayersRejected() {
    assertThrows(NullPointerException.class, () -> new LayeredCache<String, String>(null));
  }
}
