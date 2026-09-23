package io.unitycatalog.hadoop.internal.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class MapIdGeneratorTest {

  @Test
  void generateIdIsStable64CharLowercaseHex() {
    Map<String, String> first = new LinkedHashMap<>();
    first.put("b", "2");
    first.put("a", "1");

    Map<String, String> second = Map.of("a", "1", "b", "2");

    String id = MapIdGenerator.generateId(first);
    assertThat(id).isEqualTo(MapIdGenerator.generateId(second));
    assertThat(id).hasSize(64).matches("[0-9a-f]{64}");
    assertThat(MapIdGenerator.generateId(Map.of()))
        .isEqualTo("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
  }

  @Test
  void generateIdDiffersAcrossDistinctMaps() {
    Map<String, String> base = Map.of("a", "1", "b", "2");

    assertThat(MapIdGenerator.generateId(Map.of("a", "1", "b", "3")))
        .isNotEqualTo(MapIdGenerator.generateId(base));
    assertThat(MapIdGenerator.generateId(Map.of("a", "1", "c", "2")))
        .isNotEqualTo(MapIdGenerator.generateId(base));
    assertThat(MapIdGenerator.generateId(Map.of("a", "1")))
        .isNotEqualTo(MapIdGenerator.generateId(base));
    assertThat(MapIdGenerator.generateId(Map.of("a", "b\0c")))
        .isNotEqualTo(MapIdGenerator.generateId(Map.of("a\0b", "c")));
    assertThat(MapIdGenerator.generateId(Map.of("k", "café")))
        .isNotEqualTo(MapIdGenerator.generateId(Map.of("k", "cafe")));
    assertThat(MapIdGenerator.generateId(Map.of("", "v")))
        .isNotEqualTo(MapIdGenerator.generateId(Map.of("k", "")));
  }

  @Test
  void rejectsNullInputs() {
    assertThatThrownBy(() -> MapIdGenerator.generateId(null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("map cannot be null");

    Map<String, String> nullKey = new HashMap<>();
    nullKey.put(null, "v");
    assertThatThrownBy(() -> MapIdGenerator.generateId(nullKey))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("map key cannot be null");

    Map<String, String> nullValue = new HashMap<>();
    nullValue.put("k", null);
    assertThatThrownBy(() -> MapIdGenerator.generateId(nullValue))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("map value cannot be null");
  }
}
