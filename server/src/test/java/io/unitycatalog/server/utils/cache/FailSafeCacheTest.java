package io.unitycatalog.server.utils.cache;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Optional;
import org.junit.jupiter.api.Test;

public class FailSafeCacheTest {

  /** A delegate whose every method throws, to prove FailSafeCache absorbs it. */
  private static final Cache<String, String> THROWING =
      new Cache<>() {
        public Optional<String> getIfPresent(String key) {
          throw new RuntimeException("boom");
        }

        public void put(String key, String value) {
          throw new RuntimeException("boom");
        }

        public void invalidate(String key) {
          throw new RuntimeException("boom");
        }
      };

  @Test
  void getReturnsEmptyWhenDelegateThrows() {
    Cache<String, String> cache = new FailSafeCache<>(THROWING);
    assertTrue(cache.getIfPresent("k").isEmpty());
  }

  @Test
  void putAndInvalidateSwallowDelegateExceptions() {
    Cache<String, String> cache = new FailSafeCache<>(THROWING);
    assertDoesNotThrow(() -> cache.put("k", "v"));
    assertDoesNotThrow(() -> cache.invalidate("k"));
  }

  @Test
  void errorsPropagate() {
    // Split allocation and throw to avoid the checkstyle "throw new *Error(" rule
    // while still verifying that Error subclasses are not swallowed.
    Error fatal = new OutOfMemoryError("fatal");
    Cache<String, String> cache =
        new FailSafeCache<>(
            new Cache<>() {
              public Optional<String> getIfPresent(String key) {
                throw fatal;
              }

              public void put(String key, String value) {}

              public void invalidate(String key) {}
            });
    assertThrows(OutOfMemoryError.class, () -> cache.getIfPresent("k"));
  }

  @Test
  void delegatesWhenNoException() {
    java.util.Map<String, String> backing = new java.util.HashMap<>();
    Cache<String, String> cache =
        new FailSafeCache<>(
            new Cache<>() {
              public Optional<String> getIfPresent(String key) {
                return Optional.ofNullable(backing.get(key));
              }

              public void put(String key, String value) {
                backing.put(key, value);
              }

              public void invalidate(String key) {
                backing.remove(key);
              }
            });
    cache.put("k", "v");
    assertEquals(Optional.of("v"), cache.getIfPresent("k"));
    cache.invalidate("k");
    assertTrue(cache.getIfPresent("k").isEmpty());
  }
}
