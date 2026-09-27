package io.unitycatalog.server.service.credential.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Clock;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class CredentialCacheStoreContextTest {

  @Test
  void exposesFieldsAndCopiesTheMapDefensively() {
    Map<String, String> src = new HashMap<>();
    src.put("endpoint", "redis://h:6379");
    CredentialCacheStoreContext ctx = new CredentialCacheStoreContext(Clock.systemUTC(), 1000, src);

    assertEquals(1000, ctx.maxSize());
    assertEquals("redis://h:6379", ctx.backendProperties().get("endpoint"));

    // Defensive copy: mutating the source after construction must not change the context.
    src.put("endpoint", "mutated");
    assertEquals("redis://h:6379", ctx.backendProperties().get("endpoint"));
    // And the exposed map is unmodifiable.
    assertThrows(UnsupportedOperationException.class, () -> ctx.backendProperties().put("x", "y"));
  }

  @Test
  void rejectsNulls() {
    assertThrows(
        NullPointerException.class, () -> new CredentialCacheStoreContext(null, 1, Map.of()));
    assertThrows(
        NullPointerException.class,
        () -> new CredentialCacheStoreContext(Clock.systemUTC(), 1, null));
  }
}
