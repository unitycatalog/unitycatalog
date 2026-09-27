package io.unitycatalog.server.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import org.junit.jupiter.api.Test;

class ServerPropertiesBackendTest {

  private static ServerProperties props(String... kv) {
    Properties p = new Properties();
    for (int i = 0; i < kv.length; i += 2) {
      p.setProperty(kv[i], kv[i + 1]);
    }
    return new ServerProperties(p);
  }

  @Test
  void backendFqcnIsOptional() {
    assertEquals(Optional.empty(), props().getStorageCredentialCacheBackend());
    assertEquals(
        Optional.of("com.acme.Store"),
        props("server.storage-credential-cache.backend", "com.acme.Store")
            .getStorageCredentialCacheBackend());
  }

  @Test
  void backendPropertiesAreScopedAndStripped() {
    ServerProperties p =
        props(
            "server.storage-credential-cache.backend", "com.acme.Store",
            "server.storage-credential-cache.backend.endpoint", "redis://h:6379",
            "server.storage-credential-cache.backend.namespace", "prod",
            "server.storage-credential-cache.enabled", "true",
            "server.storage-credential-cache.max-size", "1000",
            "s3.secretKey.0", "supersecret"); // a real secret from server config must not leak
    Map<String, String> m = p.getStorageCredentialCacheBackendProperties();

    assertEquals("redis://h:6379", m.get("endpoint")); // prefix stripped
    assertEquals("prod", m.get("namespace"));
    assertEquals(2, m.size()); // ONLY the backend.* subtree
    assertFalse(m.containsKey("enabled")); // sibling cache keys excluded
    assertFalse(m.containsKey("max-size"));
    assertFalse(m.containsKey("backend")); // the fqcn key itself is not config
    assertFalse(m.containsKey("secretKey.0")); // other server secrets must not leak to the backend
    assertFalse(m.containsValue("supersecret")); // the secret value itself must not appear
  }

  @Test
  void backendPropertiesEmptyWhenNone() {
    assertTrue(props().getStorageCredentialCacheBackendProperties().isEmpty());
  }

  @Test
  void blankBackendIsTreatedAsUnset() {
    assertEquals(
        Optional.empty(),
        props("server.storage-credential-cache.backend", "").getStorageCredentialCacheBackend());
  }

  @Test
  void whitespaceBackendIsTreatedAsUnset() {
    assertEquals(
        Optional.empty(),
        props("server.storage-credential-cache.backend", "   ").getStorageCredentialCacheBackend());
  }
}
