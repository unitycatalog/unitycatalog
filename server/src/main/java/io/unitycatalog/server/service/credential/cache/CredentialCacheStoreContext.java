package io.unitycatalog.server.service.credential.cache;

import java.time.Clock;
import java.util.Map;
import java.util.Objects;

/**
 * Construction inputs handed to a storage-credential-cache store backend. Carries only what a store
 * needs to build itself: the injected {@link Clock} (system clock in production, a manual clock
 * under test), the {@code max-size} bound, and {@code backendProperties} — the scoped, read-only
 * {@code server.storage-credential-cache.backend.*} config subtree (prefix stripped). Deliberately
 * not the whole {@link io.unitycatalog.server.utils.ServerProperties}, so a plugin never sees
 * unrelated server config. Mirrors the typed construction context of the cloud credential vendors.
 */
public record CredentialCacheStoreContext(
    Clock clock, int maxSize, Map<String, String> backendProperties) {
  public CredentialCacheStoreContext {
    Objects.requireNonNull(clock, "clock");
    Objects.requireNonNull(backendProperties, "backendProperties");
    backendProperties = Map.copyOf(backendProperties);
  }
}
