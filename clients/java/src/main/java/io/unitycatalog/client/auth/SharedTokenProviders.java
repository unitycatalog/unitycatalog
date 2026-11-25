package io.unitycatalog.client.auth;

import io.unitycatalog.client.internal.BoundedKeyedCache;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * JVM-wide OAuth token providers shared by every caller with the same auth configuration.
 *
 * <p>Each {@link OAuthTokenProvider} caches its own token, so callers that build a new client per
 * operation would fetch a new token per operation. Sharing one provider per configuration lets them
 * reuse the cached token instead. The key is the config itself, compared by equality, so different
 * principals never share a provider and a rotated secret gets a new one.
 *
 * <p>The cache keeps the most recently looked-up providers, up to the system property {@code
 * unitycatalog.oauth.tokenProviders.cache.maxSize} (default 100). Evicting an entry is safe:
 * callers keep the provider they already hold, and the next lookup creates a new one.
 */
final class SharedTokenProviders {

  private static final String MAX_SIZE_KEY = "unitycatalog.oauth.tokenProviders.cache.maxSize";
  private static final int DEFAULT_MAX_SIZE = 100;

  private static final BoundedKeyedCache<Map<String, String>, TokenProvider> PROVIDERS =
      new BoundedKeyedCache<>(
          Integer.getInteger(MAX_SIZE_KEY, DEFAULT_MAX_SIZE), BoundedKeyedCache.noOpListener());

  private SharedTokenProviders() {}

  /** Returns the shared OAuth provider for {@code configs}, creating it on first use. */
  static TokenProvider get(Map<String, String> configs) {
    return PROVIDERS.getOrLoad(
        cacheKey(configs),
        () -> {
          OAuthTokenProvider provider = new OAuthTokenProvider();
          provider.initialize(configs);
          return provider;
        });
  }

  /**
   * The entries with lower-cased keys. The configs are a case-insensitive map, whose hash code
   * still depends on key casing, so they can't be the key as they are.
   */
  private static Map<String, String> cacheKey(Map<String, String> configs) {
    Map<String, String> key = new HashMap<>();
    configs.forEach((name, value) -> key.put(name.toLowerCase(Locale.ROOT), value));
    return Collections.unmodifiableMap(key);
  }
}
