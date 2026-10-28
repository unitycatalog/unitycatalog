package io.unitycatalog.client.auth;

import io.unitycatalog.client.internal.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * JVM-wide OAuth token providers shared by every caller with the same auth configuration.
 *
 * <p>Each {@link OAuthTokenProvider} caches its own token, so callers that build a new client per
 * operation fetch a new token per operation. Sharing one provider per configuration lets them reuse
 * the cached token instead. The key is the config itself, compared by equality, so different
 * principals never share a provider and a rotated secret gets a new one.
 *
 * <p>Evicting an entry is always safe: callers keep the provider they already hold, and the next
 * lookup creates a new one. Entries not looked up for longer than {@link #IDLE_TIMEOUT} are
 * evicted, which also drops providers for rotated secrets. Using a provider does not count as a
 * lookup, so a long-lived caller may see its entry evicted; it only costs one token on the next
 * lookup.
 */
final class SharedTokenProviders {

  static final Duration IDLE_TIMEOUT = Duration.ofHours(1);
  private static final Duration SWEEP_INTERVAL = Duration.ofMinutes(1);

  private static final SharedTokenProviders INSTANCE =
      new SharedTokenProviders(Clock.systemClock());

  private final Clock clock;
  private final ConcurrentHashMap<Map<String, String>, Entry> providers = new ConcurrentHashMap<>();
  private volatile Instant lastSweep;

  SharedTokenProviders(Clock clock) {
    this.clock = clock;
    this.lastSweep = clock.now();
  }

  static SharedTokenProviders instance() {
    return INSTANCE;
  }

  /** Returns the shared OAuth provider for {@code configs}, creating it on first use. */
  TokenProvider get(Map<String, String> configs) {
    Map<String, String> key = cacheKey(configs);
    Instant now = clock.now();
    Entry entry =
        providers.compute(
            key,
            (k, existing) -> {
              if (existing != null) {
                existing.lastAccess = now;
                return existing;
              }
              OAuthTokenProvider provider = new OAuthTokenProvider();
              provider.initialize(configs);
              return new Entry(provider, now);
            });
    sweepIfDue(now);
    return entry.provider;
  }

  int size() {
    return providers.size();
  }

  private void sweepIfDue(Instant now) {
    if (now.isBefore(lastSweep.plus(SWEEP_INTERVAL))) {
      return;
    }
    lastSweep = now;
    Instant idleCutoff = now.minus(IDLE_TIMEOUT);
    // Check and remove under the key's lock, like get, so a lookup that refreshes an entry
    // concurrently is never evicted right after.
    for (Map<String, String> key : providers.keySet()) {
      providers.computeIfPresent(
          key, (k, entry) -> entry.lastAccess.isBefore(idleCutoff) ? null : entry);
    }
  }

  /** The entries with lower-cased keys, without the sharing flag itself. */
  private static Map<String, String> cacheKey(Map<String, String> configs) {
    Map<String, String> key = new HashMap<>();
    configs.forEach(
        (name, value) -> {
          String normalizedName = name.toLowerCase(Locale.ROOT);
          if (!normalizedName.equals(AuthConfigs.OAUTH_SHARED.toLowerCase(Locale.ROOT))) {
            key.put(normalizedName, value);
          }
        });
    return Collections.unmodifiableMap(key);
  }

  private static final class Entry {
    private final TokenProvider provider;
    private volatile Instant lastAccess;

    private Entry(TokenProvider provider, Instant lastAccess) {
      this.provider = provider;
      this.lastAccess = lastAccess;
    }
  }
}
