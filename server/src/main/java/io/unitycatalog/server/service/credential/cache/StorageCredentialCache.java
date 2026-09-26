package io.unitycatalog.server.service.credential.cache;

import io.unitycatalog.server.model.TemporaryCredentials;
import io.unitycatalog.server.persist.dao.CredentialDAO;
import io.unitycatalog.server.service.credential.CloudCredentialVendor;
import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.ServerProperties;
import io.unitycatalog.server.utils.cache.CaffeineCache;
import io.unitycatalog.server.utils.cache.FailSafeCache;
import io.unitycatalog.server.utils.cache.LayeredCache;
import io.unitycatalog.server.utils.cache.ReadThroughCache;
import java.time.Clock;
import java.util.List;
import java.util.Objects;

/**
 * Caches vended cloud storage credentials, keyed by the resolved binding {@code (location,
 * privileges, scheme, roleArn)}. Sits inside {@link
 * io.unitycatalog.server.service.credential.StorageCredentialVendor} after the (never-cached) DB
 * binding resolution: on a hit it skips the cloud vend; on miss/stale/rebind it re-vends. The value
 * is validated on every hit ({@code matches && fresh}); any cache-layer failure is non-terminal.
 *
 * <p>A {@link Clock} is injected so freshness is deterministic under test; production uses {@link
 * Clock#systemUTC()}. The same clock drives both the freshness validator and the L1 expiry ticker.
 */
public class StorageCredentialCache {
  private final CloudCredentialVendor cloudCredentialVendor;
  private final boolean enabled;
  private final long maxAgeMs;
  private final Clock clock;
  private final ReadThroughCache<CredentialCacheKey, CachedCredential> readThrough;

  public StorageCredentialCache(
      CloudCredentialVendor cloudCredentialVendor, ServerProperties serverProperties) {
    this(cloudCredentialVendor, serverProperties, Clock.systemUTC());
  }

  public StorageCredentialCache(
      CloudCredentialVendor cloudCredentialVendor, ServerProperties serverProperties, Clock clock) {
    this.cloudCredentialVendor =
        Objects.requireNonNull(cloudCredentialVendor, "cloudCredentialVendor");
    this.clock = Objects.requireNonNull(clock, "clock");
    this.enabled = serverProperties.isStorageCredentialCacheEnabled();
    // A disabled cache reads no tuning config and allocates no Caffeine — it must be zero-cost and
    // must not depend on the duration/size properties at all (also keeps it constructible from a
    // bare mock ServerProperties in vend tests, where enabled defaults to false).
    if (!enabled) {
      this.maxAgeMs = 0L;
      this.readThrough = null;
      return;
    }
    long leadMs = serverProperties.getStorageCredentialCacheRenewalLeadTime().toMillis();
    this.maxAgeMs = serverProperties.getStorageCredentialCacheMaxAge().toMillis();

    CaffeineCache<CredentialCacheKey, CachedCredential> l1 =
        new CaffeineCache<>(
            serverProperties.getStorageCredentialCacheMaxSize(),
            cached -> cached.context().effectiveExpiryEpochMs(),
            clock);
    LayeredCache<CredentialCacheKey, CachedCredential> layered =
        new LayeredCache<>(List.of(new FailSafeCache<>(l1)));
    this.readThrough =
        new ReadThroughCache<>(
            layered,
            (key, cached) ->
                cached.context().matches(key)
                    && cached.context().fresh(clock.instant().toEpochMilli(), leadMs));
  }

  /** Returns credentials for the resolved context, vending on miss/stale and caching the result. */
  public TemporaryCredentials get(CredentialContext context) {
    NormalizedURL location = context.getLocations().get(0);
    if (!enabled) {
      return cloudCredentialVendor.vendCredential(context).url(location.toString());
    }
    CredentialCacheKey key = keyOf(context, location);
    return readThrough.get(key, () -> load(context, key, location)).credential();
  }

  private CachedCredential load(
      CredentialContext context, CredentialCacheKey key, NormalizedURL location) {
    TemporaryCredentials credential =
        cloudCredentialVendor.vendCredential(context).url(location.toString());
    long now = clock.instant().toEpochMilli();
    CredentialCacheContext ctx =
        new CredentialCacheContext(
            CredentialCacheContext.CURRENT_SCHEMA_VERSION,
            key.location(),
            key.scheme(),
            key.privileges(),
            key.roleArn(),
            credential.getExpirationTime(), // T1 (nullable = static credential)
            now + maxAgeMs); // T2
    return new CachedCredential(ctx, credential);
  }

  private static CredentialCacheKey keyOf(CredentialContext context, NormalizedURL location) {
    // Only AWS_IAM_ROLE credential DAOs carry a role today; per-bucket/config vends have no DAO →
    // null role, which is a valid (per-bucket) key.
    String roleArn =
        context
            .getCredentialDAO()
            .map(CredentialDAO::getAwsIamRoleResponse)
            .map(response -> response.getRoleArn())
            .orElse(null);
    return new CredentialCacheKey(
        location, context.getPrivileges(), context.getStorageScheme(), roleArn);
  }
}
