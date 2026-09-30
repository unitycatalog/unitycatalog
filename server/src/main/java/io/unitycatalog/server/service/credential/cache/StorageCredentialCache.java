package io.unitycatalog.server.service.credential.cache;

import io.unitycatalog.server.model.AwsIamRoleResponse;
import io.unitycatalog.server.model.TemporaryCredentials;
import io.unitycatalog.server.persist.dao.CredentialDAO;
import io.unitycatalog.server.service.credential.CloudCredentialVendor;
import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.ServerProperties;
import io.unitycatalog.server.utils.cache.Cache;
import io.unitycatalog.server.utils.cache.CaffeineCache;
import io.unitycatalog.server.utils.cache.FailSafeCache;
import io.unitycatalog.server.utils.cache.ReadThroughCache;
import java.time.Clock;
import java.util.Objects;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Caches vended cloud storage credentials, keyed by the resolved binding {@code (location,
 * privileges, scheme, roleArn, externalId)}. Sits inside {@link
 * io.unitycatalog.server.service.credential.StorageCredentialVendor} after the (never-cached) DB
 * binding resolution: on a hit it skips the cloud vend; on miss/stale/rebind it re-vends. The value
 * is validated on every hit ({@code matches && fresh}). Ordinary backend exceptions degrade to
 * misses/no-ops; interruptions and {@link Error}s propagate.
 *
 * <p>A {@link Clock} is injected so freshness is deterministic under test; production uses {@link
 * Clock#systemUTC()}. The same clock drives both the freshness validator and the default cache's
 * expiry.
 */
public class StorageCredentialCache {
  private static final Logger LOGGER = LoggerFactory.getLogger(StorageCredentialCache.class);
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

    this.readThrough =
        new ReadThroughCache<>(
            new FailSafeCache<>(buildStore(serverProperties, clock)),
            (key, cached) ->
                cached.context().matches(key)
                    && cached.context().fresh(clock.instant().toEpochMilli(), leadMs));
  }

  /** Builds the cache store tier: a reflectively-loaded custom backend, or the default Caffeine. */
  private Cache<CredentialCacheKey, CachedCredential> buildStore(
      ServerProperties serverProperties, Clock clock) {
    CredentialCacheStoreContext context =
        new CredentialCacheStoreContext(
            clock,
            serverProperties.getStorageCredentialCacheMaxSize(),
            serverProperties.getStorageCredentialCacheBackendProperties());
    return serverProperties
        .getStorageCredentialCacheBackend()
        .map(fqcn -> loadBackend(fqcn, context))
        .orElseGet(
            () ->
                new CaffeineCache<>(
                    context.maxSize(),
                    cached -> cached.context().effectiveExpiryEpochMs(),
                    context.clock()));
  }

  /**
   * Loads a custom {@link CredentialCacheBackend} by class name, preferring a {@link
   * CredentialCacheStoreContext} constructor and falling back to a no-arg one. Mirrors {@code
   * GcpCredentialVendor.createGenerator}; a load failure fails construction (server startup) with
   * the fqcn named.
   */
  private static Cache<CredentialCacheKey, CachedCredential> loadBackend(
      String fqcn, CredentialCacheStoreContext context) {
    try {
      Class<? extends CredentialCacheBackend> type =
          Class.forName(fqcn).asSubclass(CredentialCacheBackend.class);
      try {
        return type.getDeclaredConstructor(CredentialCacheStoreContext.class).newInstance(context);
      } catch (NoSuchMethodException noContextCtor) {
        if (!context.backendProperties().isEmpty()) {
          LOGGER.warn(
              "Storage-credential-cache backend {} has no CredentialCacheStoreContext constructor; "
                  + "ignoring {} configured backend property key(s): {}",
              fqcn,
              context.backendProperties().size(),
              context.backendProperties().keySet());
        }
        return type.getDeclaredConstructor().newInstance();
      }
    } catch (ReflectiveOperationException | ClassCastException e) {
      throw new IllegalStateException(
          "Failed to load storage-credential-cache backend: " + fqcn, e);
    }
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
            key.externalId(),
            credential.getExpirationTime(), // T1 (nullable = static credential)
            now + maxAgeMs); // T2
    return new CachedCredential(ctx, credential);
  }

  private static CredentialCacheKey keyOf(CredentialContext context, NormalizedURL location) {
    // Per-bucket/config vends have no DAO, so both binding fields remain null.
    // When a non-AWS DB-backed credential type is added to CredentialDAO.CredentialType,
    // extend this to fold that type's distinguishing identity into the key, or different
    // bindings could collide.
    Optional<AwsIamRoleResponse> awsIamRole =
        context.getCredentialDAO().map(CredentialDAO::getAwsIamRoleResponse);
    return new CredentialCacheKey(
        location,
        context.getPrivileges(),
        context.getStorageScheme(),
        awsIamRole.map(AwsIamRoleResponse::getRoleArn).orElse(null),
        awsIamRole.map(AwsIamRoleResponse::getExternalId).orElse(null));
  }
}
