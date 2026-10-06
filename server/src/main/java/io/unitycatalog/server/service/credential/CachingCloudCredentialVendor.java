package io.unitycatalog.server.service.credential;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.unitycatalog.server.model.AwsIamRoleResponse;
import io.unitycatalog.server.model.TemporaryCredentials;
import io.unitycatalog.server.persist.dao.CredentialDAO;
import io.unitycatalog.server.utils.JsonUtils;
import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.ServerProperties;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import lombok.SneakyThrows;

/**
 * A {@link CloudCredentialVendor} that reuses vended credentials instead of calling the cloud
 * provider on every request. {@link StorageCredentialVendor} resolves the database binding before
 * calling it, so only the cloud vend is skipped; the binding is re-read on every request.
 *
 * <p>An entry is keyed by {@code (locations, privileges, roleArn, externalId)}. It is served while
 * {@code now < credentialExpiry - renewalLeadTime} and {@code now < vendTime + maxAge}; a static
 * credential with no expiry is bounded by the max age alone. Callers always receive a copy, so they
 * cannot change a cached credential.
 */
public class CachingCloudCredentialVendor extends CloudCredentialVendor {
  private final CloudCredentialVendor delegate;
  private final Clock clock;
  private final long renewalLeadMs;
  private final long maxAgeMs;
  private final Cache<Key, Entry> entries;

  public CachingCloudCredentialVendor(
      CloudCredentialVendor delegate, ServerProperties serverProperties) {
    this(delegate, serverProperties, Clock.systemUTC());
  }

  /** Uses {@code clock} to decide freshness; tests pass a manual clock to control expiry. */
  public CachingCloudCredentialVendor(
      CloudCredentialVendor delegate, ServerProperties serverProperties, Clock clock) {
    // Vending is delegated, so this instance needs no cloud-specific vendors of its own.
    super(/* awsCredentialVendor= */ null, /* azure= */ null, /* gcp= */ null);
    this.delegate = Objects.requireNonNull(delegate, "delegate");
    this.clock = Objects.requireNonNull(clock, "clock");
    this.renewalLeadMs = serverProperties.getStorageCredentialCacheRenewalLeadTime().toMillis();
    this.maxAgeMs = serverProperties.getStorageCredentialCacheMaxAge().toMillis();
    // Expiry here only bounds memory; whether an entry may be served is decided by Entry#isFresh.
    this.entries =
        Caffeine.newBuilder()
            .maximumSize(serverProperties.getStorageCredentialCacheMaxSize())
            .expireAfterWrite(Duration.ofMillis(maxAgeMs))
            // Run Caffeine's small eviction work on the calling thread, not the common pool.
            .executor(Runnable::run)
            .build();
  }

  @Override
  public TemporaryCredentials vendCredential(CredentialContext context) {
    Key key = Key.of(context);
    Entry cached = entries.getIfPresent(key);
    if (cached != null && cached.isFresh(now(), renewalLeadMs)) {
      return copy(cached.credential());
    }
    TemporaryCredentials vended = delegate.vendCredential(context);
    long vendedAtMs = now();
    Entry entry = new Entry(copy(vended), vendedAtMs + maxAgeMs);
    // A credential that is already inside its renewal window would never be served from the cache.
    if (entry.isFresh(vendedAtMs, renewalLeadMs)) {
      entries.put(key, entry);
    }
    return vended;
  }

  private long now() {
    return clock.instant().toEpochMilli();
  }

  /**
   * The resolved credential binding. Changing the role or external ID of a location's credential
   * changes the key, so credentials cached under the previous binding are never reused.
   */
  private record Key(
      List<NormalizedURL> locations,
      Set<CredentialContext.Privilege> privileges,
      String roleArn,
      String externalId) {
    Key {
      // AWS and GCP scope a credential to every location in the context, so the key has them all.
      locations = List.copyOf(locations);
      privileges = Set.copyOf(privileges);
    }

    static Key of(CredentialContext context) {
      // Per-bucket config vends have no credential DAO, so both role fields are null. AWS IAM role
      // is the only DB-backed credential type today; a new type must add its binding to the key.
      Optional<AwsIamRoleResponse> awsIamRole =
          context.getCredentialDAO().map(CredentialDAO::getAwsIamRoleResponse);
      return new Key(
          context.getLocations(),
          context.getPrivileges(),
          awsIamRole.map(AwsIamRoleResponse::getRoleArn).orElse(null),
          awsIamRole.map(AwsIamRoleResponse::getExternalId).orElse(null));
    }
  }

  private record Entry(TemporaryCredentials credential, long maxAgeExpiresAtMs) {
    boolean isFresh(long nowMs, long renewalLeadMs) {
      Long expiresAtMs = credential.getExpirationTime();
      return (expiresAtMs == null || nowMs < expiresAtMs - renewalLeadMs)
          && nowMs < maxAgeExpiresAtMs;
    }
  }

  /** Deep-copies through JSON so every model field is carried, including ones added later. */
  @SneakyThrows
  private static TemporaryCredentials copy(TemporaryCredentials source) {
    ObjectMapper mapper = JsonUtils.getInstance();
    return mapper.readValue(mapper.writeValueAsString(source), TemporaryCredentials.class);
  }
}
