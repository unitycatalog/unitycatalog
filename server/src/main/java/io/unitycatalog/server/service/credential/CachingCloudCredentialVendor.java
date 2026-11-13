package io.unitycatalog.server.service.credential;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import io.unitycatalog.server.model.AwsIamRoleResponse;
import io.unitycatalog.server.model.AzureServicePrincipalRequest;
import io.unitycatalog.server.model.GcpServiceAccountKeyRequest;
import io.unitycatalog.server.model.TemporaryCredentials;
import io.unitycatalog.server.observability.StorageCredentialCacheMetrics;
import io.unitycatalog.server.observability.UnityCatalogMetrics;
import io.unitycatalog.server.persist.dao.CredentialDAO;
import io.unitycatalog.server.service.credential.aws.AwsCredentialVendor;
import io.unitycatalog.server.service.credential.azure.AzureCredentialVendor;
import io.unitycatalog.server.service.credential.gcp.GcpCredentialVendor;
import io.unitycatalog.server.utils.JsonUtils;
import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.ServerProperties;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import lombok.SneakyThrows;

/**
 * A {@link CloudCredentialVendor} that reuses vended credentials instead of calling the cloud
 * provider on every request; on a miss it vends through the inherited {@link
 * CloudCredentialVendor#vendCredential}. {@link StorageCredentialVendor} resolves the database
 * binding before calling it, so only the cloud vend is skipped; the binding is re-read on every
 * request.
 *
 * <p>An entry is keyed by {@code (locations, privileges, credential binding)}. It is served until
 * {@code min(vendTime + maxAge, credentialExpiry - expiryMargin)}, where {@code vendTime} is read
 * before the cloud call; a static credential with no expiry is bounded by the max age alone.
 * Callers always receive a copy, so they cannot change a cached credential.
 *
 * <p>Each request is recorded as a hit or a miss in {@link StorageCredentialCacheMetrics}.
 */
public class CachingCloudCredentialVendor extends CloudCredentialVendor {
  /** A cached credential is never served within this time of its expiry. */
  static final Duration DEFAULT_EXPIRY_MARGIN = Duration.ofMinutes(1);

  private final Clock clock;
  private final long maxAgeMs;
  private final long expiryMarginMs;
  private final Cache<Key, Entry> entries;
  private final StorageCredentialCacheMetrics cacheMetrics;

  /** Records each request's cache hit or miss in {@code metrics}. */
  public CachingCloudCredentialVendor(
      ServerProperties serverProperties, UnityCatalogMetrics metrics) {
    this(serverProperties, Clock.systemUTC(), DEFAULT_EXPIRY_MARGIN, metrics);
  }

  /**
   * Uses {@code clock} to decide when an entry expires and {@code expiryMargin} in place of {@link
   * #DEFAULT_EXPIRY_MARGIN}; tests pass a manual clock and a margin shorter than their credentials.
   */
  public CachingCloudCredentialVendor(
      ServerProperties serverProperties,
      Clock clock,
      Duration expiryMargin,
      UnityCatalogMetrics metrics) {
    this(
        new AwsCredentialVendor(serverProperties),
        new AzureCredentialVendor(serverProperties),
        new GcpCredentialVendor(serverProperties),
        serverProperties,
        clock,
        expiryMargin,
        metrics);
  }

  /** Vends through the given cloud-specific vendors; unit tests pass mocks. */
  CachingCloudCredentialVendor(
      AwsCredentialVendor awsCredentialVendor,
      AzureCredentialVendor azureCredentialVendor,
      GcpCredentialVendor gcpCredentialVendor,
      ServerProperties serverProperties,
      Clock clock,
      Duration expiryMargin,
      UnityCatalogMetrics metrics) {
    super(awsCredentialVendor, azureCredentialVendor, gcpCredentialVendor);
    this.clock = Objects.requireNonNull(clock, "clock");
    this.maxAgeMs = serverProperties.getStorageCredentialCacheMaxAge().toMillis();
    this.expiryMarginMs = Objects.requireNonNull(expiryMargin, "expiryMargin").toMillis();
    this.cacheMetrics = Objects.requireNonNull(metrics, "metrics").getStorageCredentialCache();
    this.entries =
        Caffeine.newBuilder()
            .maximumSize(serverProperties.getStorageCredentialCacheMaxSize())
            .ticker(() -> TimeUnit.MILLISECONDS.toNanos(now()))
            .expireAfter(new UntilServeDeadline())
            // Run Caffeine's small eviction work on the calling thread, not the common pool.
            .executor(Runnable::run)
            .build();
  }

  @Override
  public TemporaryCredentials vendCredential(CredentialContext context) {
    Key key = Key.of(context);
    Entry cached = entries.getIfPresent(key);
    // Check the entry's own deadline too: a renewal can overwrite the cache entry while this read
    // is in progress, so Caffeine may judge the old credential by the new entry's deadline.
    if (cached != null && cached.serveUntilMs() > now()) {
      cacheMetrics.recordHit();
      return copy(cached.credential());
    }
    cacheMetrics.recordMiss();
    // Read before the call: the cloud checks the binding (for example the AWS trust policy) during
    // the vend, so max age counts from then.
    long vendStartMs = now();
    TemporaryCredentials vended = super.vendCredential(context);
    long serveUntilMs = vendStartMs + maxAgeMs;
    if (vended.getExpirationTime() != null) {
      serveUntilMs = Math.min(serveUntilMs, vended.getExpirationTime() - expiryMarginMs);
    }
    // A credential already past its serve deadline is returned but not cached, so it cannot replace
    // a valid entry that a concurrent request just cached.
    if (serveUntilMs > now()) {
      entries.put(key, new Entry(copy(vended), serveUntilMs));
    }
    return vended;
  }

  private long now() {
    return clock.instant().toEpochMilli();
  }

  /**
   * The resolved credential binding: the cloud identity a storage credential vends with, and what
   * qualifies it. Changing either on a location's credential changes the key, so credentials cached
   * under the previous binding are never reused. Per-bucket config vends have no credential DAO, so
   * their binding fields are null.
   *
   * <ul>
   *   <li>AWS IAM role: the role ARN, qualified by the external ID.
   *   <li>Azure service principal: the application ID, qualified by the directory ID. A rotated
   *       client secret keeps the binding; SAS tokens already issued do not depend on it.
   *   <li>GCP service account key: the service account email, qualified by the private key ID.
   * </ul>
   */
  private record Key(
      List<NormalizedURL> locations,
      Set<CredentialContext.Privilege> privileges,
      CredentialDAO.CredentialType credentialType,
      String identity,
      String qualifier) {
    Key {
      // AWS and GCP scope a credential to every location in the context, so the key has them all.
      locations = List.copyOf(locations);
      privileges = Set.copyOf(privileges);
    }

    static Key of(CredentialContext context) {
      return context
          .getCredentialDAO()
          .map(credential -> of(context, credential))
          .orElseGet(
              () -> new Key(context.getLocations(), context.getPrivileges(), null, null, null));
    }

    private static Key of(CredentialContext context, CredentialDAO credential) {
      CredentialDAO.CredentialType type = credential.getCredentialType();
      return switch (type) {
        case AWS_IAM_ROLE -> {
          AwsIamRoleResponse role = credential.getAwsIamRoleResponse();
          yield new Key(
              context.getLocations(),
              context.getPrivileges(),
              type,
              role.getRoleArn(),
              role.getExternalId());
        }
        case AZURE_SERVICE_PRINCIPAL -> {
          AzureServicePrincipalRequest servicePrincipal = credential.getAzureServicePrincipal();
          yield new Key(
              context.getLocations(),
              context.getPrivileges(),
              type,
              servicePrincipal.getApplicationId(),
              servicePrincipal.getDirectoryId());
        }
        case GCP_SERVICE_ACCOUNT_KEY -> {
          GcpServiceAccountKeyRequest key = credential.getGcpServiceAccountKey();
          yield new Key(
              context.getLocations(),
              context.getPrivileges(),
              type,
              key.getEmail(),
              key.getPrivateKeyId());
        }
      };
    }
  }

  private record Entry(TemporaryCredentials credential, long serveUntilMs) {}

  /** Expires each entry at its serve deadline; Caffeine's time comes from {@link #clock}. */
  private static final class UntilServeDeadline implements Expiry<Key, Entry> {
    @Override
    public long expireAfterCreate(Key key, Entry entry, long currentTimeNanos) {
      return TimeUnit.MILLISECONDS.toNanos(entry.serveUntilMs()) - currentTimeNanos;
    }

    @Override
    public long expireAfterUpdate(
        Key key, Entry entry, long currentTimeNanos, long currentDurationNanos) {
      return expireAfterCreate(key, entry, currentTimeNanos);
    }

    @Override
    public long expireAfterRead(
        Key key, Entry entry, long currentTimeNanos, long currentDurationNanos) {
      return currentDurationNanos;
    }
  }

  /** Deep-copies through JSON so every model field is carried, including ones added later. */
  @SneakyThrows
  private static TemporaryCredentials copy(TemporaryCredentials source) {
    ObjectMapper mapper = JsonUtils.getInstance();
    return mapper.readValue(mapper.writeValueAsString(source), TemporaryCredentials.class);
  }
}
