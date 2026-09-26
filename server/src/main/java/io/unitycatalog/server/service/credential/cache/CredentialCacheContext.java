package io.unitycatalog.server.service.credential.cache;

import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.UriScheme;
import java.util.Objects;
import java.util.Set;

/**
 * Self-describing context stored alongside the cached credential. Mirrors every key field
 * (validated in full on each hit — fail closed) plus the two expiry clocks: T1 the credential's own
 * expiry (nullable → static), T2 the service reuse cap. The authoritative freshness check ({@link
 * #fresh}) holds only while {@code now < min(T1 − lead, T2)}.
 */
public record CredentialCacheContext(
    int schemaVersion,
    NormalizedURL location,
    UriScheme scheme,
    Set<CredentialContext.Privilege> privileges,
    String roleArn,
    Long credentialExpiresAtEpochMs,
    long cacheExpiresAtEpochMs) {

  public static final int CURRENT_SCHEMA_VERSION = 1;

  public CredentialCacheContext {
    Objects.requireNonNull(location, "location");
    Objects.requireNonNull(scheme, "scheme");
    privileges = Set.copyOf(privileges); // defensive immutable snapshot; null set throws NPE here
    if (cacheExpiresAtEpochMs <= 0) {
      throw new IllegalArgumentException("cacheExpiresAtEpochMs must be positive");
    }
    if (credentialExpiresAtEpochMs != null && credentialExpiresAtEpochMs <= 0) {
      throw new IllegalArgumentException(
          "credentialExpiresAtEpochMs must be positive when non-null");
    }
    UriScheme derived = UriScheme.fromURI(location.toUri());
    if (derived != scheme) {
      throw new IllegalArgumentException(
          "scheme " + scheme + " does not match location " + location);
    }
    // roleArn stays nullable.
  }

  /**
   * Full compare of every key field and the current schema. Fail-closed: any mismatch (including a
   * stale schema version) returns false.
   */
  public boolean matches(CredentialCacheKey key) {
    return schemaVersion == CURRENT_SCHEMA_VERSION
        && location.equals(key.location())
        && scheme == key.scheme()
        && privileges.equals(key.privileges())
        && Objects.equals(roleArn, key.roleArn());
  }

  /**
   * The earliest wall-clock instant at which the cached result becomes unusable. When T1 is null
   * (static credential with no expiry), the cache cap T2 is the sole bound.
   *
   * @return {@code min(T1, T2)} when T1 is non-null, otherwise {@code T2}
   */
  public long effectiveExpiryEpochMs() {
    return credentialExpiresAtEpochMs == null
        ? cacheExpiresAtEpochMs
        : Math.min(credentialExpiresAtEpochMs, cacheExpiresAtEpochMs);
  }

  /**
   * Returns true when the cached credential is still fresh enough to serve without re-vending.
   *
   * <p>The {@code leadMs} is subtracted from T1 (the credential's own STS expiry) to ensure callers
   * receive credentials with enough remaining lifetime. It is <em>not</em> applied to T2 (the
   * service reuse cap), which is a hard cut-off.
   *
   * @param nowMs current epoch milliseconds
   * @param leadMs minimum remaining lifetime to require from T1; use 0 for no lead; must be
   *     non-negative
   * @return true iff {@code (T1 == null || now < T1 - lead) && now < T2}
   * @throws IllegalArgumentException if {@code leadMs} is negative
   */
  public boolean fresh(long nowMs, long leadMs) {
    if (leadMs < 0) {
      throw new IllegalArgumentException("leadMs must be non-negative");
    }
    boolean t1Ok =
        credentialExpiresAtEpochMs == null || nowMs < credentialExpiresAtEpochMs - leadMs;
    return t1Ok && nowMs < cacheExpiresAtEpochMs;
  }
}
