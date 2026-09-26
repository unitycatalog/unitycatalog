package io.unitycatalog.server.service.credential.cache;

import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.UriScheme;
import java.util.Objects;
import java.util.Set;

/**
 * Cache key = the resolved credential binding at the credential's own scope. {@code roleArn} is the
 * binding identity (null for per-bucket-config vends); {@code externalId} is intentionally excluded
 * (it gates the AssumeRole call, not the resulting session). A repoint to a different role changes
 * {@code roleArn} → different key → miss → correct re-vend.
 */
public record CredentialCacheKey(
    NormalizedURL location,
    Set<CredentialContext.Privilege> privileges,
    UriScheme scheme,
    String roleArn) {

  public CredentialCacheKey {
    Objects.requireNonNull(location, "location");
    Objects.requireNonNull(scheme, "scheme");
    privileges = Set.copyOf(privileges); // defensive immutable snapshot; null set throws NPE here
    // roleArn stays nullable (per-bucket vends) — no null-check.
    UriScheme derived = UriScheme.fromURI(location.toUri());
    if (derived != scheme) {
      throw new IllegalArgumentException(
          "scheme " + scheme + " does not match location " + location);
    }
  }
}
