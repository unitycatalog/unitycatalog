package io.unitycatalog.server.service.credential.cache;

import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.UriScheme;
import java.util.Objects;
import java.util.Set;

/**
 * Cache key = the resolved credential binding at the credential's own scope. {@code roleArn} and
 * {@code externalId} identify the AWS role-assumption binding (null for per-bucket-config vends).
 * Changing either field prevents reuse of credentials cached under the previous binding.
 */
public record CredentialCacheKey(
    NormalizedURL location,
    Set<CredentialContext.Privilege> privileges,
    UriScheme scheme,
    String roleArn,
    String externalId) {

  public CredentialCacheKey {
    Objects.requireNonNull(location, "location");
    Objects.requireNonNull(scheme, "scheme");
    privileges = Set.copyOf(privileges); // defensive immutable snapshot; null set throws NPE here
    // roleArn and externalId stay nullable (per-bucket vends).
    UriScheme derived = UriScheme.fromURI(location.toUri());
    if (derived != scheme) {
      throw new IllegalArgumentException(
          "scheme " + scheme + " does not match location " + location);
    }
  }
}
