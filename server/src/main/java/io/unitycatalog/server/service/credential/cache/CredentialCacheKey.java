package io.unitycatalog.server.service.credential.cache;

import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.UriScheme;
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
    String roleArn) {}
