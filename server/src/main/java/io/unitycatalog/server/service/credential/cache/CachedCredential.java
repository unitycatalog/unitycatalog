package io.unitycatalog.server.service.credential.cache;

import io.unitycatalog.server.model.TemporaryCredentials;
import java.util.Objects;

/** The cache value: the vended credential plus the context used to validate it on every hit. */
public record CachedCredential(CredentialCacheContext context, TemporaryCredentials credential) {

  public CachedCredential {
    Objects.requireNonNull(context, "context");
    Objects.requireNonNull(credential, "credential");
  }
}
