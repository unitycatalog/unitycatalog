package io.unitycatalog.server.service.credential.cache;

import io.unitycatalog.server.model.TemporaryCredentials;

/** The cache value: the vended credential plus the context used to validate it on every hit. */
public record CachedCredential(CredentialCacheContext context, TemporaryCredentials credential) {}
