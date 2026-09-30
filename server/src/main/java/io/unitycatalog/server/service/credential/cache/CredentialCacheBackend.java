package io.unitycatalog.server.service.credential.cache;

import io.unitycatalog.server.utils.cache.Cache;

/**
 * Customer-provided storage credential cache with fixed key and value types.
 *
 * <p>Configured backends must implement this interface and expose a public constructor accepting
 * {@link CredentialCacheStoreContext}, or a public no-argument constructor.
 *
 * <p>Implementations must support concurrent calls to their cache methods.
 */
public interface CredentialCacheBackend extends Cache<CredentialCacheKey, CachedCredential> {}
