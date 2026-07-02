package io.unitycatalog.hadoop.internal.auth;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.internal.Clock;
import io.unitycatalog.hadoop.internal.id.CredId;
import io.unitycatalog.hadoop.internal.util.BoundedKeyedCache;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Caches vended {@link GenericCredential}s keyed by their scope ({@link CredId}). A cached
 * credential is reused while it is still valid and re-fetched via the supplied factory only once it
 * is about to expire.
 */
public class CredentialCache {
  private final BoundedKeyedCache<CredId, RenewableCredential> cache;

  public CredentialCache(int maxSize) {
    this.cache = new BoundedKeyedCache<>(maxSize);
  }

  public GenericCredential access(CredId credId, RenewableCredentialFactory factory)
      throws ApiException {
    synchronized (cache) {
      RenewableCredential cached = cache.getIfPresent(credId);
      // Reuse the cached credential while it's still valid; otherwise fetch and cache a fresh one.
      if (cached != null && !cached.readyToRenew()) {
        return cached.credential();
      }

      RenewableCredential created = factory.create();
      cache.put(credId, created);
      return created.credential();
    }
  }

  public void clear() {
    cache.clear();
  }

  public int size() {
    return cache.size();
  }

  public List<GenericCredential> values() {
    return cache.values().stream()
        .map(RenewableCredential::credential)
        .collect(Collectors.toList());
  }

  @FunctionalInterface
  public interface RenewableCredentialFactory {
    RenewableCredential create() throws ApiException;
  }

  public static class RenewableCredential {
    private final long renewalLeadTimeMillis;
    private final Clock clock;
    private final GenericCredential credential;

    public RenewableCredential(
        long renewalLeadTimeMillis, Clock clock, GenericCredential credential) {
      this.renewalLeadTimeMillis = renewalLeadTimeMillis;
      this.clock = clock;
      this.credential = credential;
    }

    public GenericCredential credential() {
      return credential;
    }

    public boolean readyToRenew() {
      return credential.readyToRenew(clock, renewalLeadTimeMillis);
    }
  }
}
