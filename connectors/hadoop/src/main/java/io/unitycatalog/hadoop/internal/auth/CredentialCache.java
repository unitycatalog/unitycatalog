package io.unitycatalog.hadoop.internal.auth;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.hadoop.internal.util.BoundedKeyedCache;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Caches vended credentials keyed by the supplied key ({@code K}). A cached value is reused while
 * it is still valid and re-fetched via the supplied factory only once it is about to expire, as
 * decided by the cached entry's {@link RenewableCredential#readyToRenew()} implementation.
 */
public class CredentialCache<K, T> {
  private static final int DEFAULT_MAX_SIZE = 1024;
  private static final String GLOBAL_CACHE_MAX_SIZE_KEY = "unitycatalog.credential.cache.maxSize";
  private static final String INITIAL_CACHE_MAX_SIZE_KEY =
      "unitycatalog.initial.credential.cache.maxSize";

  private final BoundedKeyedCache<K, RenewableCredential<T>> cache;

  public CredentialCache(int maxSize) {
    this.cache = new BoundedKeyedCache<>(maxSize);
  }

  /**
   * Creates the JVM-wide cache used by the Hadoop token providers to renew and share vended
   * credentials across requests targeting the same scope, saving QPS to the Unity Catalog server.
   */
  public static <K, T> CredentialCache<K, T> createGlobalCache() {
    return new CredentialCache<>(Integer.getInteger(GLOBAL_CACHE_MAX_SIZE_KEY, DEFAULT_MAX_SIZE));
  }

  /**
   * Creates the cache for the initial credential fetched during query planning (e.g. on the Spark
   * driver) so that different queries targeting the same scope reuse the same vended credential
   * instead of re-fetching from the Unity Catalog server.
   */
  public static <K, T> CredentialCache<K, T> createInitialCredentialCache() {
    return new CredentialCache<>(Integer.getInteger(INITIAL_CACHE_MAX_SIZE_KEY, DEFAULT_MAX_SIZE));
  }

  /**
   * Returns the value for {@code key}, handling three cases:
   *
   * <ul>
   *   <li>Cached and still valid: return it as is.
   *   <li>Cached but about to expire: create a fresh one via {@code factory}, cache it, return it.
   *   <li>Not cached: create it via {@code factory}, cache it, return it.
   * </ul>
   */
  public T access(K key, RenewableCredentialFactory<T> factory) throws ApiException {
    synchronized (cache) {
      RenewableCredential<T> cached = cache.getIfPresent(key);
      // Reuse the cached value while it's still valid; otherwise fetch and cache a fresh one.
      if (cached != null && !cached.readyToRenew()) {
        return cached.credential();
      }

      RenewableCredential<T> created = factory.create();
      cache.put(key, created);
      return created.credential();
    }
  }

  /** Removes all cached values. Public so tests in other packages can reset shared caches. */
  public void clear() {
    cache.clear();
  }

  // Visible for testing only.
  int size() {
    return cache.size();
  }

  // Visible for testing only.
  List<T> values() {
    return cache.values().stream()
        .map(RenewableCredential::credential)
        .collect(Collectors.toList());
  }

  @FunctionalInterface
  public interface RenewableCredentialFactory<T> {
    RenewableCredential<T> create() throws ApiException;
  }

  public abstract static class RenewableCredential<T> {
    private final T credential;

    public RenewableCredential(T credential) {
      this.credential = credential;
    }

    public T credential() {
      return credential;
    }

    public abstract boolean readyToRenew();
  }
}
