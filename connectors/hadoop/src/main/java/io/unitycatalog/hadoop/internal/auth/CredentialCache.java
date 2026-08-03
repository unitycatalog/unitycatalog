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
  private static final int DEFAULT_MAX_SIZE = 1024;
  private static final String GLOBAL_CACHE_MAX_SIZE_KEY = "unitycatalog.credential.cache.maxSize";
  private static final String INITIAL_CACHE_MAX_SIZE_KEY =
      "unitycatalog.initial.credential.cache.maxSize";

  private final BoundedKeyedCache<CredId, RenewableCredential> cache;

  public CredentialCache(int maxSize) {
    this.cache = new BoundedKeyedCache<>(maxSize);
  }

  /**
   * Creates the JVM-wide cache used by the Hadoop token providers to renew and share vended
   * credentials across requests targeting the same scope, saving QPS to the Unity Catalog server.
   */
  public static CredentialCache createGlobalCache() {
    return new CredentialCache(Integer.getInteger(GLOBAL_CACHE_MAX_SIZE_KEY, DEFAULT_MAX_SIZE));
  }

  /**
   * Creates the cache for the initial credential fetched during query planning (e.g. on the Spark
   * driver) so that different queries targeting the same scope reuse the same vended credential
   * instead of re-fetching from the Unity Catalog server.
   */
  public static CredentialCache createInitialCredentialCache() {
    return new CredentialCache(Integer.getInteger(INITIAL_CACHE_MAX_SIZE_KEY, DEFAULT_MAX_SIZE));
  }

  /**
   * Returns the credential for {@code credId}, handling three cases:
   *
   * <ul>
   *   <li>Cached and still valid: return it as is.
   *   <li>Cached but about to expire: create a fresh one via {@code factory}, cache it, return it.
   *   <li>Not cached: create it via {@code factory}, cache it, return it.
   * </ul>
   */
  public GenericCredential access(CredId credId, RenewableCredentialFactory factory)
      throws ApiException {
    // Per-key locking: only accessors of the same scope contend, and only one of them performs
    // the renewal RPC while same-scope waiters briefly block and reuse the fresh credential.
    // Accessors of other scopes are never blocked by an in-flight renewal (#1651).
    return cache.getOrLoad(credId, cached -> !cached.readyToRenew(), factory::create).credential();
  }

  /** Removes all cached credentials. Public so tests in other packages can reset shared caches. */
  public void clear() {
    cache.clear();
  }

  // Visible for testing only.
  int size() {
    return cache.size();
  }

  // Visible for testing only.
  List<GenericCredential> credentials() {
    return cache.values().stream()
        .map(RenewableCredential::credential)
        .collect(Collectors.toList());
  }

  @FunctionalInterface
  public interface RenewableCredentialFactory {
    RenewableCredential create() throws ApiException;
  }

  /**
   * A cached credential together with the renewal policy ({@code clock} and {@code
   * renewalLeadTimeMillis}) used to decide when it should be renewed. The policy is captured from
   * the caller that created the entry; since both are derived from the same Hadoop configuration,
   * later readers observe the same renewal behavior.
   */
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
