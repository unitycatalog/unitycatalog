package io.unitycatalog.hadoop.internal.auth;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.internal.Clock;
import io.unitycatalog.hadoop.internal.UCHadoopConfConstants;
import io.unitycatalog.hadoop.internal.id.CredId;
import io.unitycatalog.hadoop.internal.util.BoundedKeyedCache;
import org.apache.hadoop.conf.Configuration;

/**
 * Base class for Hadoop credential providers backed by Unity Catalog temporary credentials.
 *
 * <p>Subclasses expose cloud-specific provider interfaces while this class handles renewal and
 * cache lookup.
 */
public abstract class GenericCredentialProvider {
  // The credential cache, for saving QPS to unity catalog server. Keyed by the credential scope
  // ({@link CredId}) so that requests targeting the same scope can share a vended credential.
  static final BoundedKeyedCache<CredId, GenericCredential> globalCache;
  private static final String UC_CREDENTIAL_CACHE_MAX_SIZE =
      "unitycatalog.credential.cache.maxSize";
  private static final int UC_CREDENTIAL_CACHE_MAX_SIZE_DEFAULT = 1024;

  static {
    int maxSize =
        Integer.getInteger(UC_CREDENTIAL_CACHE_MAX_SIZE, UC_CREDENTIAL_CACHE_MAX_SIZE_DEFAULT);
    globalCache = new BoundedKeyedCache<>(maxSize, ignored -> {});
  }

  private Configuration conf;
  private Clock clock;
  private long renewalLeadTimeMillis;
  private CredId cacheKey;
  private boolean credCacheEnabled;

  private volatile GenericCredential credential;
  private volatile GenericCredentialFetcher credentialFetcher;

  protected void initialize(Configuration conf) {
    this.conf = conf;

    // Use the test clock if one is intentionally configured for testing.
    String clockName = conf.get(UCHadoopConfConstants.UC_TEST_CLOCK_NAME);
    this.clock = clockName != null ? Clock.getManualClock(clockName) : Clock.systemClock();

    this.renewalLeadTimeMillis =
        conf.getLong(
            UCHadoopConfConstants.UC_RENEWAL_LEAD_TIME_KEY,
            UCHadoopConfConstants.UC_RENEWAL_LEAD_TIME_DEFAULT_VALUE);

    // Identify the credential scope; used as the global cache key so that requests targeting the
    // same scope can share a vended credential.
    this.cacheKey = CredId.create(conf);

    this.credCacheEnabled =
        conf.getBoolean(
            UCHadoopConfConstants.UC_CREDENTIAL_CACHE_ENABLED_KEY,
            UCHadoopConfConstants.UC_CREDENTIAL_CACHE_ENABLED_DEFAULT_VALUE);

    // The initialized credentials passing-through the hadoop configuration.
    this.credential = initGenericCredential(conf);
  }

  public abstract GenericCredential initGenericCredential(Configuration conf);

  public GenericCredential accessCredentials() {
    if (credential == null || credential.readyToRenew(clock, renewalLeadTimeMillis)) {
      synchronized (this) {
        if (credential == null || credential.readyToRenew(clock, renewalLeadTimeMillis)) {
          try {
            credential = renewCredential();
          } catch (ApiException e) {
            throw new RuntimeException(e);
          }
        }
      }
    }

    return credential;
  }

  GenericCredentialFetcher genericCredentialFetcher() {
    if (credentialFetcher == null) {
      synchronized (this) {
        if (credentialFetcher == null) {
          credentialFetcher = GenericCredentialFetcher.create(conf);
        }
      }
    }
    return credentialFetcher;
  }

  private GenericCredential renewCredential() throws ApiException {
    if (credCacheEnabled) {
      synchronized (globalCache) {
        GenericCredential cached = globalCache.getIfPresent(cacheKey);
        // Use the cached one if existing and valid.
        if (cached != null && !cached.readyToRenew(clock, renewalLeadTimeMillis)) {
          return cached;
        }
        GenericCredential created = genericCredentialFetcher().createCredential();
        globalCache.put(cacheKey, created);
        return created;
      }
    } else {
      return genericCredentialFetcher().createCredential();
    }
  }
}
