package io.unitycatalog.hadoop.internal.auth;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.internal.Clock;
import io.unitycatalog.hadoop.internal.UCHadoopConfConstants;
import io.unitycatalog.hadoop.internal.util.BoundedKeyedCache;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Preconditions;

/**
 * Base class for Hadoop credential providers backed by Unity Catalog temporary credentials.
 *
 * <p>Subclasses expose cloud-specific provider interfaces while this class handles renewal and
 * cache lookup.
 */
public abstract class GenericCredentialProvider {
  // The credential cache, for saving QPS to unity catalog server.
  static final BoundedKeyedCache<String, GenericCredential> globalCache;
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
  private String credUid;
  private boolean credCacheEnabled;

  private volatile GenericCredential credential;
  private volatile TempCredentialApi credentialApi;

  protected void initialize(Configuration conf) {
    this.conf = conf;

    // Use the test clock if one is intentionally configured for testing.
    String clockName = conf.get(UCHadoopConfConstants.UC_TEST_CLOCK_NAME);
    this.clock = clockName != null ? Clock.getManualClock(clockName) : Clock.systemClock();

    this.renewalLeadTimeMillis =
        conf.getLong(
            UCHadoopConfConstants.UC_RENEWAL_LEAD_TIME_KEY,
            UCHadoopConfConstants.UC_RENEWAL_LEAD_TIME_DEFAULT_VALUE);

    this.credUid = conf.get(UCHadoopConfConstants.UC_CREDENTIALS_UID_KEY);
    Preconditions.checkState(
        credUid != null && !credUid.isEmpty(),
        "Credential UID cannot be null or empty, '%s' is not set in hadoop configuration",
        UCHadoopConfConstants.UC_CREDENTIALS_UID_KEY);

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

  TempCredentialApi tempCredentialApi() {
    if (credentialApi == null) {
      synchronized (this) {
        if (credentialApi == null) {
          credentialApi = TempCredentialApi.create(conf);
        }
      }
    }
    return credentialApi;
  }

  private GenericCredential renewCredential() throws ApiException {
    if (credCacheEnabled) {
      synchronized (globalCache) {
        GenericCredential cached = globalCache.getIfPresent(credUid);
        // Use the cached one if existing and valid.
        if (cached != null && !cached.readyToRenew(clock, renewalLeadTimeMillis)) {
          return cached;
        }
        GenericCredential created = tempCredentialApi().createCredential();
        globalCache.put(credUid, created);
        return created;
      }
    } else {
      return tempCredentialApi().createCredential();
    }
  }
}
