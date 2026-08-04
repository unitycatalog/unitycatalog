package io.unitycatalog.hadoop.internal.auth;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.internal.Clock;
import io.unitycatalog.client.internal.Preconditions;
import io.unitycatalog.hadoop.internal.CredentialUtil;
import io.unitycatalog.hadoop.internal.UCHadoopConfConstants;
import io.unitycatalog.hadoop.internal.auth.CredentialCache.RenewableCredential;
import io.unitycatalog.hadoop.internal.id.FileSystemCredId;
import io.unitycatalog.hadoop.internal.util.ClockUtil;
import java.util.List;
import org.apache.hadoop.conf.Configuration;

/**
 * Base class for Hadoop credential providers backed by Unity Catalog temporary credentials.
 *
 * <p>Subclasses expose cloud-specific provider interfaces while this class handles renewal and
 * cache lookup.
 */
public abstract class GenericCredentialProvider {
  static final CredentialCache<FileSystemCredId, GenericCredential> globalCache =
      CredentialCache.createGlobalCache();

  private Configuration conf;
  private Clock clock;
  private long renewalLeadTimeMillis;
  private FileSystemCredId cacheKey;
  private boolean credCacheEnabled;

  private volatile GenericCredential credential;
  private volatile GenericCredentialFetcher credentialFetcher;

  protected void initialize(Configuration conf) {
    this.conf = conf;
    this.clock = ClockUtil.resolveClock(conf);

    this.renewalLeadTimeMillis =
        conf.getLong(
            UCHadoopConfConstants.UC_RENEWAL_LEAD_TIME_KEY,
            UCHadoopConfConstants.UC_RENEWAL_LEAD_TIME_DEFAULT_VALUE);

    // Identify the credential scope and prefix so matching requests can share a credential.
    this.cacheKey = FileSystemCredId.create(conf);

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
      return globalCache.access(
          cacheKey,
          () -> {
            GenericCredential credential = fetchAndSelectCredential();
            return new RenewableCredential<>(credential) {
              @Override
              public boolean readyToRenew() {
                return credential.readyToRenew(clock, renewalLeadTimeMillis);
              }
            };
          });
    } else {
      return fetchAndSelectCredential();
    }
  }

  private GenericCredential fetchAndSelectCredential() throws ApiException {
    List<GenericCredential> credentials = genericCredentialFetcher().createCredentials();
    Preconditions.checkState(!credentials.isEmpty(), "No vended credential was returned.");

    // If there is only one credential, no selection is needed.
    if (credentials.size() == 1) {
      return credentials.get(0);
    }

    // For multiple credentials, UC_CREDENTIAL_PREFIX_KEY identifies which credential covers the
    // requested storage location. Without it, the provider cannot select from the vended list.
    Preconditions.checkState(
        cacheKey.prefix() != null,
        "Multiple credentials were vended but no location is set to select one.");
    return CredentialUtil.selectForLocation(cacheKey.prefix(), credentials);
  }
}
