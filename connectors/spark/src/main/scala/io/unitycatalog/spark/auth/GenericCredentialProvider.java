package io.unitycatalog.spark.auth;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.TemporaryCredentialsApi;
import io.unitycatalog.client.model.GenerateTemporaryPathCredential;
import io.unitycatalog.client.model.GenerateTemporaryTableCredential;
import io.unitycatalog.client.model.PathOperation;
import io.unitycatalog.client.model.TableOperation;
import io.unitycatalog.client.model.TemporaryCredentials;
import io.unitycatalog.spark.ApiClientFactory;
import io.unitycatalog.spark.UCHadoopConf;
import io.unitycatalog.spark.utils.Clock;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.sparkproject.guava.base.Preconditions;
import org.sparkproject.guava.cache.Cache;
import org.sparkproject.guava.cache.CacheBuilder;

public abstract class GenericCredentialProvider {
  // The credential cache, for saving QPS to unity catalog server.
  static final Cache<String, GenericCredential> globalCache;
  private static final String UC_CREDENTIAL_CACHE_MAX_SIZE =
      "unitycatalog.credential.cache.maxSize";
  private static final long UC_CREDENTIAL_CACHE_MAX_SIZE_DEFAULT = 1024;

  static {
    long maxSize = Long.getLong(UC_CREDENTIAL_CACHE_MAX_SIZE, UC_CREDENTIAL_CACHE_MAX_SIZE_DEFAULT);
    globalCache = CacheBuilder.newBuilder().maximumSize(maxSize).build();
  }

  private Configuration conf;
  private Clock clock;
  private long renewalLeadTimeMillis;
  private URI ucUri;
  private String ucToken;
  private String credUid;
  private boolean credCacheEnabled;

  private volatile GenericCredential credential;
  private volatile TemporaryCredentialsApi tempCredApi;

  protected void initialize(Configuration conf) {
    this.conf = conf;

    // Use the test clock if one is intentionally configured for testing.
    String clockName = conf.get(UCHadoopConf.UC_TEST_CLOCK_NAME);
    this.clock = clockName != null ? Clock.getManualClock(clockName) : Clock.systemClock();

    this.renewalLeadTimeMillis = conf.getLong(
        UCHadoopConf.UC_RENEWAL_LEAD_TIME_KEY,
        UCHadoopConf.UC_RENEWAL_LEAD_TIME_DEFAULT_VALUE);

    String ucUriStr = conf.get(UCHadoopConf.UC_URI_KEY);
    Preconditions.checkNotNull(ucUriStr,
        "'%s' is not set in hadoop configuration", UCHadoopConf.UC_URI_KEY);
    this.ucUri = URI.create(ucUriStr);

    String ucTokenStr = conf.get(UCHadoopConf.UC_TOKEN_KEY);
    Preconditions.checkNotNull(ucTokenStr,
        "'%s' is not set in hadoop configuration", UCHadoopConf.UC_TOKEN_KEY);
    this.ucToken = conf.get(UCHadoopConf.UC_TOKEN_KEY);

    this.credUid = conf.get(UCHadoopConf.UC_CREDENTIALS_UID_KEY);
    Preconditions.checkState(credUid != null && !credUid.isEmpty(),
        "Credential UID cannot be null or empty, '%s' is not set in hadoop configuration",
        UCHadoopConf.UC_CREDENTIALS_UID_KEY);

    this.credCacheEnabled = conf.getBoolean(
        UCHadoopConf.UC_CREDENTIAL_CACHE_ENABLED_KEY,
        UCHadoopConf.UC_CREDENTIAL_CACHE_ENABLED_DEFAULT_VALUE);

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

  protected TemporaryCredentialsApi temporaryCredentialsApi() {
    if (tempCredApi == null) {
      synchronized (this) {
        if (tempCredApi == null) {
          tempCredApi = new TemporaryCredentialsApi(
              ApiClientFactory.createApiClient(ucUri, ucToken));
        }
      }
    }

    return tempCredApi;
  }

  private GenericCredential renewCredential() throws ApiException {
    if (credCacheEnabled) {
      synchronized (globalCache) {
        GenericCredential cached = globalCache.getIfPresent(credUid);
        // Use the cached one if existing and valid.
        if (cached != null && !cached.readyToRenew(clock, renewalLeadTimeMillis)) {
          return cached;
        }
        // Renew the credential and update the cache.
        GenericCredential renewed = createGenericCredentials();
        globalCache.put(credUid, renewed);
        return renewed;
      }
    } else {
      return createGenericCredentials();
    }
  }

  private GenericCredential createGenericCredentials() throws ApiException {
    TemporaryCredentialsApi tempCredApi = temporaryCredentialsApi();

    // Generate the temporary credential via requesting UnityCatalog.
    TemporaryCredentials tempCred;
    String type = conf.get(UCHadoopConf.UC_CREDENTIALS_TYPE_KEY);
    // TODO We will need to retry the temporary credential request if any recoverable failure, for
    // more robustness.
    if (UCHadoopConf.UC_CREDENTIALS_TYPE_PATH_VALUE.equals(type)) {
      String path = conf.get(UCHadoopConf.UC_PATH_KEY);
      String pathOperation = conf.get(UCHadoopConf.UC_PATH_OPERATION_KEY);

      tempCred = tempCredApi.generateTemporaryPathCredentials(
          new GenerateTemporaryPathCredential()
              .url(path)
              .operation(PathOperation.fromValue(pathOperation))
      );
    } else if (UCHadoopConf.UC_CREDENTIALS_TYPE_TABLE_VALUE.equals(type)) {
      String tableId = conf.get(UCHadoopConf.UC_TABLE_ID_KEY);
      String tableOperation = conf.get(UCHadoopConf.UC_TABLE_OPERATION_KEY);

      tempCred = tempCredApi.generateTemporaryTableCredentials(
          new GenerateTemporaryTableCredential()
              .tableId(tableId)
              .operation(TableOperation.fromValue(tableOperation))
      );
    } else {
      throw new IllegalArgumentException(String.format(
          "Unsupported unity catalog temporary credentials type '%s', please check '%s'",
          type, UCHadoopConf.UC_CREDENTIALS_TYPE_KEY));
    }

    return new GenericCredential(tempCred);
  }
}
