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
import java.util.function.Supplier;
import org.apache.hadoop.conf.Configuration;
import org.sparkproject.guava.base.Preconditions;
import org.sparkproject.guava.cache.Cache;
import org.sparkproject.guava.cache.CacheBuilder;

public abstract class GenericCredentialProvider {
  // The remaining time before expiration, used to trigger renewal in advance.
  private static final long DEFAULT_RENEWAL_LEAD_TIME_MILLIS = 30 * 1000;

  // The credential cache, for saving QPS to unity catalog server.
  static final Cache<String, GenericCredential> globalCache;
  private static final String UC_CREDENTIAL_CACHE_MAX_SIZE =
      "unitycatalog.credential.cache.maxSize";
  private static final long UC_CREDENTIAL_CACHE_MAX_SIZE_DEFAULT = 1024;

  /**
   * Functional interface for API calls that may throw ApiException.
   */
  @FunctionalInterface
  private interface ApiCallSupplier {
    TemporaryCredentials get() throws ApiException;
  }

  static {
    long maxSize = Long.getLong(UC_CREDENTIAL_CACHE_MAX_SIZE, UC_CREDENTIAL_CACHE_MAX_SIZE_DEFAULT);
    globalCache = CacheBuilder.newBuilder().maximumSize(maxSize).build();
  }

  private final Clock clock;
  private final long renewalLeadTimeMillis;

  private Configuration conf;
  private URI ucUri;
  private String ucToken;
  private String credUid;
  private boolean credCacheEnabled;

  private volatile GenericCredential credential;
  private volatile TemporaryCredentialsApi tempCredApi;

  public GenericCredentialProvider() {
    this(Clock.systemClock(), DEFAULT_RENEWAL_LEAD_TIME_MILLIS);
  }

  GenericCredentialProvider(Clock clock, long renewalLeadTimeMillis) {
    this.clock = clock;
    this.renewalLeadTimeMillis = renewalLeadTimeMillis;
  }

  protected void initialize(Configuration conf) {
    this.conf = conf;

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

  private boolean isRecoverable(Throwable e) {
    if (e instanceof ApiException) {
      int code = ((ApiException) e).getCode();
      // Retry on rate limit (429), service unavailable (503), and 5xx server errors
      return code == 429 || code == 503 || (code >= 500 && code < 600);
    }
    // Network-level transient failures
    return e instanceof java.net.SocketTimeoutException  // Timeout
      || e instanceof java.net.SocketException           // Connection issues
      || e instanceof java.net.UnknownHostException;     // DNS resolution issues
  }

  private TemporaryCredentials callWithRetry(ApiCallSupplier apiCall) throws ApiException {
    int maxAttempts = conf.getInt(UCHadoopConf.RETRY_MAX_ATTEMPTS_KEY, UCHadoopConf.RETRY_MAX_ATTEMPTS_DEFAULT);
    long initialDelay = conf.getLong(UCHadoopConf.RETRY_INITIAL_DELAY_KEY, UCHadoopConf.RETRY_INITIAL_DELAY_DEFAULT);
    double multiplier = conf.getDouble(UCHadoopConf.RETRY_MULTIPLIER_KEY, UCHadoopConf.RETRY_MULTIPLIER_DEFAULT);

    Exception lastException = null;

    for (int attempt = 1; attempt <= maxAttempts; attempt++) {
      try {
        return apiCall.get();
      } catch (Exception e) {
        lastException = e;

        if (!isRecoverable(e) || attempt == maxAttempts) {
          break;
        }

        long baseDelay = (long) (initialDelay * Math.pow(multiplier, attempt - 1));
        double jitter = (Math.random() - 0.5) * 2 * UCHadoopConf.RETRY_JITTER_FACTOR;
        long delay = (long) (baseDelay * (1 + jitter));

        try {
          Thread.sleep(delay);
        } catch (InterruptedException interrupted) {
          Thread.currentThread().interrupt();
          throw new RuntimeException("Retry interrupted", interrupted);
        }
      }
    }

    if (lastException instanceof ApiException) {
      throw (ApiException) lastException;
    } else {
      throw new RuntimeException("Failed to obtain temporary credentials after " + maxAttempts + " attempts", lastException);
    }
  }

  private GenericCredential createGenericCredentials() throws ApiException {
    TemporaryCredentialsApi tempCredApi = temporaryCredentialsApi();

    // Generate the temporary credential via requesting UnityCatalog.
    TemporaryCredentials tempCred;
    String type = conf.get(UCHadoopConf.UC_CREDENTIALS_TYPE_KEY);
    if (UCHadoopConf.UC_CREDENTIALS_TYPE_PATH_VALUE.equals(type)) {
      String path = conf.get(UCHadoopConf.UC_PATH_KEY);
      String pathOperation = conf.get(UCHadoopConf.UC_PATH_OPERATION_KEY);

      tempCred = callWithRetry(() ->
        tempCredApi.generateTemporaryPathCredentials(
          new GenerateTemporaryPathCredential()
              .url(path)
              .operation(PathOperation.fromValue(pathOperation))
        )
      );
    } else if (UCHadoopConf.UC_CREDENTIALS_TYPE_TABLE_VALUE.equals(type)) {
      String tableId = conf.get(UCHadoopConf.UC_TABLE_ID_KEY);
      String tableOperation = conf.get(UCHadoopConf.UC_TABLE_OPERATION_KEY);

      tempCred = callWithRetry(() ->
        tempCredApi.generateTemporaryTableCredentials(
          new GenerateTemporaryTableCredential()
              .tableId(tableId)
              .operation(TableOperation.fromValue(tableOperation))
        )
      );
    } else {
      throw new IllegalArgumentException(String.format(
          "Unsupported unity catalog temporary credentials type '%s', please check '%s'",
          type, UCHadoopConf.UC_CREDENTIALS_TYPE_KEY));
    }

    return new GenericCredential(tempCred);
  }
}
