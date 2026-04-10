package io.unitycatalog.spark.auth.storage;

import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.TemporaryCredentialsApi;
import io.unitycatalog.client.auth.TokenProvider;
import io.unitycatalog.client.delta.model.CredentialOperation;
import io.unitycatalog.client.delta.model.CredentialsResponse;
import io.unitycatalog.client.delta.model.StorageCredential;
import io.unitycatalog.client.delta.model.StorageCredentialConfig;
import io.unitycatalog.client.internal.Clock;
import io.unitycatalog.client.model.AwsCredentials;
import io.unitycatalog.client.model.AzureUserDelegationSAS;
import io.unitycatalog.client.model.GcpOauthToken;
import io.unitycatalog.client.model.GenerateTemporaryPathCredential;
import io.unitycatalog.client.model.GenerateTemporaryTableCredential;
import io.unitycatalog.client.model.PathOperation;
import io.unitycatalog.client.model.TableOperation;
import io.unitycatalog.client.model.TemporaryCredentials;
import io.unitycatalog.client.retry.RetryPolicy;
import io.unitycatalog.spark.ApiClientFactory;
import io.unitycatalog.spark.UCHadoopConf;
import java.net.URI;
import java.util.List;
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
  private TokenProvider tokenProvider;
  private String credUid;
  private boolean credCacheEnabled;

  private boolean deltaRestApiEnabled;

  private volatile GenericCredential credential;
  private volatile TemporaryCredentialsApi tempCredApi;
  private volatile io.unitycatalog.client.delta.api.TemporaryCredentialsApi deltaTempCredApi;

  protected void initialize(Configuration conf) {
    this.conf = conf;

    // Use the test clock if one is intentionally configured for testing.
    String clockName = conf.get(UCHadoopConf.UC_TEST_CLOCK_NAME);
    this.clock = clockName != null ? Clock.getManualClock(clockName) : Clock.systemClock();

    this.renewalLeadTimeMillis =
        conf.getLong(
            UCHadoopConf.UC_RENEWAL_LEAD_TIME_KEY, UCHadoopConf.UC_RENEWAL_LEAD_TIME_DEFAULT_VALUE);

    String ucUriStr = conf.get(UCHadoopConf.UC_URI_KEY);
    Preconditions.checkNotNull(
        ucUriStr, "'%s' is not set in hadoop configuration", UCHadoopConf.UC_URI_KEY);
    this.ucUri = URI.create(ucUriStr);

    // Initialize the UCTokenProvider.
    this.tokenProvider = TokenProvider.create(conf.getPropsWithPrefix(UCHadoopConf.UC_AUTH_PREFIX));

    this.credUid = conf.get(UCHadoopConf.UC_CREDENTIALS_UID_KEY);
    Preconditions.checkState(
        credUid != null && !credUid.isEmpty(),
        "Credential UID cannot be null or empty, '%s' is not set in hadoop configuration",
        UCHadoopConf.UC_CREDENTIALS_UID_KEY);

    this.credCacheEnabled =
        conf.getBoolean(
            UCHadoopConf.UC_CREDENTIAL_CACHE_ENABLED_KEY,
            UCHadoopConf.UC_CREDENTIAL_CACHE_ENABLED_DEFAULT_VALUE);

    this.deltaRestApiEnabled =
        conf.getBoolean(
            UCHadoopConf.UC_DELTA_REST_API_ENABLED_KEY,
            UCHadoopConf.UC_DELTA_REST_API_ENABLED_DEFAULT_VALUE);

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
    // Retry is automatically handled by RetryingHttpClient when configured
    // via fs.unitycatalog.request.retry*
    if (tempCredApi == null) {
      synchronized (this) {
        if (tempCredApi == null) {
          RetryPolicy retryPolicy = UCHadoopConf.createRequestRetryPolicy(conf);
          tempCredApi =
              new TemporaryCredentialsApi(
                  ApiClientFactory.createApiClient(retryPolicy, ucUri, tokenProvider));
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
    if (deltaRestApiEnabled) {
      return createGenericCredentialsDeltaRest();
    }
    return createGenericCredentialsLegacy();
  }

  private GenericCredential createGenericCredentialsLegacy() throws ApiException {
    TemporaryCredentialsApi tempCredApi = temporaryCredentialsApi();

    // Generate the temporary credential via requesting UnityCatalog.
    TemporaryCredentials tempCred;
    String type = conf.get(UCHadoopConf.UC_CREDENTIALS_TYPE_KEY);
    if (UCHadoopConf.UC_CREDENTIALS_TYPE_PATH_VALUE.equals(type)) {
      String path = conf.get(UCHadoopConf.UC_PATH_KEY);
      String pathOperation = conf.get(UCHadoopConf.UC_PATH_OPERATION_KEY);

      tempCred =
          tempCredApi.generateTemporaryPathCredentials(
              new GenerateTemporaryPathCredential()
                  .url(path)
                  .operation(PathOperation.fromValue(pathOperation)));
    } else if (UCHadoopConf.UC_CREDENTIALS_TYPE_TABLE_VALUE.equals(type)) {
      String tableId = conf.get(UCHadoopConf.UC_TABLE_ID_KEY);
      String tableOperation = conf.get(UCHadoopConf.UC_TABLE_OPERATION_KEY);

      tempCred =
          tempCredApi.generateTemporaryTableCredentials(
              new GenerateTemporaryTableCredential()
                  .tableId(tableId)
                  .operation(TableOperation.fromValue(tableOperation)));
    } else {
      throw new IllegalArgumentException(
          String.format(
              "Unsupported unity catalog temporary credentials type '%s', please check '%s'",
              type, UCHadoopConf.UC_CREDENTIALS_TYPE_KEY));
    }

    return new GenericCredential(tempCred);
  }

  /** Renews credentials using the Delta REST Catalog API endpoints. */
  private GenericCredential createGenericCredentialsDeltaRest() throws ApiException {
    io.unitycatalog.client.delta.api.TemporaryCredentialsApi deltaApi =
        deltaTemporaryCredentialsApi();

    String type = conf.get(UCHadoopConf.UC_CREDENTIALS_TYPE_KEY);
    CredentialsResponse credResp;

    if (UCHadoopConf.UC_CREDENTIALS_TYPE_PATH_VALUE.equals(type)) {
      String path = conf.get(UCHadoopConf.UC_PATH_KEY);
      String pathOp = conf.get(UCHadoopConf.UC_PATH_OPERATION_KEY);
      CredentialOperation credOp =
          "READ_WRITE".equals(pathOp) ? CredentialOperation.READ_WRITE : CredentialOperation.READ;
      credResp = deltaApi.getTemporaryPathCredentials(path, credOp);
    } else if (UCHadoopConf.UC_CREDENTIALS_TYPE_TABLE_VALUE.equals(type)) {
      String catalog = conf.get(UCHadoopConf.UC_TABLE_CATALOG_KEY);
      String schema = conf.get(UCHadoopConf.UC_TABLE_SCHEMA_KEY);
      String tableName = conf.get(UCHadoopConf.UC_TABLE_NAME_KEY);
      String tableOp = conf.get(UCHadoopConf.UC_TABLE_OPERATION_KEY);
      CredentialOperation credOp =
          "READ_WRITE".equals(tableOp) ? CredentialOperation.READ_WRITE : CredentialOperation.READ;
      credResp = deltaApi.getTableCredentials(credOp, catalog, schema, tableName);
    } else {
      throw new IllegalArgumentException(
          String.format(
              "Unsupported unity catalog temporary credentials type '%s', please check '%s'",
              type, UCHadoopConf.UC_CREDENTIALS_TYPE_KEY));
    }

    // Convert the delta REST credential response to a legacy TemporaryCredentials
    List<StorageCredential> creds = credResp.getStorageCredentials();
    if (creds == null || creds.isEmpty()) {
      throw new ApiException("No storage credentials returned from delta REST API");
    }
    StorageCredential storageCred = creds.get(0);
    TemporaryCredentials tempCreds = convertStorageCredential(storageCred);
    return new GenericCredential(tempCreds);
  }

  private io.unitycatalog.client.delta.api.TemporaryCredentialsApi deltaTemporaryCredentialsApi() {
    if (deltaTempCredApi == null) {
      synchronized (this) {
        if (deltaTempCredApi == null) {
          RetryPolicy retryPolicy = UCHadoopConf.createRequestRetryPolicy(conf);
          ApiClient deltaApiClient =
              ApiClientFactory.createDeltaApiClient(retryPolicy, ucUri, tokenProvider);
          deltaTempCredApi =
              new io.unitycatalog.client.delta.api.TemporaryCredentialsApi(deltaApiClient);
        }
      }
    }
    return deltaTempCredApi;
  }

  /** Converts a delta REST API StorageCredential to the legacy TemporaryCredentials model. */
  private static TemporaryCredentials convertStorageCredential(StorageCredential storageCred) {
    StorageCredentialConfig config = storageCred.getConfig();
    TemporaryCredentials tempCreds = new TemporaryCredentials();

    if (storageCred.getExpirationTimeMs() != null) {
      tempCreds.setExpirationTime(storageCred.getExpirationTimeMs());
    }

    if (config.getS3AccessKeyId() != null) {
      AwsCredentials awsCred = new AwsCredentials();
      awsCred.setAccessKeyId(config.getS3AccessKeyId());
      awsCred.setSecretAccessKey(config.getS3SecretAccessKey());
      awsCred.setSessionToken(config.getS3SessionToken());
      tempCreds.setAwsTempCredentials(awsCred);
    } else if (config.getGcsOauthToken() != null) {
      GcpOauthToken gcpToken = new GcpOauthToken();
      gcpToken.setOauthToken(config.getGcsOauthToken());
      tempCreds.setGcpOauthToken(gcpToken);
    } else if (config.getAzureSasToken() != null) {
      AzureUserDelegationSAS azureSas = new AzureUserDelegationSAS();
      azureSas.setSasToken(config.getAzureSasToken());
      tempCreds.setAzureUserDelegationSas(azureSas);
    }

    return tempCreds;
  }
}
