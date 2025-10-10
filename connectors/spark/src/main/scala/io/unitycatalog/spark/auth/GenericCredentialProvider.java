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
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.shaded.com.google.common.base.Preconditions;
import org.sparkproject.guava.cache.Cache;
import org.sparkproject.guava.cache.CacheBuilder;

public abstract class GenericCredentialProvider {
  // The remaining time before expiration, used to trigger renewal in advance.
  private static final long DEFAULT_RENEWAL_LEAD_TIME_MILLIS = 30 * 1000;

  // The credential cache, for saving QPS to unity catalog server.
  static final Cache<String, GenericCredential> CACHE = CacheBuilder
      .newBuilder()
      .maximumSize(1000)
      .build();

  private final Configuration conf;
  private final URI ucUri;
  private final String ucToken;
  private final String credUid;
  private final boolean credCacheEnabled;

  private volatile long renewalLeadTimeMillis = DEFAULT_RENEWAL_LEAD_TIME_MILLIS;
  private volatile GenericCredential credential;
  private volatile TemporaryCredentialsApi tempCredApi;

  public GenericCredentialProvider(Configuration conf) {
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
    if (credential == null || credential.readyToRenew(renewalLeadTimeMillis)) {
      synchronized (this) {
        if (credential == null || credential.readyToRenew(renewalLeadTimeMillis)) {
          credential = renewCredential();
        }
      }
    }

    return credential;
  }

  // For testing purpose only.
  void setRenewalLeadTimeMillis(long renewalLeadTimeMillis) {
    this.renewalLeadTimeMillis = renewalLeadTimeMillis;
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

  private GenericCredential renewCredential() {
    if (credCacheEnabled) {
      synchronized (CACHE) {
        GenericCredential cached = CACHE.getIfPresent(credUid);
        // Use the cached one if existing and valid.
        if (cached != null && !cached.readyToRenew(renewalLeadTimeMillis)) {
          return cached;
        }
        // Renew the credential and update the cache.
        GenericCredential renewed = createGenericCredentials();
        CACHE.put(credUid, renewed);
        return renewed;
      }
    } else {
      return createGenericCredentials();
    }
  }

  private GenericCredential createGenericCredentials() {
    TemporaryCredentialsApi tempCredApi = temporaryCredentialsApi();

    // Generate the temporary credential via requesting UnityCatalog.
    try {
      String type = conf.get(UCHadoopConf.UC_CREDENTIALS_TYPE_KEY);
      if (UCHadoopConf.UC_CREDENTIALS_TYPE_PATH_VALUE.equals(type)) {
        String path = conf.get(UCHadoopConf.UC_PATH_KEY);
        Preconditions.checkNotNull(path,
            "'%s' is not set in hadoop configuration", UCHadoopConf.UC_PATH_KEY);

        String pathOpStr = conf.get(UCHadoopConf.UC_PATH_OPERATION_KEY);
        Preconditions.checkNotNull(pathOpStr,
            "'%s' is not set in hadoop configuration", UCHadoopConf.UC_PATH_OPERATION_KEY);
        PathOperation pathOp = PathOperation.valueOf(pathOpStr);

        TemporaryCredentials tempCred = tempCredApi.generateTemporaryPathCredentials(
            new GenerateTemporaryPathCredential()
                .url(path)
                .operation(pathOp)
        );
        return new GenericCredential(tempCred);
      } else if (UCHadoopConf.UC_CREDENTIALS_TYPE_TABLE_VALUE.equals(type)) {
        String tableId = conf.get(UCHadoopConf.UC_TABLE_ID_KEY);
        Preconditions.checkNotNull(tableId,
            "'%s' is not set in hadoop configuration", UCHadoopConf.UC_TABLE_ID_KEY);

        String tableOpStr = conf.get(UCHadoopConf.UC_TABLE_OPERATION_KEY);
        Preconditions.checkNotNull(tableOpStr,
            "'%s' is not set in hadoop configuration", UCHadoopConf.UC_TABLE_OPERATION_KEY);
        TableOperation tableOp = TableOperation.valueOf(tableOpStr);

        TemporaryCredentials tempCred = tempCredApi.generateTemporaryTableCredentials(
            new GenerateTemporaryTableCredential()
                .tableId(tableId)
                .operation(tableOp)
        );
        return new GenericCredential(tempCred);
      } else {
        throw new IllegalArgumentException(String.format(
            "Unsupported unity catalog temporary credentials type '%s', please check '%s'",
            type, UCHadoopConf.UC_CREDENTIALS_TYPE_KEY));
      }
    } catch (ApiException e) {
      throw new RuntimeException(e);
    }
  }
}
