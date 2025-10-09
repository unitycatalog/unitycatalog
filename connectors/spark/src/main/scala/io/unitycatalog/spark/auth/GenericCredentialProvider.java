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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.shaded.com.google.common.base.Preconditions;

public abstract class GenericCredentialProvider {
  // The remaining time before expiration, used to trigger renewal in advance.
  private static final long DEFAULT_RENEWAL_LEAD_TIME_MILLIS = 30 * 1000;

  // The credential cache, for saving QPS to unity catalog server.
  static final Map<GenericCredKey, GenericCredential> CACHE = new ConcurrentHashMap<>();

  private final Configuration conf;
  private final URI ucUri;
  private final String ucToken;

  private final boolean credCacheEnabled;
  private final GenericCredKey genericCredKey;

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

    this.credCacheEnabled = conf.getBoolean(
        UCHadoopConf.UC_CREDENTIAL_CACHE_ENABLED_KEY,
        UCHadoopConf.UC_CREDENTIAL_CACHE_ENABLED_DEFAULT_VALUE);

    // Init the generic credential key for caching.
    this.genericCredKey = createGenericCredKey(conf);

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

  private GenericCredKey createGenericCredKey(Configuration conf) {
    String type = conf.get(UCHadoopConf.UC_CREDENTIALS_TYPE_KEY);
    if (UCHadoopConf.UC_CREDENTIALS_TYPE_PATH_VALUE.equals(type)) {
      String path = conf.get(UCHadoopConf.UC_PATH_KEY);
      Preconditions.checkNotNull(path,
          "'%s' is not set in hadoop configuration", UCHadoopConf.UC_PATH_KEY);

      String pathOpStr = conf.get(UCHadoopConf.UC_PATH_OPERATION_KEY);
      Preconditions.checkNotNull(pathOpStr,
          "'%s' is not set in hadoop configuration", UCHadoopConf.UC_PATH_OPERATION_KEY);
      PathOperation pathOp = PathOperation.valueOf(pathOpStr);

      return new GenericCredKey.PathCredKey(ucUri.toString(), ucToken, path, pathOp);
    } else if (UCHadoopConf.UC_CREDENTIALS_TYPE_TABLE_VALUE.equals(type)) {
      String tableId = conf.get(UCHadoopConf.UC_TABLE_ID_KEY);
      Preconditions.checkNotNull(tableId,
          "'%s' is not set in hadoop configuration", UCHadoopConf.UC_TABLE_ID_KEY);

      String tableOpStr = conf.get(UCHadoopConf.UC_TABLE_OPERATION_KEY);
      Preconditions.checkNotNull(tableOpStr,
          "'%s' is not set in hadoop configuration", UCHadoopConf.UC_TABLE_OPERATION_KEY);
      TableOperation tableOp = TableOperation.valueOf(tableOpStr);

      return new GenericCredKey.TableCredKey(ucUri.toString(), ucToken, tableId, tableOp);
    } else {
      throw new IllegalArgumentException(String.format(
          "Unsupported unity catalog temporary credentials type '%s', please check '%s'",
          type, UCHadoopConf.UC_CREDENTIALS_TYPE_KEY));
    }
  }

  private GenericCredential renewCredential() {
    if (credCacheEnabled) {
      return CACHE.compute(genericCredKey, (k, cached) -> {
        if (cached == null || cached.readyToRenew(renewalLeadTimeMillis)) {
          return createGenericCredentials();
        } else {
          return cached;
        }
      });
    } else {
      return createGenericCredentials();
    }
  }

  private GenericCredential createGenericCredentials() {
    TemporaryCredentialsApi tempCredApi = temporaryCredentialsApi();

    // Generate the temporary credential via requesting UnityCatalog.
    try {
      // TODO We will need to retry the temporary credential request if any recoverable failure, for
      // more robustness.
      TemporaryCredentials tempCred;
      if (genericCredKey instanceof GenericCredKey.PathCredKey) {
        GenericCredKey.PathCredKey key = (GenericCredKey.PathCredKey) genericCredKey;
        tempCred = tempCredApi.generateTemporaryPathCredentials(
            new GenerateTemporaryPathCredential()
                .url(key.path())
                .operation(key.pathOp())
        );
      } else if (genericCredKey instanceof GenericCredKey.TableCredKey) {
        GenericCredKey.TableCredKey key = (GenericCredKey.TableCredKey) genericCredKey;
        tempCred = tempCredApi.generateTemporaryTableCredentials(
            new GenerateTemporaryTableCredential()
                .tableId(key.tableId())
                .operation(key.tableOp())
        );
      } else {
        throw new IllegalArgumentException(String.format(
            "Unsupported unity catalog temporary credentials type '%s', please check '%s'",
            conf.get(UCHadoopConf.UC_CREDENTIALS_TYPE_KEY), UCHadoopConf.UC_CREDENTIALS_TYPE_KEY));
      }

      return new GenericCredential(tempCred);
    } catch (ApiException e) {
      throw new RuntimeException(e);
    }
  }
}
