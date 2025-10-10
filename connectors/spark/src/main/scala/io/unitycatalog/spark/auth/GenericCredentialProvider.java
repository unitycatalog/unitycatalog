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

public abstract class GenericCredentialProvider {
  // The time remaining until expiration, we will try to renew the credential before the expiration
  // time.
  private static final long DEFAULT_RENEWAL_LEAD_TIME_MILLIS = 30 * 1000;

  private final Configuration conf;
  private final URI ucUri;
  private final String ucToken;

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

    // The initialized credentials passing-through the hadoop configuration.
    this.credential = initGenericCredential(conf);
  }

  public abstract GenericCredential initGenericCredential(Configuration conf);

  public GenericCredential accessCredentials() {
    if (credential == null || credential.readyToRenew(renewalLeadTimeMillis)) {
      synchronized (this) {
        if (credential == null || credential.readyToRenew(renewalLeadTimeMillis)) {
          try {
            credential = createGenericCredentials();
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
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
