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

public abstract class GeneralCredentialProvider {
  // The time remaining until expiration, we will try to renew the credential before the expiration
  // time.
  private static final long DEFAULT_RENEWAL_LEAD_TIME_MILLIS = 30 * 1000;

  private final Configuration conf;
  private final URI ucUri;
  private final String ucToken;

  private volatile long renewalLeadTime = DEFAULT_RENEWAL_LEAD_TIME_MILLIS;
  private volatile GeneralCredential credential;
  private volatile TemporaryCredentialsApi tempCredApi;

  public GeneralCredentialProvider(URI ignored, Configuration conf) {
    this.conf = conf;

    String ucUriStr = conf.get(UCHadoopConf.UC_URI);
    Preconditions.checkNotNull(ucUriStr,
        "'%s' is not set in hadoop configuration", UCHadoopConf.UC_URI);
    this.ucUri = URI.create(ucUriStr);

    String ucTokenStr = conf.get(UCHadoopConf.UC_TOKEN);
    Preconditions.checkNotNull(ucTokenStr,
        "'%s' is not set in hadoop configuration", UCHadoopConf.UC_TOKEN);
    this.ucToken = conf.get(UCHadoopConf.UC_TOKEN);

    // The initialized credentials passing-through the hadoop configuration.
    this.credential = initGeneralCredential(conf);
  }

  public abstract GeneralCredential initGeneralCredential(Configuration conf);

  public GeneralCredential accessCredentials() {
    if (credential == null || credential.readyToRenew(renewalLeadTime)) {
      synchronized (this) {
        if (credential == null || credential.readyToRenew(renewalLeadTime)) {
          try {
            credential = createGeneralCredentials();
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }
      }
    }

    return credential;
  }

  // For testing purpose only.
  void setRenewalLeadTime(long renewalLeadTime) {
    this.renewalLeadTime = renewalLeadTime;
  }

  protected TemporaryCredentialsApi temporaryCredentialsApi() {
    if (tempCredApi == null) {
      synchronized (this) {
        if (tempCredApi == null) {
          tempCredApi = new TemporaryCredentialsApi(ApiClientFactory.createApiClient(ucUri, ucToken));
        }
      }
    }

    return tempCredApi;
  }

  private GeneralCredential createGeneralCredentials() throws ApiException {
    TemporaryCredentialsApi tempCredApi = temporaryCredentialsApi();

    // Generate the temporary credential via requesting UnityCatalog.
    TemporaryCredentials tempCred;
    String type = conf.get(UCHadoopConf.UC_CREDENTIALS_TYPE);
    // TODO We will need to retry the temporary credential request if any recoverable failure, for
    // better robust.
    if (UCHadoopConf.UC_CREDENTIALS_TYPE_PATH_VALUE.equals(type)) {
      String path = conf.get(UCHadoopConf.UC_PATH);
      String pathOperation = conf.get(UCHadoopConf.UC_PATH_OPERATION);

      tempCred = tempCredApi.generateTemporaryPathCredentials(
          new GenerateTemporaryPathCredential()
              .url(path)
              .operation(PathOperation.fromValue(pathOperation))
      );
    } else if (UCHadoopConf.UC_CREDENTIALS_TYPE_TABLE_VALUE.equals(type)) {
      String tableId = conf.get(UCHadoopConf.UC_TABLE_ID);
      String tableOperation = conf.get(UCHadoopConf.UC_TABLE_OPERATION);

      tempCred = tempCredApi.generateTemporaryTableCredentials(
          new GenerateTemporaryTableCredential()
              .tableId(tableId)
              .operation(TableOperation.fromValue(tableOperation))
      );
    } else {
      throw new IllegalArgumentException(String.format(
          "Unsupported unity catalog temporary credentials type '%s', please check '%s'",
          type, UCHadoopConf.UC_CREDENTIALS_TYPE));
    }

    return new GeneralCredential(tempCred);
  }
}
