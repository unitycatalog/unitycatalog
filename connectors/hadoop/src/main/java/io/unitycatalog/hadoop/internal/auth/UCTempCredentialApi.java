package io.unitycatalog.hadoop.internal.auth;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.TemporaryCredentialsApi;
import io.unitycatalog.client.internal.Preconditions;
import io.unitycatalog.client.model.GenerateTemporaryPathCredential;
import io.unitycatalog.client.model.GenerateTemporaryTableCredential;
import io.unitycatalog.client.model.PathOperation;
import io.unitycatalog.client.model.TableOperation;
import io.unitycatalog.client.model.TemporaryCredentials;
import io.unitycatalog.hadoop.internal.UCHadoopConfConstants;
import org.apache.hadoop.conf.Configuration;

/** Adapts the standard Unity Catalog temporary credentials SDK API for Hadoop token providers. */
final class UCTempCredentialApi implements TempCredentialApi {
  private final TemporaryCredentialsApi api;
  private final GenerateTemporaryPathCredential pathRequest;
  private final GenerateTemporaryTableCredential tableRequest;

  UCTempCredentialApi(Configuration conf, TemporaryCredentialsApi api) {
    Preconditions.checkNotNull(api, "api is required");
    this.api = api;
    String type = conf.get(UCHadoopConfConstants.UC_CREDENTIALS_TYPE_KEY);
    Preconditions.checkArgument(
        UCHadoopConfConstants.UC_CREDENTIALS_TYPE_PATH_VALUE.equals(type)
            || UCHadoopConfConstants.UC_CREDENTIALS_TYPE_TABLE_VALUE.equals(type),
        "Unsupported unity catalog temporary credentials type '%s', please check '%s'",
        type,
        UCHadoopConfConstants.UC_CREDENTIALS_TYPE_KEY);
    if (UCHadoopConfConstants.UC_CREDENTIALS_TYPE_PATH_VALUE.equals(type)) {
      this.pathRequest =
          new GenerateTemporaryPathCredential()
              .url(require(conf, UCHadoopConfConstants.UC_PATH_KEY))
              .operation(
                  PathOperation.fromValue(
                      require(conf, UCHadoopConfConstants.UC_PATH_OPERATION_KEY)));
      this.tableRequest = null;
    } else {
      this.pathRequest = null;
      this.tableRequest =
          new GenerateTemporaryTableCredential()
              .tableId(require(conf, UCHadoopConfConstants.UC_TABLE_ID_KEY))
              .operation(
                  TableOperation.fromValue(
                      require(conf, UCHadoopConfConstants.UC_TABLE_OPERATION_KEY)));
    }
  }

  @Override
  public GenericCredential createCredential() throws ApiException {
    TemporaryCredentials tempCred =
        pathRequest != null
            ? api.generateTemporaryPathCredentials(pathRequest)
            : api.generateTemporaryTableCredentials(tableRequest);
    return new GenericCredential(tempCred);
  }

  private static String require(Configuration conf, String key) {
    String value = conf.get(key);
    Preconditions.checkArgument(
        value != null && !value.isEmpty(), "'%s' is not set in hadoop configuration", key);
    return value;
  }
}
