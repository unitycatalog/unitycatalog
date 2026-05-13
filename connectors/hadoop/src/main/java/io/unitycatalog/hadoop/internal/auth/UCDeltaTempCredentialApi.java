package io.unitycatalog.hadoop.internal.auth;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.delta.api.TemporaryCredentialsApi;
import io.unitycatalog.client.delta.model.CredentialOperation;
import io.unitycatalog.client.delta.model.CredentialsResponse;
import io.unitycatalog.client.internal.Preconditions;
import io.unitycatalog.hadoop.internal.DeltaStorageCredentialUtil;
import io.unitycatalog.hadoop.internal.UCHadoopConfConstants;
import org.apache.hadoop.conf.Configuration;

/** Adapts the UC Delta temporary credentials SDK API for Hadoop token providers. */
final class UCDeltaTempCredentialApi implements TempCredentialApi {
  private final TemporaryCredentialsApi api;
  private final CredentialOperation operation;
  private final String catalog;
  private final String schema;
  private final String tableName;
  private final String location;

  UCDeltaTempCredentialApi(Configuration conf, TemporaryCredentialsApi api) {
    Preconditions.checkNotNull(api, "api is required");
    this.api = api;
    this.operation =
        toCredentialOperation(require(conf, UCHadoopConfConstants.UC_TABLE_OPERATION_KEY));
    this.catalog = require(conf, UCHadoopConfConstants.UC_DELTA_CATALOG_KEY);
    this.schema = require(conf, UCHadoopConfConstants.UC_DELTA_SCHEMA_KEY);
    this.tableName = require(conf, UCHadoopConfConstants.UC_DELTA_TABLE_NAME_KEY);
    this.location = require(conf, UCHadoopConfConstants.UC_DELTA_LOCATION_KEY);
  }

  @Override
  public GenericCredential createCredential() throws ApiException {
    return createDeltaTableCredential(api, operation, catalog, schema, tableName, location);
  }

  private static GenericCredential createDeltaTableCredential(
      TemporaryCredentialsApi api,
      CredentialOperation operation,
      String catalog,
      String schema,
      String tableName,
      String location)
      throws ApiException {
    CredentialsResponse response = api.getTableCredentials(operation, catalog, schema, tableName);
    Preconditions.checkArgument(
        response != null,
        "UC Delta API returned no credentials response for '%s.%s.%s'.",
        catalog,
        schema,
        tableName);
    return new GenericCredential(
        DeltaStorageCredentialUtil.toTemporaryCredentials(
            DeltaStorageCredentialUtil.selectForLocation(
                location, response.getStorageCredentials())));
  }

  static CredentialOperation toCredentialOperation(String tableOperation) {
    switch (CredentialOperation.fromValue(tableOperation)) {
      case READ:
        return CredentialOperation.READ;
      case READ_WRITE:
        return CredentialOperation.READ_WRITE;
      default:
        throw new IllegalArgumentException(
            "UC Delta supports READ and READ_WRITE table operations, got: " + tableOperation);
    }
  }

  private static String require(Configuration conf, String key) {
    String value = conf.get(key);
    Preconditions.checkArgument(
        value != null && !value.isEmpty(), "'%s' is not set in hadoop configuration", key);
    return value;
  }
}
