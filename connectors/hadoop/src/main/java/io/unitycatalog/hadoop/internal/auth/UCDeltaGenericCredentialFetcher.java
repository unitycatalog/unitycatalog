package io.unitycatalog.hadoop.internal.auth;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.delta.api.DeltaTemporaryCredentialsApi;
import io.unitycatalog.client.delta.model.DeltaCredentialOperation;
import io.unitycatalog.client.delta.model.DeltaCredentialsResponse;
import io.unitycatalog.client.internal.Preconditions;
import io.unitycatalog.hadoop.internal.DeltaStorageCredentialUtil;
import io.unitycatalog.hadoop.internal.UCHadoopConfConstants;
import org.apache.hadoop.conf.Configuration;

/** Adapts the UC Delta temporary credentials SDK API for Hadoop token providers. */
final class UCDeltaGenericCredentialFetcher implements GenericCredentialFetcher {
  private final DeltaTemporaryCredentialsApi api;
  private final DeltaCredentialOperation operation;
  private final String catalog;
  private final String schema;
  private final String tableName;
  private final String location;

  UCDeltaGenericCredentialFetcher(Configuration conf, DeltaTemporaryCredentialsApi api) {
    Preconditions.checkNotNull(api, "api is required");
    this.api = api;
    String rawOp = require(conf, UCHadoopConfConstants.UC_TABLE_OPERATION_KEY);
    DeltaCredentialOperation op = DeltaCredentialOperation.fromValue(rawOp);
    Preconditions.checkArgument(
        op == DeltaCredentialOperation.READ || op == DeltaCredentialOperation.READ_WRITE,
        "UC Delta supports READ and READ_WRITE table operations, got: %s",
        rawOp);
    this.operation = op;
    this.catalog = require(conf, UCHadoopConfConstants.UC_DELTA_CATALOG_KEY);
    this.schema = require(conf, UCHadoopConfConstants.UC_DELTA_SCHEMA_KEY);
    this.tableName = require(conf, UCHadoopConfConstants.UC_DELTA_TABLE_NAME_KEY);
    this.location = require(conf, UCHadoopConfConstants.UC_DELTA_LOCATION_KEY);
  }

  @Override
  public GenericCredential createCredential() throws ApiException {
    DeltaCredentialsResponse response =
        api.getTableCredentials(operation, catalog, schema, tableName);
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

  private static String require(Configuration conf, String key) {
    String value = conf.get(key);
    Preconditions.checkArgument(
        value != null && !value.isEmpty(), "'%s' is not set in hadoop configuration", key);
    return value;
  }
}
