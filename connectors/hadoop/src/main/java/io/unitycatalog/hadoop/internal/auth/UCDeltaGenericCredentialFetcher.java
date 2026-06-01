package io.unitycatalog.hadoop.internal.auth;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.delta.api.TemporaryCredentialsApi;
import io.unitycatalog.client.delta.model.CredentialOperation;
import io.unitycatalog.client.delta.model.CredentialsResponse;
import io.unitycatalog.client.internal.Preconditions;
import io.unitycatalog.hadoop.internal.DeltaStorageCredentialUtil;
import io.unitycatalog.hadoop.internal.UCDeltaTableIdentifier;
import io.unitycatalog.hadoop.internal.id.DeltaTableCredId;

/** Adapts the UC Delta temporary credentials SDK API for Hadoop token providers. */
final class UCDeltaGenericCredentialFetcher implements GenericCredentialFetcher {
  private final TemporaryCredentialsApi api;
  private final DeltaTableCredId credId;
  private final CredentialOperation operation;

  UCDeltaGenericCredentialFetcher(DeltaTableCredId credId, TemporaryCredentialsApi api) {
    this.api = Preconditions.checkNotNull(api, "api is required");
    this.credId = Preconditions.checkNotNull(credId, "credId is required");
    CredentialOperation op = CredentialOperation.fromValue(credId.tableOperation());
    Preconditions.checkArgument(
        op == CredentialOperation.READ || op == CredentialOperation.READ_WRITE,
        "UC Delta supports READ and READ_WRITE table operations, got: %s",
        credId.tableOperation());
    this.operation = op;
  }

  @Override
  public GenericCredential createCredential() throws ApiException {
    UCDeltaTableIdentifier id = credId.identifier();
    CredentialsResponse response =
        api.getTableCredentials(operation, id.catalog(), id.schema(), id.table());
    Preconditions.checkArgument(
        response != null,
        "UC Delta API returned no credentials response for '%s.%s.%s'.",
        id.catalog(),
        id.schema(),
        id.table());
    return new GenericCredential(
        DeltaStorageCredentialUtil.toTemporaryCredentials(
            DeltaStorageCredentialUtil.selectForLocation(
                credId.location(), response.getStorageCredentials())));
  }
}
