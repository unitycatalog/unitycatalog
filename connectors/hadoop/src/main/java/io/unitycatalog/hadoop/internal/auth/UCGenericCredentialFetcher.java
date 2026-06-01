package io.unitycatalog.hadoop.internal.auth;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.TemporaryCredentialsApi;
import io.unitycatalog.client.internal.Preconditions;
import io.unitycatalog.client.model.GenerateTemporaryPathCredential;
import io.unitycatalog.client.model.GenerateTemporaryTableCredential;
import io.unitycatalog.client.model.PathOperation;
import io.unitycatalog.client.model.TableOperation;
import io.unitycatalog.client.model.TemporaryCredentials;
import io.unitycatalog.hadoop.internal.id.PathCredId;
import io.unitycatalog.hadoop.internal.id.TableCredId;

/** Adapts the standard Unity Catalog temporary credentials SDK API for Hadoop token providers. */
final class UCGenericCredentialFetcher implements GenericCredentialFetcher {
  private final TemporaryCredentialsApi api;
  private final GenerateTemporaryPathCredential pathRequest;
  private final GenerateTemporaryTableCredential tableRequest;

  UCGenericCredentialFetcher(TableCredId credId, TemporaryCredentialsApi api) {
    this.api = Preconditions.checkNotNull(api, "api is required");
    Preconditions.checkNotNull(credId, "credId is required");
    this.pathRequest = null;
    this.tableRequest =
        new GenerateTemporaryTableCredential()
            .tableId(credId.tableId())
            .operation(TableOperation.fromValue(credId.tableOperation()));
  }

  UCGenericCredentialFetcher(PathCredId credId, TemporaryCredentialsApi api) {
    this.api = Preconditions.checkNotNull(api, "api is required");
    Preconditions.checkNotNull(credId, "credId is required");
    this.pathRequest =
        new GenerateTemporaryPathCredential()
            .url(credId.path())
            .operation(PathOperation.fromValue(credId.pathOperation()));
    this.tableRequest = null;
  }

  @Override
  public GenericCredential createCredential() throws ApiException {
    TemporaryCredentials tempCred =
        pathRequest != null
            ? api.generateTemporaryPathCredentials(pathRequest)
            : api.generateTemporaryTableCredentials(tableRequest);
    return new GenericCredential(tempCred);
  }
}
