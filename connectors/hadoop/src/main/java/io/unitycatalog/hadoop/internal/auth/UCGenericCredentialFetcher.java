package io.unitycatalog.hadoop.internal.auth;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.TemporaryCredentialsApi;
import io.unitycatalog.client.internal.Preconditions;
import io.unitycatalog.client.model.GenerateTemporaryPathCredential;
import io.unitycatalog.client.model.GenerateTemporaryTableCredential;
import io.unitycatalog.client.model.PathOperation;
import io.unitycatalog.client.model.TableOperation;
import io.unitycatalog.client.model.TemporaryCredentials;
import io.unitycatalog.hadoop.internal.CredentialUtil;
import io.unitycatalog.hadoop.internal.id.PathCredId;
import io.unitycatalog.hadoop.internal.id.TableCredId;
import java.util.Collections;
import java.util.List;

/** Adapts the standard Unity Catalog temporary credentials SDK API for Hadoop token providers. */
final class UCGenericCredentialFetcher implements GenericCredentialFetcher {
  private final CredentialCaller credentialCaller;

  UCGenericCredentialFetcher(TableCredId credId, TemporaryCredentialsApi api) {
    Preconditions.checkNotNull(credId, "credId is required");
    Preconditions.checkNotNull(api, "api is required");

    GenerateTemporaryTableCredential tableRequest =
        new GenerateTemporaryTableCredential()
            .tableId(credId.tableId())
            .operation(TableOperation.fromValue(credId.tableOperation()));
    this.credentialCaller = () -> api.generateTemporaryTableCredentials(tableRequest);
  }

  UCGenericCredentialFetcher(PathCredId credId, TemporaryCredentialsApi api) {
    Preconditions.checkNotNull(credId, "credId is required");
    Preconditions.checkNotNull(api, "api is required");

    GenerateTemporaryPathCredential pathRequest =
        new GenerateTemporaryPathCredential()
            .url(credId.path())
            .operation(PathOperation.fromValue(credId.pathOperation()));
    this.credentialCaller = () -> api.generateTemporaryPathCredentials(pathRequest);
  }

  @Override
  public List<GenericCredential> createCredentials() throws ApiException {
    return Collections.singletonList(CredentialUtil.toGenericCredential(credentialCaller.get()));
  }

  /** Supplies temporary credentials from a pre-built request, bound at construction time. */
  @FunctionalInterface
  private interface CredentialCaller {
    TemporaryCredentials get() throws ApiException;
  }
}
