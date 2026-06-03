package io.unitycatalog.hadoop.internal.auth;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.delta.api.TemporaryCredentialsApi;
import io.unitycatalog.client.delta.model.CredentialsResponse;
import io.unitycatalog.client.internal.Preconditions;
import io.unitycatalog.hadoop.internal.DeltaStorageCredentialUtil;
import io.unitycatalog.hadoop.internal.id.DeltaStagingTableCredId;
import java.util.UUID;

/** Adapts the UC Delta staging table credentials SDK API for Hadoop token providers. */
final class UCDeltaStagingTableCredentialFetcher implements GenericCredentialFetcher {

  private final TemporaryCredentialsApi api;
  private final UUID stagingTableId;
  private final String stagingTableLocation;

  UCDeltaStagingTableCredentialFetcher(
      DeltaStagingTableCredId credId, TemporaryCredentialsApi api) {
    Preconditions.checkNotNull(credId, "credId is required");

    this.api = Preconditions.checkNotNull(api, "Temporary credentials API is required");
    this.stagingTableId = UUID.fromString(credId.stagingTableId());
    this.stagingTableLocation = credId.location();
  }

  @Override
  public GenericCredential createCredential() throws ApiException {
    CredentialsResponse response = api.getStagingTableCredentials(stagingTableId);
    Preconditions.checkNotNull(
        response,
        "UC Delta API returned no credentials response for staging table '%s'.",
        stagingTableId);

    return new GenericCredential(
        DeltaStorageCredentialUtil.toTemporaryCredentials(
            DeltaStorageCredentialUtil.selectForLocation(
                stagingTableLocation, response.getStorageCredentials())));
  }
}
