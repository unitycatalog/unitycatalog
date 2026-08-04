package io.unitycatalog.hadoop.internal.auth;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.delta.api.DeltaTemporaryCredentialsApi;
import io.unitycatalog.client.delta.model.DeltaCredentialsResponse;
import io.unitycatalog.client.delta.model.DeltaStorageCredential;
import io.unitycatalog.client.internal.Preconditions;
import io.unitycatalog.hadoop.internal.CredentialUtil;
import io.unitycatalog.hadoop.internal.id.DeltaStagingTableCredId;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

/** Adapts the UC Delta staging table credentials SDK API for Hadoop token providers. */
final class UCDeltaStagingTableCredentialFetcher implements GenericCredentialFetcher {

  private final DeltaTemporaryCredentialsApi api;
  private final UUID stagingTableId;
  private final String stagingTableLocation;

  UCDeltaStagingTableCredentialFetcher(
      DeltaStagingTableCredId credId, DeltaTemporaryCredentialsApi api) {
    Preconditions.checkNotNull(credId, "credId is required");
    Preconditions.checkNotNull(api, "Temporary credentials API is required");

    this.api = api;
    this.stagingTableId = UUID.fromString(credId.stagingTableId());
    this.stagingTableLocation = credId.location();
  }

  @Override
  public List<GenericCredential> createCredentials() throws ApiException {
    DeltaCredentialsResponse response = api.getStagingTableCredentials(stagingTableId);
    Preconditions.checkNotNull(
        response,
        "UC Delta API returned no credentials response for staging table '%s'.",
        stagingTableId);

    List<DeltaStorageCredential> storageCredentials = response.getStorageCredentials();
    Preconditions.checkArgument(
        storageCredentials != null && !storageCredentials.isEmpty(),
        "UC Delta API returned no storage credentials for staging table '%s'.",
        stagingTableId);
    return storageCredentials.stream()
        .map(CredentialUtil::toGenericCredential)
        .collect(Collectors.toList());
  }
}
