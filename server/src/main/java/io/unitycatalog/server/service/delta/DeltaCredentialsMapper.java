package io.unitycatalog.server.service.delta;

import io.unitycatalog.server.delta.model.DeltaCredentialOperation;
import io.unitycatalog.server.delta.model.DeltaCredentialsResponse;
import io.unitycatalog.server.delta.model.DeltaStorageCredential;
import io.unitycatalog.server.delta.model.DeltaStorageCredentialConfig;
import io.unitycatalog.server.model.TemporaryCredentials;
import java.util.List;

/**
 * Maps Unity Catalog's nested {@link TemporaryCredentials} (with provider-specific sub-objects) to
 * UC Delta API's flat {@link DeltaStorageCredential} wire format (with a provider-agnostic typed
 * config of {@code s3.*} / {@code azure.*} / {@code gcs.*} fields).
 *
 * <p>The {@code storage-credentials} response array carries one entry per storage prefix the caller
 * needs: a single entry for regular tables, and an additional read-only entry for the base table's
 * location when the table is a shallow clone.
 */
public final class DeltaCredentialsMapper {

  private DeltaCredentialsMapper() {}

  /**
   * Build a {@link DeltaCredentialsResponse} from UC {@link TemporaryCredentials} for a given
   * storage prefix and operation.
   */
  public static DeltaCredentialsResponse toCredentialsResponse(
      String prefix, TemporaryCredentials credentials, DeltaCredentialOperation operation) {
    return toCredentialsResponse(List.of(toStorageCredential(prefix, credentials, operation)));
  }

  /** Build a {@link DeltaCredentialsResponse} from already-mapped per-prefix credentials. */
  public static DeltaCredentialsResponse toCredentialsResponse(
      List<DeltaStorageCredential> storageCredentials) {
    return new DeltaCredentialsResponse().storageCredentials(storageCredentials);
  }

  /** Map UC {@link TemporaryCredentials} to a single per-prefix {@link DeltaStorageCredential}. */
  public static DeltaStorageCredential toStorageCredential(
      String prefix, TemporaryCredentials credentials, DeltaCredentialOperation operation) {
    DeltaStorageCredentialConfig config = new DeltaStorageCredentialConfig();
    var aws = credentials.getAwsTempCredentials();
    if (aws != null) {
      config.setS3AccessKeyId(aws.getAccessKeyId());
      config.setS3SecretAccessKey(aws.getSecretAccessKey());
      config.setS3SessionToken(aws.getSessionToken());
    }
    var azure = credentials.getAzureUserDelegationSas();
    if (azure != null) {
      config.setAzureSasToken(azure.getSasToken());
    }
    var gcp = credentials.getGcpOauthToken();
    if (gcp != null) {
      config.setGcsOauthToken(gcp.getOauthToken());
    }

    return new DeltaStorageCredential()
        .prefix(prefix)
        .operation(operation)
        .config(config)
        .expirationTimeMs(credentials.getExpirationTime());
  }
}
