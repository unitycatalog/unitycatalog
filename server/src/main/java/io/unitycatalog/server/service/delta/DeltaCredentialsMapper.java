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
 * <p>The spec currently returns a single-element {@code storage-credentials} array; the response
 * type is an array to allow future multi-credential responses (e.g., federated access) without a
 * breaking change.
 */
public final class DeltaCredentialsMapper {

  private DeltaCredentialsMapper() {}

  /**
   * Build a {@link DeltaCredentialsResponse} from UC {@link TemporaryCredentials} for a given
   * operation. The storage prefix is taken from the credential's own {@code url}, which the
   * credential vendor scopes to the requested location.
   */
  public static DeltaCredentialsResponse toCredentialsResponse(
      TemporaryCredentials credentials, DeltaCredentialOperation operation) {
    return new DeltaCredentialsResponse()
        .storageCredentials(List.of(toStorageCredential(credentials, operation)));
  }

  private static DeltaStorageCredential toStorageCredential(
      TemporaryCredentials credentials, DeltaCredentialOperation operation) {
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
        .prefix(credentials.getUrl())
        .operation(operation)
        .config(config)
        .expirationTimeMs(credentials.getExpirationTime());
  }
}
