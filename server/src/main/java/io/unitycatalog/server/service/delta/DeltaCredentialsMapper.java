package io.unitycatalog.server.service.delta;

import io.unitycatalog.server.delta.model.CredentialOperation;
import io.unitycatalog.server.delta.model.CredentialsResponse;
import io.unitycatalog.server.delta.model.StorageCredential;
import io.unitycatalog.server.delta.model.StorageCredentialConfig;
import io.unitycatalog.server.model.TemporaryCredentials;
import java.util.List;

/**
 * Maps Unity Catalog's nested {@link TemporaryCredentials} (with provider-specific sub-objects) to
 * Delta REST Catalog's flat {@link StorageCredential} wire format (with a provider-agnostic typed
 * config of {@code s3.*} / {@code azure.*} / {@code gcs.*} fields).
 *
 * <p>The spec currently returns a single-element {@code storage-credentials} array; the response
 * type is an array to allow future multi-credential responses (e.g., federated access) without a
 * breaking change.
 */
public final class DeltaCredentialsMapper {

  private DeltaCredentialsMapper() {}

  /**
   * Build a Delta {@link CredentialsResponse} from UC {@link TemporaryCredentials} for a given
   * storage prefix and operation.
   */
  public static CredentialsResponse toCredentialsResponse(
      String prefix, TemporaryCredentials credentials, CredentialOperation operation) {
    return new CredentialsResponse()
        .storageCredentials(List.of(toStorageCredential(prefix, credentials, operation)));
  }

  private static StorageCredential toStorageCredential(
      String prefix, TemporaryCredentials credentials, CredentialOperation operation) {
    StorageCredentialConfig config = new StorageCredentialConfig();
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

    return new StorageCredential()
        .prefix(prefix)
        .operation(operation)
        .config(config)
        .expirationTimeMs(credentials.getExpirationTime());
  }
}
