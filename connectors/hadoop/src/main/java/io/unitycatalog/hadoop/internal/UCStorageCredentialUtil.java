package io.unitycatalog.hadoop.internal;

import io.unitycatalog.client.internal.Preconditions;
import io.unitycatalog.client.model.AwsCredentials;
import io.unitycatalog.client.model.AzureUserDelegationSAS;
import io.unitycatalog.client.model.GcpOauthToken;
import io.unitycatalog.client.model.TemporaryCredentials;
import io.unitycatalog.hadoop.internal.auth.GenericCredential;

/** Internal utility for standard UC temporary credentials. */
public final class UCStorageCredentialUtil {
  private UCStorageCredentialUtil() {}

  /**
   * Converts standard UC {@link TemporaryCredentials} into a {@link GenericCredential}.
   *
   * <p>Exactly one cloud sub-model must be present; the credential is not scoped to a prefix (the
   * UC temporary-credentials model carries none).
   */
  public static GenericCredential toGenericCredential(TemporaryCredentials tempCred) {
    Preconditions.checkNotNull(tempCred, "temporaryCredentials cannot be null");
    Long expiry = tempCred.getExpirationTime();
    AwsCredentials aws = tempCred.getAwsTempCredentials();
    AzureUserDelegationSAS azure = tempCred.getAzureUserDelegationSas();
    GcpOauthToken gcp = tempCred.getGcpOauthToken();
    if (aws != null) {
      return GenericCredential.forAws(
          aws.getAccessKeyId(),
          aws.getSecretAccessKey(),
          aws.getSessionToken(),
          expiry == null ? Long.MAX_VALUE : expiry);
    } else if (azure != null) {
      return GenericCredential.forAzure(
          azure.getSasToken(), expiry == null ? Long.MAX_VALUE : expiry);
    } else if (gcp != null) {
      return GenericCredential.forGcs(
          gcp.getOauthToken(), expiry == null ? Long.MAX_VALUE : expiry);
    }
    throw new IllegalArgumentException(
        "UC temporary credentials contain no cloud credential values.");
  }
}
