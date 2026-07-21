package io.unitycatalog.hadoop.internal.auth;

import io.unitycatalog.client.internal.Preconditions;
import io.unitycatalog.client.model.AwsCredentials;
import io.unitycatalog.client.model.TemporaryCredentials;
import io.unitycatalog.hadoop.internal.CredentialUtil;

/** Converts a UC SDK {@link TemporaryCredentials} into an internal {@link GenericCredential}. */
final class UCTemporaryCredentialUtil {

  private UCTemporaryCredentialUtil() {}

  static GenericCredential toGenericCredential(TemporaryCredentials tempCred) {
    Long expiration = tempCred.getExpirationTime();
    if (tempCred.getAwsTempCredentials() != null) {
      AwsCredentials aws = tempCred.getAwsTempCredentials();
      return new AwsCredential(
          CredentialUtil.field(
              aws.getAccessKeyId(), "UC temporary credentials missing AWS access key"),
          CredentialUtil.field(
              aws.getSecretAccessKey(), "UC temporary credentials missing AWS secret key"),
          CredentialUtil.field(
              aws.getSessionToken(), "UC temporary credentials missing AWS session token"),
          expiration);
    } else if (tempCred.getAzureUserDelegationSas() != null) {
      return new AzureCredential(
          CredentialUtil.field(
              tempCred.getAzureUserDelegationSas().getSasToken(),
              "UC temporary credentials missing Azure SAS token"),
          expiration);
    } else {
      Preconditions.checkArgument(
          tempCred.getGcpOauthToken() != null, "UC temporary credentials missing GCP OAuth token");
      return new GcsCredential(
          CredentialUtil.field(
              tempCred.getGcpOauthToken().getOauthToken(),
              "UC temporary credentials missing GCS OAuth token"),
          expiration);
    }
  }
}
