package io.unitycatalog.hadoop.internal.auth;

import io.unitycatalog.client.model.AwsCredentials;
import io.unitycatalog.client.model.TemporaryCredentials;
import io.unitycatalog.hadoop.internal.CredentialUtil;

/** Converts a UC SDK {@link TemporaryCredentials} into an internal {@link GenericCredential}. */
final class UCTemporaryCredentialUtil {
  private static final String DESCRIPTION = "UC temporary credentials";

  private UCTemporaryCredentialUtil() {}

  static GenericCredential toGenericCredential(TemporaryCredentials tempCred) {
    Long expiration = tempCred.getExpirationTime();
    if (tempCred.getAwsTempCredentials() != null) {
      AwsCredentials aws = tempCred.getAwsTempCredentials();
      return GenericCredential.forAws(
          CredentialUtil.field(aws.getAccessKeyId(), DESCRIPTION, "AWS access key"),
          CredentialUtil.field(aws.getSecretAccessKey(), DESCRIPTION, "AWS secret key"),
          CredentialUtil.field(aws.getSessionToken(), DESCRIPTION, "AWS session token"),
          expiration);
    } else if (tempCred.getAzureUserDelegationSas() != null) {
      return GenericCredential.forAzure(
          CredentialUtil.field(
              tempCred.getAzureUserDelegationSas().getSasToken(), DESCRIPTION, "Azure SAS token"),
          expiration);
    } else {
      var gcp = tempCred.getGcpOauthToken();
      String oauthToken = gcp != null ? gcp.getOauthToken() : null;
      return GenericCredential.forGcs(
          CredentialUtil.field(oauthToken, DESCRIPTION, "GCS OAuth token"), expiration);
    }
  }
}
