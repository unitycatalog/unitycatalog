package io.unitycatalog.hadoop.internal.auth;

import io.unitycatalog.client.model.AwsCredentials;
import io.unitycatalog.client.model.TemporaryCredentials;

/** Converts a UC SDK {@link TemporaryCredentials} into an internal {@link GenericCredential}. */
final class UCTemporaryCredentialUtil {

  private UCTemporaryCredentialUtil() {}

  static GenericCredential toGenericCredential(TemporaryCredentials tempCred) {
    Long expiration = tempCred.getExpirationTime();
    if (tempCred.getAwsTempCredentials() != null) {
      AwsCredentials aws = tempCred.getAwsTempCredentials();
      return new AwsCredential(
          aws.getAccessKeyId(), aws.getSecretAccessKey(), aws.getSessionToken(), expiration);
    } else if (tempCred.getAzureUserDelegationSas() != null) {
      return new AzureCredential(tempCred.getAzureUserDelegationSas().getSasToken(), expiration);
    } else if (tempCred.getGcpOauthToken() != null) {
      return new GcsCredential(tempCred.getGcpOauthToken().getOauthToken(), expiration);
    }
    throw new IllegalArgumentException("UC temporary credentials contained no cloud credential");
  }
}
