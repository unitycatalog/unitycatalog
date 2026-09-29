package io.unitycatalog.server.service.credential.cache;

import io.unitycatalog.server.model.AwsCredentials;
import io.unitycatalog.server.model.AzureUserDelegationSAS;
import io.unitycatalog.server.model.GcpOauthToken;
import io.unitycatalog.server.model.TemporaryCredentials;
import java.util.Objects;

/**
 * The cache value: an immutable context and a private credential snapshot. Mutable credential
 * models are deep-copied on construction and access.
 */
public record CachedCredential(CredentialCacheContext context, TemporaryCredentials credential) {

  public CachedCredential {
    Objects.requireNonNull(context, "context");
    credential = copy(Objects.requireNonNull(credential, "credential"));
  }

  /** Returns a copy that callers can modify without changing the cached snapshot. */
  public TemporaryCredentials credential() {
    return copy(credential);
  }

  private static TemporaryCredentials copy(TemporaryCredentials source) {
    TemporaryCredentials target =
        new TemporaryCredentials().expirationTime(source.getExpirationTime()).url(source.getUrl());
    if (source.getAwsTempCredentials() != null) {
      AwsCredentials aws = source.getAwsTempCredentials();
      target.awsTempCredentials(
          new AwsCredentials()
              .accessKeyId(aws.getAccessKeyId())
              .secretAccessKey(aws.getSecretAccessKey())
              .sessionToken(aws.getSessionToken()));
    }
    if (source.getAzureUserDelegationSas() != null) {
      target.azureUserDelegationSas(
          new AzureUserDelegationSAS().sasToken(source.getAzureUserDelegationSas().getSasToken()));
    }
    if (source.getGcpOauthToken() != null) {
      target.gcpOauthToken(
          new GcpOauthToken().oauthToken(source.getGcpOauthToken().getOauthToken()));
    }
    return target;
  }
}
