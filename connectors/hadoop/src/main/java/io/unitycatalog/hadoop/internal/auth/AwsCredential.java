package io.unitycatalog.hadoop.internal.auth;

import io.unitycatalog.client.internal.Preconditions;
import java.util.Objects;

public final class AwsCredential extends GenericCredential {
  private final String accessKeyId;
  private final String secretAccessKey;
  private final String sessionToken;

  public AwsCredential(
      String accessKeyId,
      String secretAccessKey,
      String sessionToken,
      Long expirationTimeMillis,
      String location) {
    super(expirationTimeMillis, location);
    Preconditions.checkArgument(
        accessKeyId != null && !accessKeyId.isEmpty(), "AWS access key is missing");
    Preconditions.checkArgument(
        secretAccessKey != null && !secretAccessKey.isEmpty(), "AWS secret key is missing");
    Preconditions.checkArgument(
        sessionToken != null && !sessionToken.isEmpty(), "AWS session token is missing");
    this.accessKeyId = accessKeyId;
    this.secretAccessKey = secretAccessKey;
    this.sessionToken = sessionToken;
  }

  public String accessKeyId() {
    return accessKeyId;
  }

  public String secretAccessKey() {
    return secretAccessKey;
  }

  public String sessionToken() {
    return sessionToken;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof AwsCredential) || !super.equals(o)) {
      return false;
    }
    AwsCredential that = (AwsCredential) o;
    return Objects.equals(accessKeyId, that.accessKeyId)
        && Objects.equals(secretAccessKey, that.secretAccessKey)
        && Objects.equals(sessionToken, that.sessionToken);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), accessKeyId, secretAccessKey, sessionToken);
  }
}
