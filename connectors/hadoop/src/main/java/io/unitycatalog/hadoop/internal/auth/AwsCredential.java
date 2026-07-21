package io.unitycatalog.hadoop.internal.auth;

import java.util.Objects;

public final class AwsCredential extends GenericCredential {
  private final String accessKeyId;
  private final String secretAccessKey;
  private final String sessionToken;

  public AwsCredential(
      String accessKeyId, String secretAccessKey, String sessionToken, Long expirationTimeMillis) {
    super(expirationTimeMillis);
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
    if (!(o instanceof AwsCredential)) {
      return false;
    }
    AwsCredential that = (AwsCredential) o;
    return Objects.equals(accessKeyId, that.accessKeyId)
        && Objects.equals(secretAccessKey, that.secretAccessKey)
        && Objects.equals(sessionToken, that.sessionToken)
        && Objects.equals(expirationTimeMillis(), that.expirationTimeMillis());
  }

  @Override
  public int hashCode() {
    return Objects.hash(accessKeyId, secretAccessKey, sessionToken, expirationTimeMillis());
  }
}
