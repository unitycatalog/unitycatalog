package io.unitycatalog.hadoop.internal.auth;

import io.unitycatalog.hadoop.internal.CredentialUtil;
import java.util.Objects;

public final class AwsCredential extends GenericCredential {
  private final String accessKeyId;
  private final String secretAccessKey;
  private final String sessionToken;

  public AwsCredential(
      String accessKeyId, String secretAccessKey, String sessionToken, Long expirationTimeMillis) {
    super(expirationTimeMillis);
    this.accessKeyId = CredentialUtil.field(accessKeyId, "AWS access key is missing");
    this.secretAccessKey = CredentialUtil.field(secretAccessKey, "AWS secret key is missing");
    this.sessionToken = CredentialUtil.field(sessionToken, "AWS session token is missing");
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
