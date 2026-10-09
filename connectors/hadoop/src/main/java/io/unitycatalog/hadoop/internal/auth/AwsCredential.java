package io.unitycatalog.hadoop.internal.auth;

import io.unitycatalog.client.internal.Preconditions;
import java.util.Objects;

public final class AwsCredential extends GenericCredential {
  private final String accessKeyId;
  private final String secretAccessKey;
  private final String sessionToken;
  private final String endpointUrl;

  public AwsCredential(
      String accessKeyId,
      String secretAccessKey,
      String sessionToken,
      Long expirationTimeMillis,
      String prefix) {
    this(accessKeyId, secretAccessKey, sessionToken, expirationTimeMillis, prefix, null);
  }

  public AwsCredential(
      String accessKeyId,
      String secretAccessKey,
      String sessionToken,
      Long expirationTimeMillis,
      String prefix,
      String endpointUrl) {
    super(expirationTimeMillis, prefix);
    Preconditions.checkArgument(
        accessKeyId != null && !accessKeyId.isEmpty(), "AWS access key is missing");
    Preconditions.checkArgument(
        secretAccessKey != null && !secretAccessKey.isEmpty(), "AWS secret key is missing");
    this.accessKeyId = accessKeyId;
    this.secretAccessKey = secretAccessKey;
    // Session tokens are required for STS temporary credentials, but static S3 access keys
    // (STS-less stores) vend access/secret only. Treat blank as absent so Hadoop/S3A can use
    // AwsBasicCredentials without changing the STS path.
    this.sessionToken = sessionToken == null || sessionToken.isEmpty() ? null : sessionToken;
    this.endpointUrl = endpointUrl;
  }

  /** {@code true} when this credential includes an STS session token. */
  public boolean hasSessionToken() {
    return sessionToken != null;
  }

  public String accessKeyId() {
    return accessKeyId;
  }

  /** S3-compatible endpoint the credential was vended for, or {@code null} for real AWS S3. */
  public String endpointUrl() {
    return endpointUrl;
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
        && Objects.equals(sessionToken, that.sessionToken)
        && Objects.equals(endpointUrl, that.endpointUrl);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), accessKeyId, secretAccessKey, sessionToken, endpointUrl);
  }
}
