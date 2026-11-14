package io.unitycatalog.hadoop.internal.auth;

import io.unitycatalog.client.internal.Preconditions;
import java.util.Objects;

public final class AwsCredential extends GenericCredential {
  private final String accessKeyId;
  private final String secretAccessKey;
  private final String sessionToken;
  // S3 endpoint of S3-compatible storage; null for AWS S3.
  private final String s3Endpoint;
  // Region to sign requests to s3Endpoint with; null if not known.
  private final String s3Region;

  public AwsCredential(
      String accessKeyId,
      String secretAccessKey,
      String sessionToken,
      Long expirationTimeMillis,
      String prefix) {
    this(accessKeyId, secretAccessKey, sessionToken, expirationTimeMillis, prefix, null);
  }

  /**
   * For S3-compatible storage the keys may be absent, when Unity Catalog vends only the S3
   * endpoint, and the session token may be absent, for a key pair that does not expire.
   *
   * @param accessKeyId the access key ID
   * @param secretAccessKey the secret access key
   * @param sessionToken the session token
   * @param expirationTimeMillis the expiration time, or null or {@code Long.MAX_VALUE} if it does
   *     not expire
   * @param prefix the storage path the credential covers
   * @param s3Endpoint the S3 endpoint of S3-compatible storage, or null for AWS S3
   */
  public AwsCredential(
      String accessKeyId,
      String secretAccessKey,
      String sessionToken,
      Long expirationTimeMillis,
      String prefix,
      String s3Endpoint) {
    this(
        accessKeyId,
        secretAccessKey,
        sessionToken,
        expirationTimeMillis,
        prefix,
        s3Endpoint,
        /* s3Region= */ null);
  }

  /**
   * Like {@link #AwsCredential(String, String, String, Long, String, String)}, with the region of
   * the S3-compatible storage.
   *
   * @param s3Region the region to sign requests to {@code s3Endpoint} with, or null if not known
   */
  public AwsCredential(
      String accessKeyId,
      String secretAccessKey,
      String sessionToken,
      Long expirationTimeMillis,
      String prefix,
      String s3Endpoint,
      String s3Region) {
    super(expirationTimeMillis, prefix);
    boolean isEndpointOnly =
        s3Endpoint != null
            && accessKeyId == null
            && secretAccessKey == null
            && sessionToken == null;
    if (!isEndpointOnly) {
      Preconditions.checkArgument(
          accessKeyId != null && !accessKeyId.isEmpty(), "AWS access key is missing");
      Preconditions.checkArgument(
          secretAccessKey != null && !secretAccessKey.isEmpty(), "AWS secret key is missing");
      // Only S3-compatible storage vends a key pair without a session token.
      Preconditions.checkArgument(
          s3Endpoint != null
              ? sessionToken == null || !sessionToken.isEmpty()
              : sessionToken != null && !sessionToken.isEmpty(),
          "AWS session token is missing");
    }
    this.accessKeyId = accessKeyId;
    this.secretAccessKey = secretAccessKey;
    this.sessionToken = sessionToken;
    this.s3Endpoint = s3Endpoint;
    this.s3Region = s3Region;
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

  /** The S3 endpoint of S3-compatible storage, or null for AWS S3. */
  public String s3Endpoint() {
    return s3Endpoint;
  }

  /** The region to sign requests to {@link #s3Endpoint()} with, or null if not known. */
  public String s3Region() {
    return s3Region;
  }

  /**
   * Whether this carries keys; an S3-endpoint-only credential leaves the engine's own keys in use.
   */
  public boolean hasKeys() {
    return accessKeyId != null;
  }

  /**
   * Session credentials, and a key pair vended with a lifetime, are renewed so that an update of
   * the key pair reaches the engine; a key pair without a lifetime, or only an S3 endpoint, is not.
   */
  @Override
  public boolean isRenewable() {
    Long expiration = expirationTimeMillis();
    return sessionToken != null
        || (hasKeys() && expiration != null && expiration != Long.MAX_VALUE);
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
        && Objects.equals(s3Endpoint, that.s3Endpoint)
        && Objects.equals(s3Region, that.s3Region);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        super.hashCode(), accessKeyId, secretAccessKey, sessionToken, s3Endpoint, s3Region);
  }
}
