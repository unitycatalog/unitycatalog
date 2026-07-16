package io.unitycatalog.hadoop.internal.auth;

import io.unitycatalog.client.internal.Clock;
import java.util.Objects;

/**
 * Internal credential model used by Hadoop token providers.
 *
 * <p>A plain, cloud-agnostic value type: it holds one cloud's credential values (AWS, Azure, or
 * GCS), an optional scope {@code prefix}, and an optional expiration.
 */
public final class GenericCredential {

  private final String awsAccessKeyId;
  private final String awsSecretAccessKey;
  private final String awsSessionToken;
  private final String azureSasToken;
  private final String gcsOauthToken;
  private final String prefix;
  private final Long expirationTimeMillis;

  private GenericCredential(
      String awsAccessKeyId,
      String awsSecretAccessKey,
      String awsSessionToken,
      String azureSasToken,
      String gcsOauthToken,
      String prefix,
      Long expirationTimeMillis) {
    this.awsAccessKeyId = awsAccessKeyId;
    this.awsSecretAccessKey = awsSecretAccessKey;
    this.awsSessionToken = awsSessionToken;
    this.azureSasToken = azureSasToken;
    this.gcsOauthToken = gcsOauthToken;
    this.prefix = prefix;
    this.expirationTimeMillis = expirationTimeMillis;
  }

  public static GenericCredential forAws(
      String accessKey, String secretKey, String sessionToken, long expiredTimeMillis) {
    return new GenericCredential(
        accessKey, secretKey, sessionToken, null, null, null, expiredTimeMillis);
  }

  public static GenericCredential forAzure(String sasToken, long expiredTimeMillis) {
    return new GenericCredential(null, null, null, sasToken, null, null, expiredTimeMillis);
  }

  public static GenericCredential forGcs(String oauthToken, long expiredTimeMillis) {
    return new GenericCredential(null, null, null, null, oauthToken, null, expiredTimeMillis);
  }

  /** Returns a copy of this credential scoped to {@code prefix}. */
  public GenericCredential withPrefix(String prefix) {
    return new GenericCredential(
        awsAccessKeyId,
        awsSecretAccessKey,
        awsSessionToken,
        azureSasToken,
        gcsOauthToken,
        prefix,
        expirationTimeMillis);
  }

  public String awsAccessKeyId() {
    return awsAccessKeyId;
  }

  public String awsSecretAccessKey() {
    return awsSecretAccessKey;
  }

  public String awsSessionToken() {
    return awsSessionToken;
  }

  public String azureSasToken() {
    return azureSasToken;
  }

  public String gcsOauthToken() {
    return gcsOauthToken;
  }

  public Long expirationTimeMillis() {
    return expirationTimeMillis;
  }

  /** The storage path prefix this credential is scoped to, or null if not scoped. */
  public String prefix() {
    return prefix;
  }

  /**
   * Decide whether it's time to renew the credential/token in advance.
   *
   * @param clock to get the latest timestamp.
   * @param renewalLeadTimeMillis The amount of time before something expires when the renewal
   *     process should start.
   * @return true if it's ready to renew.
   */
  public boolean readyToRenew(Clock clock, long renewalLeadTimeMillis) {
    return expirationTimeMillis != null
        && expirationTimeMillis <= clock.now().toEpochMilli() + renewalLeadTimeMillis;
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        awsAccessKeyId,
        awsSecretAccessKey,
        awsSessionToken,
        azureSasToken,
        gcsOauthToken,
        prefix,
        expirationTimeMillis);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    GenericCredential that = (GenericCredential) o;
    return Objects.equals(awsAccessKeyId, that.awsAccessKeyId)
        && Objects.equals(awsSecretAccessKey, that.awsSecretAccessKey)
        && Objects.equals(awsSessionToken, that.awsSessionToken)
        && Objects.equals(azureSasToken, that.azureSasToken)
        && Objects.equals(gcsOauthToken, that.gcsOauthToken)
        && Objects.equals(prefix, that.prefix)
        && Objects.equals(expirationTimeMillis, that.expirationTimeMillis);
  }
}
