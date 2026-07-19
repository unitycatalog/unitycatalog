package io.unitycatalog.hadoop.internal.auth;

import io.unitycatalog.client.internal.Clock;
import io.unitycatalog.client.internal.Preconditions;
import java.util.Objects;

/**
 * Internal credential model used by Hadoop token providers.
 *
 * <p>A plain, cloud-agnostic value type holding one cloud family's normalized credential values
 * (AWS, Azure, or GCS) plus an optional expiration. Constructed via the {@link #forAws}, {@link
 * #forAzure}, and {@link #forGcs} factories, each of which sets exactly one cloud family.
 */
public final class GenericCredential {
  private final String awsAccessKeyId;
  private final String awsSecretAccessKey;
  private final String awsSessionToken;
  private final String azureSasToken;
  private final String gcsOauthToken;
  private final Long expirationTimeMillis;

  private GenericCredential(
      String awsAccessKeyId,
      String awsSecretAccessKey,
      String awsSessionToken,
      String azureSasToken,
      String gcsOauthToken,
      Long expirationTimeMillis) {
    boolean isAws = awsAccessKeyId != null || awsSecretAccessKey != null || awsSessionToken != null;
    boolean isAzure = azureSasToken != null;
    boolean isGcs = gcsOauthToken != null;
    Preconditions.checkArgument(
        (isAws ? 1 : 0) + (isAzure ? 1 : 0) + (isGcs ? 1 : 0) == 1,
        "GenericCredential must hold exactly one cloud family's credential values.");

    this.awsAccessKeyId = awsAccessKeyId;
    this.awsSecretAccessKey = awsSecretAccessKey;
    this.awsSessionToken = awsSessionToken;
    this.azureSasToken = azureSasToken;
    this.gcsOauthToken = gcsOauthToken;
    this.expirationTimeMillis = expirationTimeMillis;
  }

  public static GenericCredential forAws(
      String accessKey, String secretKey, String sessionToken, Long expiredTimeMillis) {
    return new GenericCredential(accessKey, secretKey, sessionToken, null, null, expiredTimeMillis);
  }

  public static GenericCredential forAzure(String sasToken, Long expiredTimeMillis) {
    return new GenericCredential(null, null, null, sasToken, null, expiredTimeMillis);
  }

  public static GenericCredential forGcs(String oauthToken, Long expiredTimeMillis) {
    return new GenericCredential(null, null, null, null, oauthToken, expiredTimeMillis);
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
        && Objects.equals(expirationTimeMillis, that.expirationTimeMillis);
  }
}
