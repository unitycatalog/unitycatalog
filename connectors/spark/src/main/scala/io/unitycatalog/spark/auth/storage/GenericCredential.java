package io.unitycatalog.spark.auth.storage;

import io.unitycatalog.client.model.AwsCredentials;
import io.unitycatalog.client.model.AzureUserDelegationSAS;
import io.unitycatalog.client.model.GcpOauthToken;
import io.unitycatalog.client.model.TemporaryCredentials;
import io.unitycatalog.spark.utils.Clock;
import java.util.Objects;

public class GenericCredential {
  private final TemporaryCredentials tempCred;

  public GenericCredential(TemporaryCredentials tempCred) {
    this.tempCred = tempCred;
  }

  public static GenericCredential forAws(
      String accessKey,
      String secretKey,
      String sessionToken,
      long expiredTimeMillis) {
    // Initialize the aws credentials.
    AwsCredentials awsCredentials = new AwsCredentials();
    awsCredentials.setAccessKeyId(accessKey);
    awsCredentials.setSecretAccessKey(secretKey);
    awsCredentials.setSessionToken(sessionToken);

    // Initialize the unity catalog's temporary credentials.
    TemporaryCredentials tempCred = new TemporaryCredentials();
    tempCred.setAwsTempCredentials(awsCredentials);
    tempCred.setExpirationTime(expiredTimeMillis);

    return new GenericCredential(tempCred);
  }

  public static GenericCredential forAzure(String sasToken, long expiredTimeMillis) {
    AzureUserDelegationSAS azureSAS = new AzureUserDelegationSAS();
    azureSAS.setSasToken(sasToken);

    TemporaryCredentials tempCred = new TemporaryCredentials();
    tempCred.setAzureUserDelegationSas(azureSAS);
    tempCred.setExpirationTime(expiredTimeMillis);

    return new GenericCredential(tempCred);
  }

  public static GenericCredential forGcs(String oauthToken, long expiredTimeMillis) {
    GcpOauthToken gcpOauthToken = new GcpOauthToken();
    gcpOauthToken.setOauthToken(oauthToken);

    TemporaryCredentials tempCred = new TemporaryCredentials();
    tempCred.setGcpOauthToken(gcpOauthToken);
    tempCred.setExpirationTime(expiredTimeMillis);

    return new GenericCredential(tempCred);
  }

  public TemporaryCredentials temporaryCredentials() {
    return tempCred;
  }

  /**
   * Decide whether it's time to renew the credential/token in advance.
   *
   * @param clock                 to get the latest timestamp.
   * @param renewalLeadTimeMillis The amount of time before something expires when the renewal
   *                              process should start.
   * @return true if it's ready to renew.
   */
  public boolean readyToRenew(Clock clock, long renewalLeadTimeMillis) {
    return tempCred.getExpirationTime() != null &&
        tempCred.getExpirationTime() <= clock.now().toEpochMilli() + renewalLeadTimeMillis;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(tempCred);
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
    return Objects.equals(tempCred, that.tempCred);
  }
}
