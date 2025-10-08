package io.unitycatalog.spark.auth;

import io.unitycatalog.client.model.AwsCredentials;
import io.unitycatalog.client.model.TemporaryCredentials;

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

  public TemporaryCredentials temporaryCredentials() {
    return tempCred;
  }

  /**
   * Decide whether it's time to renew the credential/token in advance.
   *
   * @param renewalLeadTime The amount of time before something expires when the renewal process
   *                        should start.
   * @return true if it's ready to renew.
   */
  public boolean readyToRenew(long renewalLeadTime) {
    return tempCred.getExpirationTime() != null &&
        tempCred.getExpirationTime() <= System.currentTimeMillis() + renewalLeadTime;
  }
}
