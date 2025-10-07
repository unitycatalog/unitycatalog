package io.unitycatalog.spark.auth;

import io.unitycatalog.client.model.AwsCredentials;
import io.unitycatalog.client.model.TemporaryCredentials;

public class GeneralCredential {
  private final TemporaryCredentials tempCred;

  public GeneralCredential(TemporaryCredentials tempCred) {
    this.tempCred = tempCred;
  }

  public static GeneralCredential forAws(
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

    return new GeneralCredential(tempCred);
  }

  public TemporaryCredentials temporaryCredentials() {
    return tempCred;
  }

  public boolean readyToRenew(long renewalLeadTime) {
    return tempCred.getExpirationTime() != null &&
        tempCred.getExpirationTime() <= System.currentTimeMillis() + renewalLeadTime;
  }
}
