package io.unitycatalog.hadoop.internal.auth;

import io.unitycatalog.client.internal.Clock;
import io.unitycatalog.client.model.AwsCredentials;
import io.unitycatalog.client.model.AzureUserDelegationSAS;
import io.unitycatalog.client.model.GcpOauthToken;
import io.unitycatalog.client.model.TemporaryCredentials;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Internal credential wrapper used by Hadoop token providers.
 *
 * <p>This class normalizes UC SDK temporary credentials into cloud-specific credential values. A
 * vend may also carry {@link #additionalScopedCredentials()} for other locations a table's reads
 * span.
 */
public class GenericCredential {
  private final TemporaryCredentials tempCred;
  private final List<ScopedCredential> additionalScopedCredentials;

  public GenericCredential(TemporaryCredentials tempCred) {
    this(tempCred, Collections.emptyList());
  }

  public GenericCredential(
      TemporaryCredentials tempCred, List<ScopedCredential> additionalScopedCredentials) {
    this.tempCred = tempCred;
    this.additionalScopedCredentials =
        Collections.unmodifiableList(new ArrayList<>(additionalScopedCredentials));
  }

  public static GenericCredential forAws(
      String accessKey, String secretKey, String sessionToken, long expiredTimeMillis) {
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
   * Credentials vended alongside the primary one for other location prefixes. Empty unless the
   * backing API returned multiple scoped credentials.
   */
  public List<ScopedCredential> additionalScopedCredentials() {
    return additionalScopedCredentials;
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
    return tempCred.getExpirationTime() != null
        && tempCred.getExpirationTime() <= clock.now().toEpochMilli() + renewalLeadTimeMillis;
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
