package io.unitycatalog.server.service.credential;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.AwsCredentials;
import io.unitycatalog.server.model.AzureUserDelegationSAS;
import io.unitycatalog.server.model.GcpOauthToken;
import io.unitycatalog.server.model.TemporaryCredentials;
import io.unitycatalog.server.service.credential.aws.AwsCredentialVendor;
import io.unitycatalog.server.service.credential.azure.AzureCredential;
import io.unitycatalog.server.service.credential.azure.AzureCredentialVendor;
import io.unitycatalog.server.service.credential.gcp.GcpCredentialVendor;
import java.net.URI;
import java.util.Set;
import com.google.auth.oauth2.AccessToken;
import io.unitycatalog.server.utils.NormalizedURL;
import software.amazon.awssdk.services.sts.model.Credentials;

public class CloudCredentialVendor {

  private final AwsCredentialVendor awsCredentialVendor;
  private final AzureCredentialVendor azureCredentialVendor;
  private final GcpCredentialVendor gcpCredentialVendor;

  public CloudCredentialVendor(
      AwsCredentialVendor awsCredentialVendor,
      AzureCredentialVendor azureCredentialVendor,
      GcpCredentialVendor gcpCredentialVendor) {
    this.awsCredentialVendor = awsCredentialVendor;
    this.azureCredentialVendor = azureCredentialVendor;
    this.gcpCredentialVendor = gcpCredentialVendor;
  }

  public TemporaryCredentials vendCredential(
      NormalizedURL path,
      Set<CredentialContext.Privilege> privileges) {
    if (path == null || path.isEmpty()) {
      throw new BaseException(ErrorCode.FAILED_PRECONDITION, "Storage location is null or empty.");
    }
    URI storageLocationUri = path.toUri();
    // TODO: At some point, we need to check if user/subject has privileges they are asking for
    CredentialContext credentialContext = CredentialContext.create(storageLocationUri, privileges);
    return vendCredential(credentialContext);
  }

  public TemporaryCredentials vendCredential(CredentialContext context) {
    TemporaryCredentials temporaryCredentials = new TemporaryCredentials();

    switch (context.getStorageScheme()) {
      case ABFS, ABFSS -> {
        AzureCredential azureCredential = vendAzureCredential(context);
        temporaryCredentials
            .azureUserDelegationSas(
                new AzureUserDelegationSAS().sasToken(azureCredential.getSasToken()))
            .expirationTime(azureCredential.getExpirationTimeInEpochMillis());
      }
      case GS -> {
        AccessToken gcpToken = vendGcpToken(context);
        temporaryCredentials
            .gcpOauthToken(new GcpOauthToken().oauthToken(gcpToken.getTokenValue()))
            .expirationTime(gcpToken.getExpirationTime().getTime());
      }
      case S3 -> {
        Credentials awsSessionCredentials = vendAwsCredential(context);
        temporaryCredentials.awsTempCredentials(new AwsCredentials()
                .accessKeyId(awsSessionCredentials.accessKeyId())
                .secretAccessKey(awsSessionCredentials.secretAccessKey())
                .sessionToken(awsSessionCredentials.sessionToken()));

        // Explicitly set the expiration time for the temporary credentials if it's a non-static
        // credential. For static credential, the expiration time can be nullable.
        if (awsSessionCredentials.expiration() != null) {
          temporaryCredentials.expirationTime(awsSessionCredentials.expiration().toEpochMilli());
        }
      }
      // For local file system, we return empty credentials
      case FILE, NULL -> {}
    }

    return temporaryCredentials;
  }

  public Credentials vendAwsCredential(CredentialContext context) {
    return awsCredentialVendor.vendAwsCredentials(context);
  }

  public AzureCredential vendAzureCredential(CredentialContext context) {
    return azureCredentialVendor.vendAzureCredential(context);
  }

  public AccessToken vendGcpToken(CredentialContext context) {
    return gcpCredentialVendor.vendGcpToken(context);
  }
}
