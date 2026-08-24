package io.unitycatalog.server.service.credential;

import com.google.auth.oauth2.AccessToken;
import io.unitycatalog.server.model.AwsCredentials;
import io.unitycatalog.server.model.AzureUserDelegationSAS;
import io.unitycatalog.server.model.GcpOauthToken;
import io.unitycatalog.server.model.TemporaryCredentials;
import io.unitycatalog.server.service.credential.aws.AwsCredentialVendor;
import io.unitycatalog.server.service.credential.azure.AzureCredential;
import io.unitycatalog.server.service.credential.azure.AzureCredentialVendor;
import io.unitycatalog.server.service.credential.gcp.GcpCredentialVendor;
import io.unitycatalog.server.utils.ServerProperties;
import software.amazon.awssdk.services.sts.model.Credentials;

public class CloudCredentialVendor {

  private final AwsCredentialVendor awsCredentialVendor;
  private final AzureCredentialVendor azureCredentialVendor;
  private final GcpCredentialVendor gcpCredentialVendor;

  public CloudCredentialVendor(ServerProperties serverProperties) {
    this.awsCredentialVendor = new AwsCredentialVendor(serverProperties);
    this.azureCredentialVendor = new AzureCredentialVendor(serverProperties);
    this.gcpCredentialVendor = new GcpCredentialVendor(serverProperties);
  }

  public CloudCredentialVendor(
      AwsCredentialVendor awsCredentialVendor,
      AzureCredentialVendor azureCredentialVendor,
      GcpCredentialVendor gcpCredentialVendor) {
    this.awsCredentialVendor = awsCredentialVendor;
    this.azureCredentialVendor = azureCredentialVendor;
    this.gcpCredentialVendor = gcpCredentialVendor;
  }

  public TemporaryCredentials vendCredential(CredentialContext context) {
    return switch (context.getStorageScheme()) {
      case ABFS, ABFSS -> vendAzureCredential(context);
      case GS -> vendGcpCredential(context);
      case S3 -> vendAwsCredential(context);
      // For local file system, we return empty credentials
      case FILE, NULL -> new TemporaryCredentials();
    };
  }

  private TemporaryCredentials vendAzureCredential(CredentialContext context) {
    AzureCredential azureCredential = azureCredentialVendor.vendAzureCredential(context);
    return new TemporaryCredentials()
        .azureUserDelegationSas(
            new AzureUserDelegationSAS().sasToken(azureCredential.getSasToken()))
        .expirationTime(azureCredential.getExpirationTimeInEpochMillis());
  }

  private TemporaryCredentials vendGcpCredential(CredentialContext context) {
    AccessToken gcpToken = gcpCredentialVendor.vendGcpCredential(context);
    return new TemporaryCredentials()
        .gcpOauthToken(new GcpOauthToken().oauthToken(gcpToken.getTokenValue()))
        .expirationTime(gcpToken.getExpirationTime().getTime());
  }

  private TemporaryCredentials vendAwsCredential(CredentialContext context) {
    Credentials awsSessionCredentials = awsCredentialVendor.vendAwsCredentials(context);
    TemporaryCredentials temporaryCredentials =
        new TemporaryCredentials()
            .awsTempCredentials(
                new AwsCredentials()
                    .accessKeyId(awsSessionCredentials.accessKeyId())
                    .secretAccessKey(awsSessionCredentials.secretAccessKey())
                    .sessionToken(awsSessionCredentials.sessionToken()));

    // Explicitly set the expiration time for the temporary credentials if it's a non-static
    // credential. For static credential, the expiration time can be nullable.
    if (awsSessionCredentials.expiration() != null) {
      temporaryCredentials.expirationTime(awsSessionCredentials.expiration().toEpochMilli());
    }
    return temporaryCredentials;
  }
}
