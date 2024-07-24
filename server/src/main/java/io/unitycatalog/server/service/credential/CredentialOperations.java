package io.unitycatalog.server.service.credential;

import com.google.auth.oauth2.AccessToken;
import io.unitycatalog.server.model.AwsCredentials;
import io.unitycatalog.server.model.AzureUserDelegationSAS;
import io.unitycatalog.server.model.GcpOauthToken;
import io.unitycatalog.server.model.GenerateTemporaryTableCredentialResponse;
import io.unitycatalog.server.model.TableInfo;
import io.unitycatalog.server.service.credential.aws.AwsCredentialVendor;
import io.unitycatalog.server.service.credential.azure.AzureCredential;
import io.unitycatalog.server.service.credential.azure.AzureCredentialVendor;
import io.unitycatalog.server.service.credential.gcp.GcpCredentialVendor;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;

import java.net.URI;
import java.util.List;
import java.util.Set;

import static io.unitycatalog.server.utils.Constants.URI_SCHEME_ABFS;
import static io.unitycatalog.server.utils.Constants.URI_SCHEME_ABFSS;
import static io.unitycatalog.server.utils.Constants.URI_SCHEME_GS;
import static io.unitycatalog.server.utils.Constants.URI_SCHEME_S3;

public class CredentialOperations {

  private AwsCredentialVendor awsCredentialVendor;
  private AzureCredentialVendor azureCredentialVendor;
  private GcpCredentialVendor gcpCredentialVendor;

  public CredentialOperations() {
    this.awsCredentialVendor = new AwsCredentialVendor();
    this.azureCredentialVendor = new AzureCredentialVendor();
    this.gcpCredentialVendor = new GcpCredentialVendor();
  }

  public GenerateTemporaryTableCredentialResponse vendCredentialForTable(TableInfo table) {
    URI storageLocationUri = URI.create(table.getStorageLocation());

    // FIXME!! Update privileges when access controls are implemented
    CredentialContext credentialContext = CredentialContext.builder().storageScheme(storageLocationUri.getScheme()).storageBasePath(storageLocationUri.getPath())
      .privileges(Set.of(CredentialContext.Privilege.SELECT, CredentialContext.Privilege.UPDATE)).locations(List.of(table.getStorageLocation())).build();

    return switch (credentialContext.storageScheme) {
      case URI_SCHEME_ABFS, URI_SCHEME_ABFSS -> vendAzureCredentialForTable(credentialContext);
      case URI_SCHEME_GS -> vendGcpCredentialForTable(credentialContext);
      case URI_SCHEME_S3 -> vendAwsCredentialForTable(credentialContext);
      default -> new GenerateTemporaryTableCredentialResponse();
    };
  }

  public GenerateTemporaryTableCredentialResponse vendAwsCredentialForTable(CredentialContext context) {
    AwsSessionCredentials awsSessionCredentials = vendAwsCredential(context);
    return new GenerateTemporaryTableCredentialResponse().awsTempCredentials(new AwsCredentials()
      .accessKeyId(awsSessionCredentials.accessKeyId())
      .secretAccessKey(awsSessionCredentials.secretAccessKey())
      .sessionToken(awsSessionCredentials.sessionToken()));
  }

  public GenerateTemporaryTableCredentialResponse vendAzureCredentialForTable(CredentialContext context) {
    AzureCredential azureCredential = vendAzureCredential(context);
    return new GenerateTemporaryTableCredentialResponse()
      .azureUserDelegationSas(new AzureUserDelegationSAS().sasToken(azureCredential.sasToken()))
      .expirationTime(azureCredential.expirationTimeInEpochMillis());
  }

  public GenerateTemporaryTableCredentialResponse vendGcpCredentialForTable(CredentialContext context) {
    AccessToken gcpToken = vendGcpToken(context);
    return new GenerateTemporaryTableCredentialResponse()
      .gcpOauthToken(new GcpOauthToken().oauthToken(gcpToken.getTokenValue()))
      .expirationTime(gcpToken.getExpirationTime().getTime());
  }

  public AwsSessionCredentials vendAwsCredential(CredentialContext context) {
    return awsCredentialVendor.vendAwsCredentials(context);
  }

  public AzureCredential vendAzureCredential(CredentialContext context) {
    return azureCredentialVendor.vendAzureCredential(context);
  }

  public AccessToken vendGcpToken(CredentialContext context) {
    return gcpCredentialVendor.vendGcpToken(context);
  }

}
