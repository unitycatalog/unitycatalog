package io.unitycatalog.server.service.credential;

import com.google.auth.oauth2.AccessToken;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.*;
import io.unitycatalog.server.service.credential.aws.AwsCredentialVendor;
import io.unitycatalog.server.service.credential.azure.AzureCredential;
import io.unitycatalog.server.service.credential.azure.AzureCredentialVendor;
import io.unitycatalog.server.service.credential.gcp.GcpCredentialVendor;
import software.amazon.awssdk.services.sts.model.Credentials;

import java.net.URI;
import java.util.Set;

import static io.unitycatalog.server.utils.Constants.URI_SCHEME_ABFS;
import static io.unitycatalog.server.utils.Constants.URI_SCHEME_ABFSS;
import static io.unitycatalog.server.utils.Constants.URI_SCHEME_GS;
import static io.unitycatalog.server.utils.Constants.URI_SCHEME_S3;

public class CredentialOperations {

  private final AwsCredentialVendor awsCredentialVendor;
  private final AzureCredentialVendor azureCredentialVendor;
  private final GcpCredentialVendor gcpCredentialVendor;

  public CredentialOperations() {
    this.awsCredentialVendor = new AwsCredentialVendor();
    this.azureCredentialVendor = new AzureCredentialVendor();
    this.gcpCredentialVendor = new GcpCredentialVendor();
  }

  public CredentialResponse vendCredentialForPath(String path, Set<CredentialContext.Privilege> privileges) {
    if (path == null || path.isEmpty()) {
      throw new BaseException(ErrorCode.FAILED_PRECONDITION, "Storage location is null or empty.");
    }
    URI storageLocationUri = URI.create(path);
    // TODO: At some point, we need to check if user/subject has privileges they are asking for
    CredentialContext credentialContext = CredentialContext.create(storageLocationUri, privileges)
    return vendCredential(credentialContext);
  }

  public CredentialResponse vendCredential(CredentialContext context) {
    CredentialResponse.CredentialResponseBuilder builder = new CredentialResponse.CredentialResponseBuilder();

    switch (context.getStorageScheme()) {
      case URI_SCHEME_ABFS, URI_SCHEME_ABFSS -> {
        AzureCredential azureCredential = vendAzureCredential(context);
        builder.azureUserDelegationSas(new AzureUserDelegationSAS().sasToken(azureCredential.getSasToken()))
          .expirationTime(azureCredential.getExpirationTimeInEpochMillis());
      }
      case URI_SCHEME_GS -> {
        AccessToken gcpToken = vendGcpToken(context);
        builder.gcpOauthToken(new GcpOauthToken().oauthToken(gcpToken.getTokenValue()))
          .expirationTime(gcpToken.getExpirationTime().getTime());
      }
      case URI_SCHEME_S3 -> {
        Credentials awsSessionCredentials = vendAwsCredential(context);
        builder.awsTempCredentials(new AwsCredentials()
          .accessKeyId(awsSessionCredentials.accessKeyId())
          .secretAccessKey(awsSessionCredentials.secretAccessKey())
          .sessionToken(awsSessionCredentials.sessionToken()));
      }
    }

    return builder.build();
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
