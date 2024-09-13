package io.unitycatalog.server.service.credential;

import io.unitycatalog.server.model.*;
import lombok.Builder;
import lombok.Getter;

@Builder
@Getter
public class CredentialResponse {
  private AwsCredentials awsTempCredentials;
  private AzureUserDelegationSAS azureUserDelegationSas;
  private GcpOauthToken gcpOauthToken;
  private Long expirationTime;

  public GenerateTemporaryModelVersionCredentialsResponse toModelVersionCredentialsResponse() {
    return new GenerateTemporaryModelVersionCredentialsResponse()
        .awsTempCredentials(awsTempCredentials)
        .azureUserDelegationSas(azureUserDelegationSas)
        .gcpOauthToken(gcpOauthToken)
        .expirationTime(expirationTime);
  }

  public GenerateTemporaryTableCredentialResponse toTableCredentialResponse() {
    return new GenerateTemporaryTableCredentialResponse()
        .awsTempCredentials(awsTempCredentials)
        .azureUserDelegationSas(azureUserDelegationSas)
        .gcpOauthToken(gcpOauthToken)
        .expirationTime(expirationTime);
  }

  public GenerateTemporaryVolumeCredentialResponse toVolumeCredentialResponse() {
    return new GenerateTemporaryVolumeCredentialResponse()
        .awsTempCredentials(awsTempCredentials)
        .azureUserDelegationSas(azureUserDelegationSas)
        .gcpOauthToken(gcpOauthToken)
        .expirationTime(expirationTime);
  }

  public GenerateTemporaryPathCredentialResponse toPathCredentialResponse() {
    return new GenerateTemporaryPathCredentialResponse()
        .awsTempCredentials(awsTempCredentials)
        .azureUserDelegationSas(azureUserDelegationSas)
        .gcpOauthToken(gcpOauthToken)
        .expirationTime(expirationTime);
  }
}
