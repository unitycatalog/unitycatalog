package io.unitycatalog.server.service.credential;

import io.unitycatalog.server.model.AwsCredentials;
import io.unitycatalog.server.model.AzureUserDelegationSAS;
import io.unitycatalog.server.model.GcpOauthToken;
import io.unitycatalog.server.model.GenerateTemporaryTableCredentialResponse;
import io.unitycatalog.server.model.GenerateTemporaryVolumeCredentialResponse;
import lombok.Builder;
import lombok.Getter;

@Builder
@Getter
public class CredentialResponse {
  private AwsCredentials awsTempCredentials;
  private AzureUserDelegationSAS azureUserDelegationSas;
  private GcpOauthToken gcpOauthToken;
  private Long expirationTime;

  public GenerateTemporaryTableCredentialResponse toTableCredentialResponse() {
    return new GenerateTemporaryTableCredentialResponse().awsTempCredentials(awsTempCredentials)
      .azureUserDelegationSas(azureUserDelegationSas).gcpOauthToken(gcpOauthToken).expirationTime(expirationTime);
  }

  public GenerateTemporaryVolumeCredentialResponse toVolumeCredentialResponse() {
    return new GenerateTemporaryVolumeCredentialResponse().awsTempCredentials(awsTempCredentials)
      .azureUserDelegationSas(azureUserDelegationSas).gcpOauthToken(gcpOauthToken).expirationTime(expirationTime);
  }
}
