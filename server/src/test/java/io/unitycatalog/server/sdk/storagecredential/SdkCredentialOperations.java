package io.unitycatalog.server.sdk.storagecredential;

import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.CredentialsApi;
import io.unitycatalog.client.model.CreateCredentialRequest;
import io.unitycatalog.client.model.CredentialInfo;
import io.unitycatalog.client.model.CredentialPurpose;
import io.unitycatalog.client.model.UpdateCredentialRequest;
import io.unitycatalog.server.base.credential.CredentialOperations;
import java.util.List;
import java.util.Optional;

public class SdkCredentialOperations implements CredentialOperations {
  private final CredentialsApi credentialsApi;

  public SdkCredentialOperations(ApiClient apiClient) {
    this.credentialsApi = new CredentialsApi(apiClient);
  }

  public CredentialInfo createCredential(CreateCredentialRequest createCredentialRequest)
      throws ApiException {
    return credentialsApi.createCredential(createCredentialRequest);
  }

  @Override
  public List<CredentialInfo> listCredentials(Optional<String> pageToken, CredentialPurpose purpose)
      throws ApiException {
    return credentialsApi.listCredentials(100, pageToken.orElse(null), purpose).getCredentials();
  }

  @Override
  public CredentialInfo getCredential(String name) throws ApiException {
    return credentialsApi.getCredential(name);
  }

  @Override
  public CredentialInfo updateCredential(
      String name, UpdateCredentialRequest updateCredentialRequest) throws ApiException {
    return credentialsApi.updateCredential(name, updateCredentialRequest);
  }

  @Override
  public void deleteCredential(String name) throws ApiException {
    credentialsApi.deleteCredential(name, true);
  }
}
