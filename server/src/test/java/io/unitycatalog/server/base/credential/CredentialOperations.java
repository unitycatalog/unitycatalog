package io.unitycatalog.server.base.credential;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.CreateCredentialRequest;
import io.unitycatalog.client.model.CredentialInfo;
import io.unitycatalog.client.model.CredentialPurpose;
import io.unitycatalog.client.model.UpdateCredentialRequest;
import java.util.List;
import java.util.Optional;

public interface CredentialOperations {
  CredentialInfo createCredential(CreateCredentialRequest createCredentialRequest)
      throws ApiException;

  List<CredentialInfo> listCredentials(Optional<String> pageToken, CredentialPurpose purpose)
      throws ApiException;

  CredentialInfo getCredential(String name) throws ApiException;

  CredentialInfo updateCredential(String name, UpdateCredentialRequest updateCredentialRequest)
      throws ApiException;

  void deleteCredential(String name) throws ApiException;
}
