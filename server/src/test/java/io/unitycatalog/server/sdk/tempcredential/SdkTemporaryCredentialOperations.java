package io.unitycatalog.server.sdk.tempcredential;

import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.TemporaryCredentialsApi;
import io.unitycatalog.client.model.*;
import io.unitycatalog.server.base.tempcredential.TemporaryCredentialOperations;

public class SdkTemporaryCredentialOperations implements TemporaryCredentialOperations {
  private final TemporaryCredentialsApi temporaryCredentialsApi;

  public SdkTemporaryCredentialOperations(ApiClient apiClient) {
    this.temporaryCredentialsApi = new TemporaryCredentialsApi(apiClient);
  }

  @Override
  public TemporaryCredentials generateTemporaryModelVersionCredentials(
      GenerateTemporaryModelVersionCredential generateTemporaryModelVersionCredentials)
      throws ApiException {
    return temporaryCredentialsApi.generateTemporaryModelVersionCredentials(
        generateTemporaryModelVersionCredentials);
  }

  @Override
  public TemporaryCredentials generateTemporaryTableCredentials(
      GenerateTemporaryTableCredential generateTemporaryTableCredential) throws ApiException {
    return temporaryCredentialsApi.generateTemporaryTableCredentials(
        generateTemporaryTableCredential);
  }

  @Override
  public TemporaryCredentials generateTemporaryPathCredentials(
      GenerateTemporaryPathCredential generateTemporaryPathCredential) throws ApiException {
    return temporaryCredentialsApi.generateTemporaryPathCredentials(
        generateTemporaryPathCredential);
  }

  @Override
  public TemporaryCredentials generateTemporaryVolumeCredentials(
      GenerateTemporaryVolumeCredential generateTemporaryVolumeCredential) throws ApiException {
    return temporaryCredentialsApi.generateTemporaryVolumeCredentials(
        generateTemporaryVolumeCredential);
  }
}
