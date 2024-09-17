package io.unitycatalog.server.sdk.credentials;

import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.TemporaryCredentialsApi;
import io.unitycatalog.client.model.*;
import io.unitycatalog.server.base.credentials.TemporaryCredentialsOperations;

public class SdkTemporaryCredentialsOperations implements TemporaryCredentialsOperations {
  private final TemporaryCredentialsApi temporaryCredentialsApi;

  public SdkTemporaryCredentialsOperations(ApiClient apiClient) {
    this.temporaryCredentialsApi = new TemporaryCredentialsApi(apiClient);
  }

  @Override
  public TemporaryCredentials generateTemporaryModelVersionCredentials(
      GenerateTemporaryModelVersionCredential generateTemporaryModelVersionCredentials)
      throws ApiException {
    return temporaryCredentialsApi.generateTemporaryModelVersionCredentials(
        generateTemporaryModelVersionCredentials);
  }
}
