package io.unitycatalog.server.sdk.credentials;

import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.TemporaryModelVersionCredentialsApi;
import io.unitycatalog.client.model.*;
import io.unitycatalog.server.base.credentials.TemporaryModelVersionCredentialsOperations;

public class SdkTemporaryModelVersionCredentialsOperations
    implements TemporaryModelVersionCredentialsOperations {
  private final TemporaryModelVersionCredentialsApi temporaryModelVersionCredentialsApi;

  public SdkTemporaryModelVersionCredentialsOperations(ApiClient apiClient) {
    this.temporaryModelVersionCredentialsApi = new TemporaryModelVersionCredentialsApi(apiClient);
  }

  @Override
  public GenerateTemporaryModelVersionCredentialsResponse generateTemporaryModelVersionCredentials(
      GenerateTemporaryModelVersionCredentials generateTemporaryModelVersionCredentials)
      throws ApiException {
    return temporaryModelVersionCredentialsApi.generateTemporaryModelVersionCredentials(
        generateTemporaryModelVersionCredentials);
  }
}
