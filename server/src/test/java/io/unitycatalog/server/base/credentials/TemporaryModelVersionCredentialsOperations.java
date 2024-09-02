package io.unitycatalog.server.base.credentials;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.GenerateTemporaryModelVersionCredentials;
import io.unitycatalog.client.model.GenerateTemporaryModelVersionCredentialsResponse;

public interface TemporaryModelVersionCredentialsOperations {
  GenerateTemporaryModelVersionCredentialsResponse generateTemporaryModelVersionCredentials(
      GenerateTemporaryModelVersionCredentials generateTemporaryModelVersionCredentials)
      throws ApiException;
}
