package io.unitycatalog.server.base.credentials;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.GenerateTemporaryModelVersionCredential;
import io.unitycatalog.client.model.TemporaryCredentials;

public interface TemporaryCredentialsOperations {
  TemporaryCredentials generateTemporaryModelVersionCredentials(
      GenerateTemporaryModelVersionCredential generateTemporaryModelVersionCredentials)
      throws ApiException;
}
