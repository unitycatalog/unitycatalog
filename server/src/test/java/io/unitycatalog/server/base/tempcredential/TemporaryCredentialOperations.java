package io.unitycatalog.server.base.tempcredential;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.*;

public interface TemporaryCredentialOperations {
  TemporaryCredentials generateTemporaryModelVersionCredentials(
      GenerateTemporaryModelVersionCredential generateTemporaryModelVersionCredentials)
      throws ApiException;

  TemporaryCredentials generateTemporaryTableCredentials(
      GenerateTemporaryTableCredential generateTemporaryTableCredential) throws ApiException;

  TemporaryCredentials generateTemporaryPathCredentials(
      GenerateTemporaryPathCredential generateTemporaryPathCredential) throws ApiException;

  TemporaryCredentials generateTemporaryVolumeCredentials(
      GenerateTemporaryVolumeCredential generateTemporaryVolumeCredential) throws ApiException;
}
