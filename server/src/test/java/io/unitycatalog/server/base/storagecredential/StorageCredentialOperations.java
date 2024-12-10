package io.unitycatalog.server.base.storagecredential;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.CreateStorageCredential;
import io.unitycatalog.client.model.StorageCredentialInfo;
import io.unitycatalog.client.model.UpdateStorageCredential;
import java.util.List;
import java.util.Optional;

public interface StorageCredentialOperations {
  StorageCredentialInfo createStorageCredential(CreateStorageCredential createStorageCredential)
      throws ApiException;

  List<StorageCredentialInfo> listStorageCredentials(Optional<String> pageToken)
      throws ApiException;

  StorageCredentialInfo getStorageCredential(String name) throws ApiException;

  StorageCredentialInfo updateStorageCredential(
      String name, UpdateStorageCredential updateStorageCredential) throws ApiException;

  void deleteStorageCredential(String name) throws ApiException;
}
