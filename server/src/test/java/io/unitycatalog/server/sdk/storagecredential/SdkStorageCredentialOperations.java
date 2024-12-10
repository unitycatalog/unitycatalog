package io.unitycatalog.server.sdk.storagecredential;

import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.StorageCredentialsApi;
import io.unitycatalog.client.model.CreateStorageCredential;
import io.unitycatalog.client.model.StorageCredentialInfo;
import io.unitycatalog.client.model.UpdateStorageCredential;
import io.unitycatalog.server.base.storagecredential.StorageCredentialOperations;
import java.util.List;
import java.util.Optional;

public class SdkStorageCredentialOperations implements StorageCredentialOperations {
  private final StorageCredentialsApi storageCredentialsApi;

  public SdkStorageCredentialOperations(ApiClient apiClient) {
    this.storageCredentialsApi = new StorageCredentialsApi(apiClient);
  }

  @Override
  public StorageCredentialInfo createStorageCredential(
      CreateStorageCredential createStorageCredential) throws ApiException {
    return storageCredentialsApi.createStorageCredential(createStorageCredential);
  }

  @Override
  public List<StorageCredentialInfo> listStorageCredentials(Optional<String> pageToken)
      throws ApiException {
    return storageCredentialsApi
        .listStorageCredentials(100, pageToken.orElse(null))
        .getStorageCredentials();
  }

  @Override
  public StorageCredentialInfo getStorageCredential(String name) throws ApiException {
    return storageCredentialsApi.getStorageCredential(name);
  }

  @Override
  public StorageCredentialInfo updateStorageCredential(
      String name, UpdateStorageCredential updateStorageCredential) throws ApiException {
    return storageCredentialsApi.updateStorageCredential(name, updateStorageCredential);
  }

  @Override
  public void deleteStorageCredential(String name) throws ApiException {
    storageCredentialsApi.deleteStorageCredential(name, true);
  }
}
