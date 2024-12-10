package io.unitycatalog.server.sdk.storagecredential;

import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.catalog.CatalogOperations;
import io.unitycatalog.server.base.storagecredential.BaseStorageCredentialCRUDTest;
import io.unitycatalog.server.base.storagecredential.StorageCredentialOperations;
import io.unitycatalog.server.sdk.catalog.SdkCatalogOperations;
import io.unitycatalog.server.utils.TestUtils;

public class SdkStorageCredentialCRUDTest extends BaseStorageCredentialCRUDTest {
  @Override
  protected CatalogOperations createCatalogOperations(ServerConfig serverConfig) {
    return new SdkCatalogOperations(TestUtils.createApiClient(serverConfig));
  }

  @Override
  protected StorageCredentialOperations createStorageCredentialOperations(ServerConfig config) {
    return new SdkStorageCredentialOperations(TestUtils.createApiClient(config));
  }
}
