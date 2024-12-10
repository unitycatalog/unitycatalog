package io.unitycatalog.server.sdk.externallocation;

import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.catalog.CatalogOperations;
import io.unitycatalog.server.base.externallocation.BaseExternalLocationCRUDTest;
import io.unitycatalog.server.base.externallocation.ExternalLocationOperations;
import io.unitycatalog.server.base.storagecredential.StorageCredentialOperations;
import io.unitycatalog.server.sdk.catalog.SdkCatalogOperations;
import io.unitycatalog.server.sdk.storagecredential.SdkStorageCredentialOperations;
import io.unitycatalog.server.utils.TestUtils;

public class SdkExternalLocationCRUDTest extends BaseExternalLocationCRUDTest {
  @Override
  protected CatalogOperations createCatalogOperations(ServerConfig serverConfig) {
    return new SdkCatalogOperations(TestUtils.createApiClient(serverConfig));
  }

  @Override
  protected ExternalLocationOperations createExternalLocationOperations(ServerConfig config) {
    return new SdkExternalLocationOperations(TestUtils.createApiClient(serverConfig));
  }

  @Override
  protected StorageCredentialOperations createStorageCredentialOperations(ServerConfig config) {
    return new SdkStorageCredentialOperations(TestUtils.createApiClient(serverConfig));
  }
}
