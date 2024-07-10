package io.unitycatalog.server.sdk.catalog;

import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.catalog.BaseCatalogCRUDTest;
import io.unitycatalog.server.base.catalog.CatalogOperations;
import io.unitycatalog.server.utils.TestUtils;

public class SdkCatalogCRUDTest extends BaseCatalogCRUDTest {

  @Override
  protected CatalogOperations createCatalogOperations(ServerConfig config) {
    return new SdkCatalogOperations(TestUtils.createApiClient(config));
  }
}
