package io.unitycatalog.server.sdk.metastore;

import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.catalog.CatalogOperations;
import io.unitycatalog.server.base.metastore.BaseMetastoreSummaryTest;
import io.unitycatalog.server.base.metastore.MetastoreOperations;
import io.unitycatalog.server.sdk.catalog.SdkCatalogOperations;
import io.unitycatalog.server.utils.TestUtils;

public class SdkMetastoreSummaryTest extends BaseMetastoreSummaryTest {
  @Override
  protected CatalogOperations createCatalogOperations(ServerConfig serverConfig) {
    return new SdkCatalogOperations(TestUtils.createApiClient(serverConfig));
  }

  @Override
  protected MetastoreOperations createMetastoreOperations(ServerConfig serverConfig) {
    return new SdkMetastoreOperations(TestUtils.createApiClient(serverConfig));
  }
}
