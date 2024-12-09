package io.unitycatalog.cli.metastore;

import io.unitycatalog.cli.catalog.CliCatalogOperations;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.catalog.CatalogOperations;
import io.unitycatalog.server.base.metastore.BaseMetastoreSummaryTest;
import io.unitycatalog.server.base.metastore.MetastoreOperations;

public class CliMetastoreSummaryTest extends BaseMetastoreSummaryTest {
  @Override
  protected CatalogOperations createCatalogOperations(ServerConfig serverConfig) {
    return new CliCatalogOperations(serverConfig);
  }

  @Override
  protected MetastoreOperations createMetastoreOperations(ServerConfig serverConfig) {
    return new CliMetastoreOperations(serverConfig);
  }
}
