package io.unitycatalog.cli.catalog;

import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.catalog.BaseCatalogCRUDTest;
import io.unitycatalog.server.base.catalog.CatalogOperations;

public class CliCatalogCRUDTest extends BaseCatalogCRUDTest {

  @Override
  protected CatalogOperations createCatalogOperations(ServerConfig config) {
    return new CliCatalogOperations(config);
  }
}
