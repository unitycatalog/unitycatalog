package io.unitycatalog.server.base;

import io.unitycatalog.server.base.catalog.CatalogOperations;
import org.junit.jupiter.api.BeforeEach;

public abstract class BaseCRUDTest extends BaseServerTest {

  protected CatalogOperations catalogOperations;

  @BeforeEach
  @Override
  public void setUp() {
    super.setUp();
    catalogOperations = createCatalogOperations(serverConfig);
  }

  protected abstract CatalogOperations createCatalogOperations(ServerConfig serverConfig);
}
