package io.unitycatalog.server.base;

import static io.unitycatalog.server.utils.TestUtils.CATALOG_NAME;
import static io.unitycatalog.server.utils.TestUtils.CATALOG_NEW_NAME;

import io.unitycatalog.server.base.catalog.CatalogOperations;
import java.util.Optional;
import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public abstract class BaseCRUDTest extends BaseServerTest {

  protected CatalogOperations catalogOperations;

  @After
  public void cleanUp() {
    try {
      catalogOperations.deleteCatalog(CATALOG_NAME, Optional.of(true));
    } catch (Exception e) {
      // Ignore
    }
    try {
      catalogOperations.deleteCatalog(CATALOG_NEW_NAME, Optional.of(true));
    } catch (Exception e) {
      // Ignore
    }
  }

  @Before
  @Override
  public void setUp() {
    super.setUp();
    catalogOperations = createCatalogOperations(serverConfig);
  }

  protected abstract CatalogOperations createCatalogOperations(ServerConfig serverConfig);
}
