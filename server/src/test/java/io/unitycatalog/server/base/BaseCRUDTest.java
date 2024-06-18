package io.unitycatalog.server.base;

import io.unitycatalog.server.base.catalog.CatalogOperations;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Optional;

import static io.unitycatalog.server.utils.TestUtils.CATALOG_NAME;
import static io.unitycatalog.server.utils.TestUtils.CATALOG_NEW_NAME;

@RunWith(Parameterized.class)
public abstract class BaseCRUDTest extends BaseServerTest {

    protected CatalogOperations catalogOperations;

    protected void cleanUp() {
        try {
            if (catalogOperations.getCatalog(CATALOG_NAME) != null) {
                catalogOperations.deleteCatalog(CATALOG_NAME, Optional.of(true));
            }
        } catch (Exception e) {
            // Ignore
        }
        try {
            if (catalogOperations.getCatalog(CATALOG_NEW_NAME) != null) {
                catalogOperations.deleteCatalog(CATALOG_NEW_NAME, Optional.of(true));
            }
        } catch (Exception e) {
            // Ignore
        }
    }

    @Before
    public void setUp() {
        super.setUp();
        catalogOperations = createCatalogOperations(serverConfig);
    }

    protected abstract CatalogOperations createCatalogOperations(ServerConfig serverConfig);
}
