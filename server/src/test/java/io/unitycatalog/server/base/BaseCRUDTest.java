package io.unitycatalog.server.base;

import io.unitycatalog.server.base.catalog.CatalogOperations;
import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static io.unitycatalog.server.utils.TestUtils.CATALOG_NAME;
import static io.unitycatalog.server.utils.TestUtils.CATALOG_NEW_NAME;

@RunWith(Parameterized.class)
public abstract class BaseCRUDTest extends BaseServerTest {

    protected CatalogOperations catalogOperations;

    @After
    public void cleanUp() {
        try {
            catalogOperations.deleteCatalog(CATALOG_NAME);
        } catch (Exception e) {
            // Ignore
        }
        try {
            catalogOperations.deleteCatalog(CATALOG_NEW_NAME);
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
