package io.unitycatalog.server.base;

import io.unitycatalog.server.base.catalog.CatalogOperations;
import org.junit.jupiter.api.BeforeEach;

import static io.unitycatalog.server.utils.TestUtils.CATALOG_NAME;
import static io.unitycatalog.server.utils.TestUtils.CATALOG_NEW_NAME;

public abstract class BaseCRUDTest extends BaseServerTest {

    protected CatalogOperations catalogOperations;

    protected void cleanUp() {
        try {
            if (catalogOperations.getCatalog(CATALOG_NAME) != null) {
                catalogOperations.deleteCatalog(CATALOG_NAME);
            }
        } catch (Exception e) {
            // Ignore
        }
        try {
            if (catalogOperations.getCatalog(CATALOG_NEW_NAME) != null) {
                catalogOperations.deleteCatalog(CATALOG_NEW_NAME);
            }
        } catch (Exception e) {
            // Ignore
        }
    }

    @BeforeEach
    public void setUp() {
        super.setUp();
        catalogOperations = createCatalogOperations(serverConfig);
    }

    protected abstract CatalogOperations createCatalogOperations(ServerConfig serverConfig);
}
