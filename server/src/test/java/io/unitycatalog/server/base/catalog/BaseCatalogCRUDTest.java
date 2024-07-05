package io.unitycatalog.server.base.catalog;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.CreateCatalog;
import io.unitycatalog.client.model.UpdateCatalog;
import io.unitycatalog.server.base.BaseCRUDTest;
import org.junit.*;

import io.unitycatalog.client.model.CatalogInfo;

import java.util.List;
import java.util.Objects;

import static io.unitycatalog.server.utils.TestUtils.*;

public abstract class BaseCatalogCRUDTest extends BaseCRUDTest {

    protected void assertCatalog(CatalogInfo catalogInfo, String name, String comment) {
        Assert.assertEquals(name, catalogInfo.getName());
        Assert.assertEquals(comment, catalogInfo.getComment());
        Assert.assertNotNull(catalogInfo.getCreatedAt());
        // TODO: Also assert properties once CLI supports it
    }

    protected void assertCatalogExists(List<CatalogInfo> catalogList, String name, String comment) {
        Assert.assertTrue(catalogList.stream().anyMatch(c ->
                Objects.equals(c.getName(), name) && Objects.equals(c.getComment(), comment)));
    }

    protected void assertCatalogNotExists(List<CatalogInfo> catalogList, String name) {
        Assert.assertFalse(catalogList.stream().anyMatch(c ->
                Objects.equals(c.getName(), name)));
    }

    @Test
    public void testCatalogCRUD() throws ApiException {
        // Create a catalog
        System.out.println("Testing create catalog..");
        CreateCatalog createCatalog = new CreateCatalog()
                .name(CATALOG_NAME)
                .comment(COMMENT)
                .properties(PROPERTIES);
        CatalogInfo catalogInfo = catalogOperations.createCatalog(createCatalog);
        assertCatalog(catalogInfo, CATALOG_NAME, COMMENT);

        // List catalogs
        System.out.println("Testing list catalogs..");
        List<CatalogInfo> catalogList = catalogOperations.listCatalogs();
        Assert.assertNotNull(catalogList);
        assertCatalogExists(catalogList, CATALOG_NAME, COMMENT);

        // Get catalog
        System.out.println("Testing get catalog..");
        CatalogInfo catalogInfo2 = catalogOperations.getCatalog(CATALOG_NAME);
        Assert.assertEquals(catalogInfo, catalogInfo2);

        // Update catalog
        System.out.println("Testing update catalog..");
        UpdateCatalog updateCatalog = new UpdateCatalog().newName(CATALOG_NEW_NAME).comment(CATALOG_NEW_COMMENT);
        CatalogInfo updatedCatalogInfo = catalogOperations.updateCatalog(CATALOG_NAME, updateCatalog);
        assertCatalog(updatedCatalogInfo, CATALOG_NEW_NAME, CATALOG_NEW_COMMENT);

        // Delete catalog
        System.out.println("Testing delete catalog..");
        catalogOperations.deleteCatalog(CATALOG_NEW_NAME);
        catalogList = catalogOperations.listCatalogs();
        Assert.assertNotNull(catalogList);
        assertCatalogNotExists(catalogList, CATALOG_NEW_NAME);
    }
}