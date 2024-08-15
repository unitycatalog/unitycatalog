package io.unitycatalog.server.base.catalog;

import static io.unitycatalog.server.utils.TestUtils.*;
import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.CatalogInfo;
import io.unitycatalog.client.model.CreateCatalog;
import io.unitycatalog.client.model.UpdateCatalog;
import io.unitycatalog.server.base.BaseCRUDTest;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.junit.jupiter.api.Test;

public abstract class BaseCatalogCRUDTest extends BaseCRUDTest {

  protected void assertCatalog(CatalogInfo catalogInfo, String name, String comment) {
    assertThat(catalogInfo.getName()).isEqualTo(name);
    assertThat(catalogInfo.getComment()).isEqualTo(comment);
    assertThat(catalogInfo.getCreatedAt()).isNotNull();
    // TODO: Also assert properties once CLI supports it
  }

  protected void assertCatalogExists(List<CatalogInfo> catalogList, String name, String comment) {
    assertThat(catalogList)
        .anyMatch(
            c -> Objects.equals(c.getName(), name) && Objects.equals(c.getComment(), comment));
  }

  protected void assertCatalogNotExists(List<CatalogInfo> catalogList, String name) {
    assertThat(catalogList).noneMatch(c -> Objects.equals(c.getName(), name));
  }

  @Test
  public void testCatalogCRUD() throws ApiException {
    // Create a catalog
    System.out.println("Testing create catalog..");
    CreateCatalog createCatalog =
        new CreateCatalog().name(CATALOG_NAME).comment(COMMENT).properties(PROPERTIES);
    CatalogInfo catalogInfo = catalogOperations.createCatalog(createCatalog);
    assertCatalog(catalogInfo, CATALOG_NAME, COMMENT);

    // List catalogs
    System.out.println("Testing list catalogs..");
    List<CatalogInfo> catalogList = catalogOperations.listCatalogs();
    assertThat(catalogList).isNotNull();
    assertCatalogExists(catalogList, CATALOG_NAME, COMMENT);

    // Get catalog
    System.out.println("Testing get catalog..");
    CatalogInfo catalogInfo2 = catalogOperations.getCatalog(CATALOG_NAME);
    assertThat(catalogInfo2).isEqualTo(catalogInfo);

    // Calling update catalog with nothing to update should not change anything
    System.out.println("Testing updating catalog with nothing to update..");
    UpdateCatalog emptyUpdateCatalog = new UpdateCatalog();
    CatalogInfo emptyUpdateCatalogInfo =
        catalogOperations.updateCatalog(CATALOG_NAME, emptyUpdateCatalog);
    CatalogInfo catalogInfo3 = catalogOperations.getCatalog(CATALOG_NAME);
    assertThat(catalogInfo3).isEqualTo(catalogInfo);

    // Update catalog name without updating comment
    System.out.println("Testing update catalog: changing name..");
    UpdateCatalog updateCatalog = new UpdateCatalog().newName(CATALOG_NEW_NAME);
    CatalogInfo updatedCatalogInfo = catalogOperations.updateCatalog(CATALOG_NAME, updateCatalog);
    assertCatalog(updatedCatalogInfo, CATALOG_NEW_NAME, COMMENT);

    // Update catalog comment without updating name
    System.out.println("Testing update catalog: changing comment..");
    UpdateCatalog updateCatalog2 = new UpdateCatalog().comment(CATALOG_NEW_COMMENT);
    CatalogInfo updatedCatalogInfo2 =
        catalogOperations.updateCatalog(CATALOG_NEW_NAME, updateCatalog2);
    assertCatalog(updatedCatalogInfo2, CATALOG_NEW_NAME, CATALOG_NEW_COMMENT);

    // Delete catalog
    System.out.println("Testing delete catalog..");
    catalogOperations.deleteCatalog(CATALOG_NEW_NAME, Optional.of(false));
    catalogList = catalogOperations.listCatalogs();
    assertThat(catalogList).isNotNull();
    assertCatalogNotExists(catalogList, CATALOG_NEW_NAME);
  }
}
