package io.unitycatalog.server.base.catalog;

import static io.unitycatalog.server.utils.TestUtils.CATALOG_NAME;
import static io.unitycatalog.server.utils.TestUtils.CATALOG_NEW_COMMENT;
import static io.unitycatalog.server.utils.TestUtils.CATALOG_NEW_NAME;
import static io.unitycatalog.server.utils.TestUtils.COMMENT;
import static io.unitycatalog.server.utils.TestUtils.COMMON_ENTITY_NAME;
import static io.unitycatalog.server.utils.TestUtils.NEW_PROPERTIES;
import static io.unitycatalog.server.utils.TestUtils.PROPERTIES;
import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.CatalogInfo;
import io.unitycatalog.client.model.CreateCatalog;
import io.unitycatalog.client.model.UpdateCatalog;
import io.unitycatalog.server.base.BaseCRUDTest;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.persist.dao.CatalogInfoDAO;
import io.unitycatalog.server.persist.dao.PropertyDAO;
import io.unitycatalog.server.utils.Constants;
import io.unitycatalog.server.utils.TestUtils;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.hibernate.LockMode;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.junit.jupiter.api.Test;

public abstract class BaseCatalogCRUDTest extends BaseCRUDTest {

  protected void assertCatalog(
      CatalogInfo catalogInfo, String name, String comment, Map<String, String> properties) {
    assertThat(catalogInfo.getName()).isEqualTo(name);
    assertThat(catalogInfo.getComment()).isEqualTo(comment);
    assertThat(catalogInfo.getProperties()).isEqualTo(properties);
    assertThat(catalogInfo.getCreatedAt()).isNotNull();
  }

  protected void assertCatalogExists(
      List<CatalogInfo> catalogList, String name, String comment, Map<String, String> properties) {
    assertThat(catalogList)
        .anyMatch(
            c ->
                Objects.equals(c.getName(), name)
                    && Objects.equals(c.getComment(), comment)
                    && Objects.equals(c.getProperties(), properties));
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
    assertCatalog(catalogInfo, CATALOG_NAME, COMMENT, PROPERTIES);

    // Create another catalog to test pagination
    CreateCatalog createCatalog2 =
        new CreateCatalog().name(COMMON_ENTITY_NAME).comment(COMMENT).properties(PROPERTIES);
    CatalogInfo catalogInfo2 = catalogOperations.createCatalog(createCatalog2);
    assertCatalog(catalogInfo2, COMMON_ENTITY_NAME, COMMENT, PROPERTIES);

    // List catalogs
    System.out.println("Testing list catalogs..");
    List<CatalogInfo> catalogList = catalogOperations.listCatalogs(Optional.empty());
    assertThat(catalogList).isNotNull();
    assertCatalogExists(catalogList, CATALOG_NAME, COMMENT, PROPERTIES);
    assertCatalogExists(catalogList, COMMON_ENTITY_NAME, COMMENT, PROPERTIES);

    // List catalogs with page token
    System.out.println("Testing list catalogs with page token..");
    catalogList = catalogOperations.listCatalogs(Optional.of(CATALOG_NAME));
    assertThat(catalogList).isNotNull();
    assertCatalogNotExists(catalogList, CATALOG_NAME);
    assertCatalogExists(catalogList, COMMON_ENTITY_NAME, COMMENT, PROPERTIES);

    // Get catalog
    System.out.println("Testing get catalog..");
    CatalogInfo catalogInfo3 = catalogOperations.getCatalog(CATALOG_NAME);
    assertThat(catalogInfo3).isEqualTo(catalogInfo);

    // Calling update catalog with nothing to update should not change anything
    System.out.println("Testing updating catalog with nothing to update..");
    UpdateCatalog emptyUpdateCatalog = new UpdateCatalog();
    catalogOperations.updateCatalog(CATALOG_NAME, emptyUpdateCatalog);
    CatalogInfo catalogInfo4 = catalogOperations.getCatalog(CATALOG_NAME);
    assertThat(catalogInfo4).isEqualTo(catalogInfo);

    // Update catalog name without updating comment and properties
    System.out.println("Testing update catalog: changing name..");
    UpdateCatalog updateCatalog = new UpdateCatalog().newName(CATALOG_NEW_NAME);
    CatalogInfo updatedCatalogInfo = catalogOperations.updateCatalog(CATALOG_NAME, updateCatalog);
    assertCatalog(updatedCatalogInfo, CATALOG_NEW_NAME, COMMENT, PROPERTIES);

    // Update catalog comment without updating name and properties
    System.out.println("Testing update catalog: changing comment..");
    UpdateCatalog updateCatalog2 = new UpdateCatalog().comment(CATALOG_NEW_COMMENT);
    CatalogInfo updatedCatalogInfo2 =
        catalogOperations.updateCatalog(CATALOG_NEW_NAME, updateCatalog2);
    assertCatalog(updatedCatalogInfo2, CATALOG_NEW_NAME, CATALOG_NEW_COMMENT, PROPERTIES);

    // Update catalog properties without updating name and comment
    System.out.println("Testing update catalog: changing properties..");
    UpdateCatalog updateCatalog3 = new UpdateCatalog().properties(NEW_PROPERTIES);
    CatalogInfo updatedCatalogInfo3 =
        catalogOperations.updateCatalog(CATALOG_NEW_NAME, updateCatalog3);
    assertCatalog(updatedCatalogInfo3, CATALOG_NEW_NAME, CATALOG_NEW_COMMENT, NEW_PROPERTIES);

    // Delete catalog
    System.out.println("Testing delete catalog..");
    catalogOperations.deleteCatalog(CATALOG_NEW_NAME, Optional.of(false));
    catalogOperations.deleteCatalog(COMMON_ENTITY_NAME, Optional.of(false));
    catalogList = catalogOperations.listCatalogs(Optional.empty());
    assertThat(catalogList).isNotNull();
    assertCatalogNotExists(catalogList, CATALOG_NEW_NAME);
    assertCatalogNotExists(catalogList, COMMON_ENTITY_NAME);
  }

  /** A drop that waits for a concurrent rename must not delete the renamed catalog. */
  @Test
  public void dropRacingARenameDoesNotDeleteTheRenamedCatalog() throws Exception {
    UUID id = UUID.fromString(catalogOperations.createCatalog(newCatalog("drop_race")).getId());

    TestUtils.assertApiException(
        () ->
            runBlockedByHeldWrite(
                held -> setColumn(held, CatalogInfoDAO.class, id, "name", "drop_race_renamed"),
                () -> {
                  catalogOperations.deleteCatalog("drop_race", Optional.empty());
                  return null;
                }),
        ErrorCode.CATALOG_NOT_FOUND,
        "Catalog not found");

    assertThat(catalogOperations.getCatalog("drop_race_renamed").getId()).isEqualTo(id.toString());
  }

  /** A rename that waits for a concurrent edit of another column keeps that edit. */
  @Test
  public void renameKeepsAConcurrentCommentEdit() throws Exception {
    UUID id = UUID.fromString(catalogOperations.createCatalog(newCatalog("edit_race")).getId());

    runBlockedByHeldWrite(
        held -> setColumn(held, CatalogInfoDAO.class, id, "comment", "held comment"),
        () -> catalogOperations.updateCatalog("edit_race", new UpdateCatalog().newName("edited")));

    CatalogInfo renamed = catalogOperations.getCatalog("edited");
    assertThat(renamed.getId()).isEqualTo(id.toString());
    assertThat(renamed.getComment()).isEqualTo("held comment");
  }

  /**
   * A properties update that waits for a concurrent one replaces the committed properties, not the
   * ones it saw before the wait (stale under MySQL's REPEATABLE READ).
   */
  @Test
  public void propertiesUpdateReplacesConcurrentlyCommittedProperties() throws Exception {
    CreateCatalog create = newCatalog("props_race").properties(Map.of("a", "1"));
    UUID id = UUID.fromString(catalogOperations.createCatalog(create).getId());

    runBlockedByHeldWrite(
        held -> {
          // Like an update in flight: it holds the catalog row and swaps property "a" for "b".
          setColumn(held, CatalogInfoDAO.class, id, "updatedBy", "held");
          held.createMutationQuery(
                  "delete from PropertyDAO p where p.entityId = :id and p.key = 'a'")
              .setParameter("id", id)
              .executeUpdate();
          PropertyDAO.from(Map.of("b", "2"), id, Constants.CATALOG).forEach(held::persist);
        },
        () ->
            catalogOperations.updateCatalog(
                "props_race", new UpdateCatalog().properties(Map.of("c", "3"))));

    assertThat(catalogOperations.getCatalog("props_race").getProperties())
        .isEqualTo(Map.of("c", "3"));
  }

  /**
   * A properties update must not hold a range lock on the property table. On MySQL's REPEATABLE
   * READ a locking read that finds no rows gap-locks the whole (shared) empty range, so two such
   * transactions on different property-less catalogs each wait for the other's gap lock on their
   * insert and one is a deadlock victim. The held side stands in for the other transaction.
   */
  @Test
  public void propertiesUpdatesOfPropertylessCatalogsDoNotDeadlock() throws Exception {
    UUID first = UUID.fromString(catalogOperations.createCatalog(newCatalog("gap_a")).getId());
    catalogOperations.createCatalog(newCatalog("gap_b"));
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try (Session held = testDatabase.sessionFactory().openSession()) {
      Transaction transaction = held.beginTransaction();
      held.createQuery(
              "FROM PropertyDAO p WHERE p.entityId = :id and p.entityType = :type",
              PropertyDAO.class)
          .setParameter("id", first)
          .setParameter("type", Constants.CATALOG)
          .setHibernateLockMode(LockMode.PESSIMISTIC_WRITE)
          .list();
      Future<CatalogInfo> update =
          executor.submit(
              () ->
                  catalogOperations.updateCatalog(
                      "gap_b", new UpdateCatalog().properties(Map.of("k", "v"))));
      // On MySQL the update now waits for the held side's gap lock; elsewhere it just finishes.
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
      while (!testDatabase.hasLockWaiter() && !update.isDone() && System.nanoTime() < deadline) {
        Thread.sleep(100);
      }
      PropertyDAO.from(Map.of("k", "v"), first, Constants.CATALOG).forEach(held::persist);
      held.flush();
      transaction.commit();

      assertThat(update.get(30, TimeUnit.SECONDS).getProperties()).isEqualTo(Map.of("k", "v"));
      assertThat(catalogOperations.getCatalog("gap_a").getProperties()).isEqualTo(Map.of("k", "v"));
    } finally {
      executor.shutdownNow();
    }
  }

  private static CreateCatalog newCatalog(String name) {
    return new CreateCatalog().name(name).comment(COMMENT);
  }

  /** An update that waits for a concurrent rename of the catalog misses it and changes nothing. */
  @Test
  public void updateRacingARenameIsNotFound() throws Exception {
    UUID id = UUID.fromString(catalogOperations.createCatalog(newCatalog("update_race")).getId());

    TestUtils.assertApiException(
        () ->
            runBlockedByHeldWrite(
                held -> setColumn(held, CatalogInfoDAO.class, id, "name", "update_race_renamed"),
                () ->
                    catalogOperations.updateCatalog(
                        "update_race", new UpdateCatalog().comment("lost"))),
        ErrorCode.CATALOG_NOT_FOUND,
        "Catalog not found");

    CatalogInfo renamed = catalogOperations.getCatalog("update_race_renamed");
    assertThat(renamed.getId()).isEqualTo(id.toString());
    assertThat(renamed.getComment()).isNotEqualTo("lost");
  }
}
