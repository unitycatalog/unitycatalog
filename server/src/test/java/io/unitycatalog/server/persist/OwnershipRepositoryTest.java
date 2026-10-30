package io.unitycatalog.server.persist;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.util.SafeCloseable;
import com.linecorp.armeria.server.ServiceRequestContext;
import io.unitycatalog.server.auth.CasbinPolicyRefresher;
import io.unitycatalog.server.auth.JCasbinAuthorizer;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.model.UpdateCatalog;
import io.unitycatalog.server.persist.dao.CatalogInfoDAO;
import io.unitycatalog.server.persist.dao.UserDAO;
import io.unitycatalog.server.persist.model.Privileges;
import io.unitycatalog.server.persist.utils.HibernateConfigurator;
import io.unitycatalog.server.service.CatalogService;
import io.unitycatalog.server.testdatabase.TestDatabase;
import io.unitycatalog.server.testdatabase.TestDatabases;
import io.unitycatalog.server.utils.ServerProperties;
import io.unitycatalog.server.utils.ServerProperties.Property;
import java.time.Duration;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class OwnershipRepositoryTest {
  private static final String CATALOG = "ownership_catalog";
  private static final String FIRST_OWNER = "first-owner@localhost";
  private static final String SECOND_OWNER = "second-owner@localhost";
  private static final String THIRD_OWNER = "third-owner@localhost";

  private final UUID resourceId = UUID.randomUUID();
  private final UUID firstId = UUID.randomUUID();
  private final UUID secondId = UUID.randomUUID();
  private final UUID thirdId = UUID.randomUUID();
  private final UUID childId = UUID.randomUUID();
  private TestDatabase database;
  private Properties hibernateProperties;
  private ServerProperties properties;
  private HibernateConfigurator configurator;
  private SessionFactory sessionFactory;
  private Repositories repositories;
  private JCasbinAuthorizer authorizer;
  private SafeCloseable requestContext;

  @BeforeEach
  void setUp() throws Exception {
    requestContext = ServiceRequestContext.of(HttpRequest.of(HttpMethod.PATCH, "/")).push();
    Properties values = new Properties();
    values.setProperty(Property.SERVER_ENV.getKey(), "test");
    values.setProperty(Property.AUTHORIZATION_ENABLED.getKey(), "enable");
    values.setProperty(Property.POLICY_REFRESH_ENABLED.getKey(), "false");
    properties = new ServerProperties(values);
    database = TestDatabases.newDatabase();
    hibernateProperties = database.hibernateProperties();
    // Keep the data when reopening the persistence layer in the restart test.
    hibernateProperties.setProperty("hibernate.hbm2ddl.auto", "update");
    openRepositories();
    sessionFactory.inTransaction(
        session -> {
          List<String> emails = List.of(FIRST_OWNER, SECOND_OWNER, THIRD_OWNER);
          List<UUID> ids = List.of(firstId, secondId, thirdId);
          for (int i = 0; i < emails.size(); i++) {
            session.persist(
                UserDAO.builder()
                    .id(ids.get(i))
                    .name(emails.get(i))
                    .email(emails.get(i))
                    .state("ENABLED")
                    .createdAt(new Date())
                    .build());
          }
          session.persist(
              CatalogInfoDAO.builder()
                  .id(resourceId)
                  .name(CATALOG)
                  .owner(FIRST_OWNER)
                  .comment("original")
                  .createdAt(new Date())
                  .build());
          session.persist(
              CatalogInfoDAO.builder()
                  .id(UUID.randomUUID())
                  .name("occupied")
                  .owner(FIRST_OWNER)
                  .createdAt(new Date())
                  .build());
        });
    authorizer.grantAuthorization(firstId, resourceId, Privileges.OWNER);
    authorizer.grantAuthorization(thirdId, resourceId, Privileges.MANAGE);
    authorizer.addHierarchyChild(resourceId, childId);
  }

  private void openRepositories() throws Exception {
    configurator = database.openHibernateConfigurator(hibernateProperties);
    sessionFactory = configurator.getSessionFactory();
    repositories = new Repositories(sessionFactory, properties);
    authorizer = new JCasbinAuthorizer(configurator, properties);
  }

  @AfterEach
  void tearDown() {
    try {
      if (authorizer != null) {
        authorizer.close();
      }
      if (database != null) {
        database.close();
      }
    } finally {
      requestContext.close();
    }
  }

  @Test
  void failedMetadataUpdateRollsBackOwnerFieldAndPolicy() {
    assertThatThrownBy(
            () ->
                repositories
                    .getCatalogRepository()
                    .updateCatalog(
                        CATALOG, new UpdateCatalog().owner(SECOND_OWNER).newName("occupied")))
        .isInstanceOf(BaseException.class)
        .hasMessageContaining("Catalog already exists");
    authorizer.reloadAuthorizations();
    assertOwnership(FIRST_OWNER, firstId);
    assertThat(repositories.getCatalogRepository().getCatalog(CATALOG).getComment())
        .isEqualTo("original");

    for (String invalidOwner : List.of("", "missing-owner@localhost")) {
      assertThatThrownBy(
              () ->
                  repositories
                      .getCatalogRepository()
                      .updateCatalog(CATALOG, new UpdateCatalog().owner(invalidOwner)))
          .isInstanceOf(BaseException.class);
      authorizer.reloadAuthorizations();
      assertOwnership(FIRST_OWNER, firstId);
    }
  }

  @Test
  void committedOwnershipSurvivesRestart() throws Exception {
    CatalogService service = new CatalogService(authorizer, repositories, properties);
    try (JCasbinAuthorizer other = new JCasbinAuthorizer(configurator, properties);
        CasbinPolicyRefresher refresher =
            new CasbinPolicyRefresher(other::reloadAuthorizations, sessionFactory, Duration.ZERO)) {
      assertThat(refresher.checkAndReload()).isTrue();
      service.updateCatalog(CATALOG, new UpdateCatalog().owner(SECOND_OWNER).comment("changed"));
      assertThat(other.authorize(firstId, resourceId, Privileges.OWNER)).isTrue();
      assertThat(refresher.checkAndReload()).isTrue();
      assertThat(other.authorize(firstId, resourceId, Privileges.OWNER)).isFalse();
      assertThat(other.authorize(secondId, resourceId, Privileges.OWNER)).isTrue();
    }
    assertOwnership(SECOND_OWNER, secondId);
    assertThat(repositories.getCatalogRepository().getCatalog(CATALOG).getComment())
        .isEqualTo("changed");
    authorizer.close();
    configurator.close();
    openRepositories();
    assertOwnership(SECOND_OWNER, secondId);
  }

  @Test
  void failedCacheReloadDeniesOldPolicyUntilRecovery() {
    repositories
        .getCatalogRepository()
        .updateCatalog(CATALOG, new UpdateCatalog().owner(SECOND_OWNER));
    sessionFactory.inTransaction(
        session ->
            session
                .createNativeMutationQuery("ALTER TABLE casbin_rule RENAME TO unavailable_policy")
                .executeUpdate());
    try {
      assertThatThrownBy(authorizer::reloadAuthorizations)
          .isInstanceOfAny(RuntimeException.class, Error.class);
      for (UUID id : List.of(firstId, secondId, thirdId)) {
        assertThat(authorizer.authorize(id, resourceId, Privileges.OWNER)).isFalse();
        assertThat(authorizer.authorize(id, resourceId, Privileges.MANAGE)).isFalse();
      }
      assertThatThrownBy(
              () -> authorizer.grantAuthorization(thirdId, resourceId, Privileges.SELECT))
          .isInstanceOfAny(RuntimeException.class, Error.class);
    } finally {
      sessionFactory.inTransaction(
          session ->
              session
                  .createNativeMutationQuery("ALTER TABLE unavailable_policy RENAME TO casbin_rule")
                  .executeUpdate());
    }
    assertThat(authorizer.refreshAuthorizations(System.nanoTime())).isTrue();
    assertOwnership(SECOND_OWNER, secondId);
    assertThat(authorizer.authorize(thirdId, resourceId, Privileges.SELECT)).isFalse();
  }

  @Test
  void concurrentTransfersLeaveOneOwner() throws Exception {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    CountDownLatch loaded = new CountDownLatch(1);
    CountDownLatch transfer = new CountDownLatch(1);
    try (Session first = sessionFactory.openSession()) {
      Transaction tx = first.beginTransaction();
      CatalogInfoDAO firstCopy = first.get(CatalogInfoDAO.class, resourceId);
      Future<?> second =
          executor.submit(
              () -> {
                try (Session session = sessionFactory.openSession()) {
                  Transaction other = session.beginTransaction();
                  CatalogInfoDAO staleCopy = session.get(CatalogInfoDAO.class, resourceId);
                  loaded.countDown();
                  assertThat(transfer.await(10, TimeUnit.SECONDS)).isTrue();
                  staleCopy.setOwner(
                      repositories
                          .getOwnershipRepository()
                          .updateOwner(session, staleCopy, FIRST_OWNER));
                  other.commit();
                }
                return null;
              });
      assertThat(loaded.await(10, TimeUnit.SECONDS)).isTrue();
      firstCopy.setOwner(
          repositories.getOwnershipRepository().updateOwner(first, firstCopy, SECOND_OWNER));
      transfer.countDown();
      tx.commit();
      second.get(10, TimeUnit.SECONDS);
      authorizer.reloadAuthorizations();
      assertOwnership(FIRST_OWNER, firstId);
    } finally {
      transfer.countDown();
      executor.shutdownNow();
      assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
    }
  }

  @Test
  void staleMetadataUpdateDoesNotRestorePreviousOwner() {
    try (Session stale = sessionFactory.openSession()) {
      Transaction tx = stale.beginTransaction();
      CatalogInfoDAO staleCopy = stale.get(CatalogInfoDAO.class, resourceId);
      new CatalogService(authorizer, repositories, properties)
          .updateCatalog(CATALOG, new UpdateCatalog().owner(SECOND_OWNER));
      staleCopy.setComment("concurrent metadata");
      tx.commit();
    }
    authorizer.reloadAuthorizations();
    assertOwnership(SECOND_OWNER, secondId);
    assertThat(repositories.getCatalogRepository().getCatalog(CATALOG).getComment())
        .isEqualTo("concurrent metadata");
  }

  private void assertOwnership(String email, UUID principal) {
    assertThat(repositories.getCatalogRepository().getCatalog(CATALOG).getOwner()).isEqualTo(email);
    for (UUID id : List.of(firstId, secondId, thirdId)) {
      assertThat(authorizer.authorize(id, resourceId, Privileges.OWNER))
          .isEqualTo(id.equals(principal));
    }
    assertThat(authorizer.listAuthorizations(thirdId, resourceId)).contains(Privileges.MANAGE);
    assertThat(authorizer.getHierarchyParent(childId)).isEqualTo(resourceId);
    try (Session session = sessionFactory.openSession()) {
      assertThat(
              session
                  .createNativeQuery(
                      "select v0 from casbin_rule where ptype = 'p' and v1 = :id and v2 = 'OWNER'",
                      String.class)
                  .setParameter("id", resourceId.toString())
                  .getResultList())
          .containsExactly(principal.toString());
    }
  }
}
