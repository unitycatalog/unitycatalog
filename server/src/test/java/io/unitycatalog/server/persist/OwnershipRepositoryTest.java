package io.unitycatalog.server.persist;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.assertj.core.api.Assertions.fail;
import static org.assertj.core.api.Assertions.tuple;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.util.SafeCloseable;
import com.linecorp.armeria.server.ServiceRequestContext;
import io.unitycatalog.server.audit.AuditRecord;
import io.unitycatalog.server.audit.Auditor;
import io.unitycatalog.server.audit.CapturingAuditSink;
import io.unitycatalog.server.auth.CasbinPolicyRefresher;
import io.unitycatalog.server.auth.JCasbinAuthorizer;
import io.unitycatalog.server.auth.UnityCatalogAuthorizer;
import io.unitycatalog.server.auth.decorator.UnityAccessEvaluator;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.SecurableType;
import io.unitycatalog.server.model.TableInfo;
import io.unitycatalog.server.model.TableType;
import io.unitycatalog.server.model.UpdateCatalog;
import io.unitycatalog.server.model.UpdateFunction;
import io.unitycatalog.server.model.UpdateTable;
import io.unitycatalog.server.persist.dao.CatalogInfoDAO;
import io.unitycatalog.server.persist.dao.FunctionInfoDAO;
import io.unitycatalog.server.persist.dao.IdentifiableDAO;
import io.unitycatalog.server.persist.dao.SchemaInfoDAO;
import io.unitycatalog.server.persist.dao.TableInfoDAO;
import io.unitycatalog.server.persist.dao.UserDAO;
import io.unitycatalog.server.persist.model.Privileges;
import io.unitycatalog.server.persist.utils.HibernateConfigurator;
import io.unitycatalog.server.service.CatalogService;
import io.unitycatalog.server.service.TableService;
import io.unitycatalog.server.testdatabase.SupportedDatabase;
import io.unitycatalog.server.testdatabase.TestDatabase;
import io.unitycatalog.server.testdatabase.TestDatabases;
import io.unitycatalog.server.utils.ServerProperties;
import io.unitycatalog.server.utils.ServerProperties.Property;
import java.time.Duration;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.UnaryOperator;
import org.hibernate.LockMode;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

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
  private Auditor auditor;
  private SafeCloseable requestContext;

  @BeforeEach
  void setUp() throws Exception {
    requestContext = ServiceRequestContext.of(HttpRequest.of(HttpMethod.PATCH, "/")).push();
    Properties values = new Properties();
    values.setProperty(Property.SERVER_ENV.getKey(), "test");
    values.setProperty(Property.AUTHORIZATION_ENABLED.getKey(), "enable");
    values.setProperty(Property.POLICY_REFRESH_ENABLED.getKey(), "false");
    values.setProperty(Property.AUDIT_SINK.getKey(), CapturingAuditSink.class.getName());
    properties = new ServerProperties(values);
    CapturingAuditSink.reset();
    auditor = Auditor.fromProperties(properties).orElseThrow();
    database = TestDatabases.newDatabase();
    hibernateProperties = database.hibernateProperties();
    // Keep the data when reopening the persistence layer in the restart test.
    if ("create-drop".equals(hibernateProperties.getProperty("hibernate.hbm2ddl.auto"))) {
      hibernateProperties.setProperty("hibernate.hbm2ddl.auto", "update");
    }
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
    repositories =
        new Repositories(
            sessionFactory, properties, null, UnaryOperator.identity(), null, Optional.of(auditor));
    authorizer = new JCasbinAuthorizer(configurator, properties);
  }

  @AfterEach
  void tearDown() {
    try {
      if (authorizer != null) {
        authorizer.close();
      }
      if (auditor != null) {
        auditor.close();
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

    UnityCatalogAuthorizer serviceAuthorizer =
        mock(UnityCatalogAuthorizer.class, delegatesTo(authorizer));
    doThrow(new IllegalStateException("policy reload must not run after rollback"))
        .when(serviceAuthorizer)
        .reloadAuthorizations();
    CatalogService service = new CatalogService(serviceAuthorizer, repositories, properties);
    assertThatExceptionOfType(BaseException.class)
        .isThrownBy(
            () ->
                service.updateCatalog(
                    CATALOG, new UpdateCatalog().owner(SECOND_OWNER).newName("occupied")))
        .extracting(BaseException::getErrorCode)
        .isEqualTo(ErrorCode.CATALOG_ALREADY_EXISTS);
    assertOwnership(FIRST_OWNER, firstId);
    for (var invalid :
        Map.of("", ErrorCode.INVALID_ARGUMENT, "missing-owner@localhost", ErrorCode.NOT_FOUND)
            .entrySet()) {
      assertThatExceptionOfType(BaseException.class)
          .isThrownBy(
              () -> service.updateCatalog(CATALOG, new UpdateCatalog().owner(invalid.getKey())))
          .extracting(BaseException::getErrorCode)
          .isEqualTo(invalid.getValue());
      assertThatExceptionOfType(BaseException.class)
          .isThrownBy(
              () ->
                  repositories
                      .getCatalogRepository()
                      .updateCatalog(CATALOG, new UpdateCatalog().owner(invalid.getKey())))
          .extracting(BaseException::getErrorCode)
          .isEqualTo(invalid.getValue());
      authorizer.reloadAuthorizations();
      assertOwnership(FIRST_OWNER, firstId);
    }
    verify(serviceAuthorizer, never()).reloadAuthorizations();
    assertThat(CapturingAuditSink.RECORDS).isEmpty();
  }

  @Test
  void committedOwnershipSurvivesRestart() throws Exception {
    CatalogService service = new CatalogService(auditor.wrap(authorizer), repositories, properties);
    try (JCasbinAuthorizer other = new JCasbinAuthorizer(configurator, properties);
        CasbinPolicyRefresher refresher =
            new CasbinPolicyRefresher(other::reloadAuthorizations, sessionFactory, Duration.ZERO)) {
      assertThat(refresher.checkAndReload()).isTrue();
      service.updateCatalog(CATALOG, new UpdateCatalog().owner(SECOND_OWNER).comment("changed"));
      assertThat(CapturingAuditSink.RECORDS)
          .extracting(
              AuditRecord::getAction,
              AuditRecord::getPrincipalId,
              AuditRecord::getResourceId,
              AuditRecord::getPrivilege)
          .containsExactly(
              tuple(AuditRecord.Action.REVOKE, firstId, resourceId, Privileges.OWNER.getValue()),
              tuple(AuditRecord.Action.GRANT, secondId, resourceId, Privileges.OWNER.getValue()));
      assertThat(other.authorize(firstId, SecurableType.CATALOG, resourceId, Privileges.OWNER))
          .isTrue();
      assertThat(refresher.checkAndReload()).isTrue();
      assertThat(other.authorize(firstId, SecurableType.CATALOG, resourceId, Privileges.OWNER))
          .isFalse();
      assertThat(other.authorize(secondId, SecurableType.CATALOG, resourceId, Privileges.OWNER))
          .isTrue();
    }
    assertOwnership(SECOND_OWNER, secondId);
    assertThat(repositories.getCatalogRepository().getCatalog(CATALOG).getComment())
        .isEqualTo("changed");
    authorizer.close();
    configurator.close();
    openRepositories();
    assertOwnership(SECOND_OWNER, secondId);
  }

  @ParameterizedTest
  @CsvSource({"false, false", "false, true", "true, false", "true, true"})
  void committedOwnershipReloadsAfterCommitOrCloseFailure(
      boolean commitFails, boolean reloadFails) {
    RuntimeException updateError = new IllegalStateException("committed update failed");
    RuntimeException reloadError = new IllegalStateException("policy reload failed");
    UnityCatalogAuthorizer serviceAuthorizer =
        mock(UnityCatalogAuthorizer.class, delegatesTo(authorizer));
    if (reloadFails) {
      doThrow(reloadError).when(serviceAuthorizer).reloadAuthorizations();
    }
    try (Session realSession = sessionFactory.openSession()) {
      Session session = mock(Session.class, delegatesTo(realSession));
      if (commitFails) {
        Transaction realTransaction = realSession.beginTransaction();
        Transaction transaction = mock(Transaction.class, delegatesTo(realTransaction));
        doReturn(transaction).when(session).beginTransaction();
        doReturn(transaction).when(session).getTransaction();
        doAnswer(
                invocation -> {
                  realTransaction.commit();
                  throw updateError;
                })
            .when(transaction)
            .commit();
      } else {
        doAnswer(
                invocation -> {
                  realSession.close();
                  throw updateError;
                })
            .when(session)
            .close();
      }
      SessionFactory failingFactory = mock(SessionFactory.class);
      when(failingFactory.openSession()).thenReturn(session);
      Repositories serviceRepositories = spy(repositories);
      doReturn(new CatalogRepository(repositories, failingFactory))
          .when(serviceRepositories)
          .getCatalogRepository();
      CatalogService service =
          new CatalogService(serviceAuthorizer, serviceRepositories, properties);

      Throwable error =
          catchThrowable(
              () -> service.updateCatalog(CATALOG, new UpdateCatalog().owner(SECOND_OWNER)));
      if (commitFails) {
        assertThat(error).isInstanceOf(BaseException.class).hasCause(updateError);
      } else {
        assertThat(error).isSameAs(updateError);
      }
      verify(serviceAuthorizer).reloadAuthorizations();
      assertThat(error.getSuppressed())
          .containsExactly(reloadFails ? new Throwable[] {reloadError} : new Throwable[0]);
    }
    assertThat(repositories.getCatalogRepository().getCatalog(CATALOG).getOwner())
        .isEqualTo(SECOND_OWNER);
    if (!reloadFails) {
      assertOwnership(SECOND_OWNER, secondId);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void failedCacheReloadDeniesOldPolicyUntilRecovery(boolean loadOwner) throws Exception {
    repositories
        .getCatalogRepository()
        .updateCatalog(CATALOG, new UpdateCatalog().owner(SECOND_OWNER));
    sessionFactory.inTransaction(
        session ->
            session
                .createNativeMutationQuery("ALTER TABLE casbin_rule RENAME TO unavailable_policy")
                .executeUpdate());
    try {
      assertThatExceptionOfType(BaseException.class)
          .isThrownBy(authorizer::reloadAuthorizations)
          .extracting(BaseException::getErrorCode)
          .isEqualTo(ErrorCode.UNAVAILABLE);
      for (UUID id : List.of(firstId, secondId, thirdId)) {
        assertThat(authorizer.authorize(id, SecurableType.CATALOG, resourceId, Privileges.OWNER))
            .isFalse();
        assertThat(authorizer.authorize(id, SecurableType.CATALOG, resourceId, Privileges.MANAGE))
            .isFalse();
      }
      // Operations must refuse the incomplete policy while the store is unavailable.
      assertThat(
              new UnityAccessEvaluator(authorizer, true)
                  .evaluate(
                      firstId,
                      "#authorize(#principal, #catalog, OWNER)",
                      Map.of(SecurableType.CATALOG, resourceId),
                      Map.of()))
          .isFalse();
      List<Runnable> blocked =
          List.of(
              () -> authorizer.refreshAuthorizations(System.nanoTime()),
              () -> authorizer.grantAuthorization(thirdId, resourceId, Privileges.MANAGE),
              () -> authorizer.revokeAuthorization(secondId, resourceId, Privileges.OWNER),
              () -> authorizer.clearAuthorizationsForPrincipal(secondId),
              () -> authorizer.clearAuthorizationsForResource(resourceId),
              () -> authorizer.addHierarchyChild(resourceId, UUID.randomUUID()),
              () -> authorizer.removeHierarchyChild(resourceId, childId),
              () -> authorizer.removeHierarchyChildren(resourceId),
              () -> authorizer.listAuthorizations(resourceId),
              () -> authorizer.listAuthorizations(secondId, resourceId),
              () -> authorizer.getHierarchyParent(childId));
      for (Runnable operation : blocked) {
        assertThatExceptionOfType(BaseException.class)
            .isThrownBy(operation::run)
            .extracting(BaseException::getErrorCode)
            .isEqualTo(ErrorCode.UNAVAILABLE);
      }
    } finally {
      sessionFactory.inTransaction(
          session ->
              session
                  .createNativeMutationQuery("ALTER TABLE unavailable_policy RENAME TO casbin_rule")
                  .executeUpdate());
    }
    if (loadOwner) {
      authorizer.loadOwner(resourceId);
    } else {
      assertThat(authorizer.refreshAuthorizations(System.nanoTime())).isTrue();
    }
    assertOwnership(SECOND_OWNER, secondId);
    try (Session session = sessionFactory.openSession()) {
      assertThat(
              session
                  .createNativeQuery(
                      "select v2 from casbin_rule where ptype = 'p' and v0 = :principal and v1 ="
                          + " :id",
                      String.class)
                  .setParameter("principal", thirdId.toString())
                  .setParameter("id", resourceId.toString())
                  .getResultList())
          .containsExactly("MANAGE");
    }
    assertThat(authorizer.authorize(thirdId, SecurableType.CATALOG, resourceId, Privileges.SELECT))
        .isFalse();
  }

  @Test
  void creatingOwnerDoesNotBlockUnrelatedGrants() throws Exception {
    UUID created = UUID.randomUUID();
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try (Session session = sessionFactory.openSession()) {
      Transaction transaction = session.beginTransaction();
      try {
        repositories.getOwnershipRepository().initializeOwner(session, created, FIRST_OWNER);
        Future<Boolean> grant =
            executor.submit(
                () -> authorizer.grantAuthorization(thirdId, resourceId, Privileges.SELECT));
        assertThat(grant.get(10, TimeUnit.SECONDS)).isTrue();
        transaction.commit();
      } finally {
        if (transaction.isActive()) {
          transaction.rollback();
        }
      }
    } finally {
      executor.shutdownNow();
      assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
    }
    authorizer.loadOwner(created);
    assertThat(authorizer.listAuthorizations(firstId, created)).containsExactly(Privileges.OWNER);
    assertThat(authorizer.listAuthorizations(thirdId, resourceId))
        .containsExactlyInAnyOrder(Privileges.MANAGE, Privileges.SELECT);
    assertThat(CapturingAuditSink.RECORDS)
        .extracting(AuditRecord::getAction, AuditRecord::getPrincipalId, AuditRecord::getResourceId)
        .containsExactly(tuple(AuditRecord.Action.GRANT, firstId, created));
  }

  @Test
  void transferringOwnerDoesNotBlockUnrelatedGrantsOrRevokes() throws Exception {
    UUID unrelated = UUID.randomUUID();
    authorizer.grantAuthorization(thirdId, unrelated, Privileges.MANAGE);
    authorizer.grantAuthorization(thirdId, unrelated, Privileges.SELECT);
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try (Session session = sessionFactory.openSession()) {
      Transaction transaction = session.beginTransaction();
      try {
        CatalogInfoDAO resource = session.get(CatalogInfoDAO.class, resourceId);
        resource.setOwner(
            repositories.getOwnershipRepository().updateOwner(session, resource, SECOND_OWNER));
        Future<?> changes =
            executor.submit(
                () -> {
                  assertThat(authorizer.grantAuthorization(thirdId, unrelated, Privileges.MODIFY))
                      .isTrue();
                  assertThat(authorizer.revokeAuthorization(thirdId, unrelated, Privileges.SELECT))
                      .isTrue();
                });
        changes.get(10, TimeUnit.SECONDS);
        transaction.commit();
      } finally {
        if (transaction.isActive()) {
          transaction.rollback();
        }
      }
    } finally {
      executor.shutdownNow();
      assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
    }
    authorizer.reloadAuthorizations();
    assertOwnership(SECOND_OWNER, secondId);
    assertThat(authorizer.listAuthorizations(thirdId, unrelated))
        .containsExactlyInAnyOrder(Privileges.MANAGE, Privileges.MODIFY);
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
      assertThat(CapturingAuditSink.RECORDS).isEmpty();
      transfer.countDown();
      tx.commit();
      second.get(10, TimeUnit.SECONDS);
      authorizer.reloadAuthorizations();
      assertOwnership(FIRST_OWNER, firstId);
      assertThat(CapturingAuditSink.RECORDS)
          .extracting(AuditRecord::getAction, AuditRecord::getPrincipalId)
          .containsExactlyInAnyOrder(
              tuple(AuditRecord.Action.REVOKE, firstId),
              tuple(AuditRecord.Action.GRANT, secondId),
              tuple(AuditRecord.Action.REVOKE, secondId),
              tuple(AuditRecord.Action.GRANT, firstId));
    } finally {
      transfer.countDown();
      executor.shutdownNow();
      assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void concurrentTransfersRefreshCollectionsAfterLock(boolean function) throws Exception {
    UUID schemaId = UUID.randomUUID();
    UUID targetId = UUID.randomUUID();
    Class<? extends IdentifiableDAO> entityType =
        function ? FunctionInfoDAO.class : TableInfoDAO.class;
    String fullName = CATALOG + ".schema.target";
    sessionFactory.inTransaction(
        session -> {
          session.persist(
              SchemaInfoDAO.builder().id(schemaId).name("schema").catalogId(resourceId).build());
          session.persist(
              function
                  ? FunctionInfoDAO.builder()
                      .id(targetId)
                      .name("target")
                      .schemaId(schemaId)
                      .owner(FIRST_OWNER)
                      .build()
                  : TableInfoDAO.builder()
                      .id(targetId)
                      .name("target")
                      .schemaId(schemaId)
                      .type(TableType.EXTERNAL.getValue())
                      .owner(FIRST_OWNER)
                      .build());
        });
    authorizer.grantAuthorization(firstId, targetId, Privileges.OWNER);
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try (Session first = sessionFactory.openSession()) {
      Transaction transaction = first.beginTransaction();
      try {
        IdentifiableDAO resource = first.get(entityType, targetId);
        String owner =
            repositories.getOwnershipRepository().updateOwner(first, resource, SECOND_OWNER);
        TestDatabase.setColumn(first, entityType, targetId, "owner", owner);
        Future<String> second =
            executor.submit(
                () -> {
                  try (SafeCloseable ignored =
                      ServiceRequestContext.of(HttpRequest.of(HttpMethod.PATCH, "/")).push()) {
                    return function
                        ? repositories
                            .getFunctionRepository()
                            .updateFunction(fullName, new UpdateFunction().owner(FIRST_OWNER))
                            .getOwner()
                        : repositories
                            .getTableRepository()
                            .updateTable(
                                fullName, new UpdateTable().owner(FIRST_OWNER), true, () -> {})
                            .getOwner();
                  }
                });
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (!database.hasLockWaiter()) {
          if (second.isDone()) {
            second.get();
            fail("The second transfer did not wait for the row lock");
          }
          assertThat(System.nanoTime()).isLessThan(deadline);
          Thread.sleep(100);
        }
        transaction.commit();
        assertThat(second.get(30, TimeUnit.SECONDS)).isEqualTo(FIRST_OWNER);
      } finally {
        if (transaction.isActive()) {
          transaction.rollback();
        }
      }
    } finally {
      executor.shutdownNow();
      assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
    }
    String storedOwner =
        sessionFactory.fromSession(
            session ->
                session
                    .createSelectionQuery(
                        "select owner from " + entityType.getSimpleName() + " where id = :id",
                        String.class)
                    .setParameter("id", targetId)
                    .getSingleResult());
    assertThat(storedOwner).isEqualTo(FIRST_OWNER);
    authorizer.reloadAuthorizations();
    assertThat(authorizer.listAuthorizations(firstId, targetId)).containsExactly(Privileges.OWNER);
    assertThat(authorizer.listAuthorizations(secondId, targetId)).isEmpty();
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void transferLockTimeoutIsUnavailableAndRollsBack(boolean policyLock) {
    Properties impatient = database.hibernateProperties();
    impatient.setProperty("hibernate.hbm2ddl.auto", "none");
    impatient.setProperty(
        "hibernate.connection.url",
        database.jdbcUrl()
            + switch (SupportedDatabase.selected()) {
              case H2 -> ";LOCK_TIMEOUT=50";
              case POSTGRES_18 -> "?options=-c%20lock_timeout%3D50";
              case MYSQL_8 -> "?sessionVariables=innodb_lock_wait_timeout=1";
            });
    CatalogRepository updates =
        new CatalogRepository(
            repositories, database.openHibernateConfigurator(impatient).getSessionFactory());
    try (Session held = sessionFactory.openSession()) {
      Transaction transaction = held.beginTransaction();
      try {
        if (policyLock) {
          held.createNativeQuery(
                  "SELECT v0 FROM casbin_rule WHERE ptype = 'p' AND v1 = :id AND v2 = 'OWNER'"
                      + " FOR UPDATE",
                  String.class)
              .setParameter("id", resourceId.toString())
              .getResultList();
        } else {
          held.get(CatalogInfoDAO.class, resourceId, LockMode.PESSIMISTIC_WRITE);
        }
        assertThatExceptionOfType(BaseException.class)
            .isThrownBy(
                () -> updates.updateCatalog(CATALOG, new UpdateCatalog().owner(SECOND_OWNER)))
            .extracting(BaseException::getErrorCode)
            .isEqualTo(ErrorCode.UNAVAILABLE);
      } finally {
        transaction.rollback();
      }
    }
    authorizer.reloadAuthorizations();
    assertOwnership(FIRST_OWNER, firstId);
    assertThat(CapturingAuditSink.RECORDS).isEmpty();
  }

  @Test
  void viewTransferValidatesTheLockedResource() {
    UUID schemaId = UUID.randomUUID();
    UUID tableId = UUID.randomUUID();
    String fullName = CATALOG + ".schema.table";
    sessionFactory.inTransaction(
        session -> {
          session.persist(
              SchemaInfoDAO.builder().id(schemaId).name("schema").catalogId(resourceId).build());
          session.persist(
              TableInfoDAO.builder()
                  .id(tableId)
                  .name("table")
                  .schemaId(schemaId)
                  .type(TableType.VIEW.getValue())
                  .owner(FIRST_OWNER)
                  .build());
        });
    authorizer.grantAuthorization(firstId, tableId, Privileges.OWNER);
    TableRepository tableRepository = spy(repositories.getTableRepository());
    // The old pre-update lookup could see an external table before a DROP and CREATE VIEW.
    doReturn(new TableInfo().tableType(TableType.EXTERNAL))
        .when(tableRepository)
        .getTable(fullName);
    Repositories serviceRepositories = spy(repositories);
    doReturn(tableRepository).when(serviceRepositories).getTableRepository();
    TableService service =
        new TableService(authorizer, serviceRepositories, properties) {
          @Override
          protected boolean canTransferOwnerTo(String newOwner) {
            return false;
          }
        };

    assertThatExceptionOfType(BaseException.class)
        .isThrownBy(() -> service.updateTable(fullName, new UpdateTable().owner(SECOND_OWNER)))
        .extracting(BaseException::getErrorCode)
        .isEqualTo(ErrorCode.PERMISSION_DENIED);
    String storedOwner =
        sessionFactory.fromSession(session -> session.get(TableInfoDAO.class, tableId).getOwner());
    assertThat(storedOwner).isEqualTo(FIRST_OWNER);
    assertThat(authorizer.listAuthorizations(firstId, tableId)).contains(Privileges.OWNER);
    assertThat(authorizer.listAuthorizations(secondId, tableId)).doesNotContain(Privileges.OWNER);
    assertThat(CapturingAuditSink.RECORDS).isEmpty();
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
      assertThat(authorizer.authorize(id, SecurableType.CATALOG, resourceId, Privileges.OWNER))
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
