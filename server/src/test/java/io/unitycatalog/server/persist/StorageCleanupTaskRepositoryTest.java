package io.unitycatalog.server.persist;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.persist.StorageCleanupTaskRepository.Claim;
import io.unitycatalog.server.persist.StorageCleanupTaskRepository.CleanupFailureReport;
import io.unitycatalog.server.persist.dao.StorageCleanupTaskDAO;
import io.unitycatalog.server.persist.dao.StorageCleanupTaskDAO.ResourceType;
import io.unitycatalog.server.persist.utils.HibernateConfigurator;
import io.unitycatalog.server.persist.utils.TransactionManager;
import java.time.Duration;
import java.util.Date;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class StorageCleanupTaskRepositoryTest {
  private static final Duration INITIAL_DELAY = Duration.ofHours(1);
  private static final Duration RETRY_BACKOFF = Duration.ofMinutes(30);
  private static final Duration LEASE_DURATION = Duration.ofMinutes(10);

  private SessionFactory sessionFactory;
  private StorageCleanupTaskRepository repository;

  protected void configureDatabase(Properties properties) {
    properties.setProperty("hibernate.connection.driver_class", "org.h2.Driver");
    properties.setProperty(
        "hibernate.connection.url", "jdbc:h2:mem:" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
  }

  @BeforeEach
  void setUpRepository() {
    Properties properties = new Properties();
    configureDatabase(properties);
    properties.setProperty("hibernate.hbm2ddl.auto", "create-drop");
    properties.setProperty("hibernate.show_sql", "false");
    sessionFactory = new HibernateConfigurator(properties).getSessionFactory();
    repository = new StorageCleanupTaskRepository(sessionFactory);
  }

  @AfterEach
  void closeSessionFactory() {
    sessionFactory.close();
  }

  @Test
  void createsNormalizedTask() {
    Date before = databaseNow();
    StorageCleanupTaskDAO task = create("s3://bucket/a/unused/../b/c///");
    Date after = databaseNow();

    assertThat(task.getStorageLocation()).isEqualTo("s3://bucket/a/b/c");
    assertThat(task.getDeletedAt()).isBetween(before, after);
    assertThat(repository.hasPathOverlap("s3://bucket/a/b")).isTrue();
    assertThat(repository.hasPathOverlap("s3://bucket/a/b/c/")).isTrue();
    assertThat(repository.hasPathOverlap("s3://bucket/a/b/c/child")).isTrue();
    assertThat(repository.hasPathOverlap("s3://bucket/a/b/d")).isFalse();
    assertThat(repository.hasPathOverlap("s3://bucket/A/b/c")).isFalse();
  }

  @Test
  void createsTaskInCallerTransaction() {
    UUID resourceId = UUID.randomUUID();

    assertThatThrownBy(
            () ->
                TransactionManager.executeWithTransaction(
                    sessionFactory,
                    session -> {
                      repository.create(
                          session, ResourceType.TABLE, resourceId, "s3://bucket/path");
                      throw new IllegalStateException("rollback");
                    },
                    "Expected rollback",
                    /* readOnly= */ false))
        .isInstanceOf(RuntimeException.class);

    assertThat(find(resourceId)).isEmpty();
  }

  @Test
  void findsLongPathOverlapsUsingFullLocation() {
    String commonPrefix = "s3://bucket/" + "a".repeat(800);
    String location = commonPrefix + "/stored";
    create(location);

    assertThat(repository.hasPathOverlap(commonPrefix)).isTrue();
    assertThat(repository.hasPathOverlap(location)).isTrue();
    assertThat(repository.hasPathOverlap(location + "/child")).isTrue();
    assertThat(repository.hasPathOverlap(commonPrefix + "/sibling")).isFalse();
  }

  @Test
  void escapesLikeMetacharactersInPathChecks() {
    create("s3://bucket/literalX/child");
    create("s3://bucket/percentXYZ25/child");

    assertThat(repository.hasPathOverlap("s3://bucket/literal_")).isFalse();
    assertThat(repository.hasPathOverlap("s3://bucket/percent%25")).isFalse();
  }

  @Test
  void claimsOldestReadyTaskAndRespectsLeases() {
    Date now = databaseNow();
    StorageCleanupTaskDAO earliest =
        create("s3://bucket/earliest", add(now, INITIAL_DELAY.plusMinutes(2).negated()));
    StorageCleanupTaskDAO later =
        create("s3://bucket/later", add(now, INITIAL_DELAY.plusMinutes(1).negated()));
    create("s3://bucket/future", add(now, INITIAL_DELAY.minusMinutes(5).negated()));

    Date beforeClaim = databaseNow();
    Claim first = repository.claim(LEASE_DURATION, INITIAL_DELAY).orElseThrow();
    Date afterClaim = databaseNow();
    assertThat(first.resourceId()).isEqualTo(earliest.getResourceId());
    assertThat(first.resourceType()).isEqualTo(ResourceType.TABLE);
    assertThat(first.storageLocation()).isEqualTo("s3://bucket/earliest");
    assertThat(get(earliest.getResourceId()).getLeaseToken()).isEqualTo(first.leaseToken());
    assertThat(get(earliest.getResourceId()).getLeaseExpiresAt().getTime())
        .isBetween(
            add(beforeClaim, LEASE_DURATION).getTime(), add(afterClaim, LEASE_DURATION).getTime());

    Claim second = repository.claim(LEASE_DURATION, INITIAL_DELAY).orElseThrow();
    assertThat(second.resourceId()).isEqualTo(later.getResourceId());
    assertThat(repository.claim(LEASE_DURATION, INITIAL_DELAY)).isEmpty();
  }

  @Test
  void claimsOldestAvailableTaskAcrossTaskStates() {
    Date now = databaseNow();
    create("s3://bucket/new", add(now, INITIAL_DELAY.negated()));
    StorageCleanupTaskDAO partial =
        create("s3://bucket/partial", add(now, INITIAL_DELAY.negated()));
    StorageCleanupTaskDAO failed = create("s3://bucket/failed", add(now, INITIAL_DELAY.negated()));
    StorageCleanupTaskDAO crashed =
        create("s3://bucket/crashed", add(now, INITIAL_DELAY.negated()));
    setState(partial.getResourceId(), null, add(now, Duration.ofMinutes(-70)), 0);
    setState(failed.getResourceId(), null, add(now, Duration.ofMinutes(-75)), 1);
    setState(crashed.getResourceId(), UUID.randomUUID(), add(now, Duration.ofMinutes(-80)), 0);

    assertThat(repository.claim(LEASE_DURATION, INITIAL_DELAY))
        .get()
        .extracting(Claim::resourceId)
        .isEqualTo(crashed.getResourceId());
  }

  @Test
  void validatesMinimumLeaseDuration() {
    create("s3://bucket/duration");

    for (Duration duration :
        java.util.List.of(Duration.ZERO, Duration.ofNanos(-1), Duration.ofNanos(1))) {
      assertThatThrownBy(() -> repository.claim(duration, Duration.ZERO))
          .isInstanceOfSatisfying(
              BaseException.class,
              exception -> {
                assertThat(exception.getErrorCode()).isEqualTo(ErrorCode.INVALID_ARGUMENT);
                assertThat(exception).hasMessage("Lease duration must be at least one millisecond");
              });
    }
    assertThat(repository.claim(Duration.ofMillis(1), Duration.ZERO)).isPresent();
  }

  @Test
  void rejectsNegativeDelays() {
    assertInvalidArgument(
        () -> repository.claim(LEASE_DURATION, Duration.ofMillis(-1)),
        "Initial cleanup delay cannot be negative");
    assertInvalidArgument(
        () -> new CleanupFailureReport("error", Duration.ofMillis(-1)),
        "Retry backoff cannot be negative");
  }

  @Test
  void onlyOneCompetingClaimSucceeds() throws Exception {
    create("s3://bucket/competing", add(databaseNow(), Duration.ofMinutes(-1)));
    CountDownLatch ready = new CountDownLatch(2);
    CountDownLatch start = new CountDownLatch(1);
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      java.util.concurrent.Callable<Optional<Claim>> claim =
          () -> {
            ready.countDown();
            start.await();
            return repository.claim(LEASE_DURATION, Duration.ZERO);
          };
      Future<Optional<Claim>> first = executor.submit(claim);
      Future<Optional<Claim>> second = executor.submit(claim);
      assertThat(ready.await(10, TimeUnit.SECONDS)).isTrue();
      start.countDown();

      assertThat(
              java.util.List.of(first.get(10, TimeUnit.SECONDS), second.get(10, TimeUnit.SECONDS)))
          .filteredOn(Optional::isPresent)
          .hasSize(1);
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  void reportsFailure() {
    StorageCleanupTaskDAO task =
        create("s3://bucket/retry", add(databaseNow(), Duration.ofMinutes(-1)));
    Claim first = repository.claim(LEASE_DURATION, Duration.ZERO).orElseThrow();
    Date beforeReport = databaseNow();

    assertThat(
            repository.reportFailure(
                task.getResourceId(),
                first.leaseToken(),
                new CleanupFailureReport("x".repeat(2100), RETRY_BACKOFF)))
        .isTrue();
    Date afterReport = databaseNow();
    StorageCleanupTaskDAO failed = get(task.getResourceId());
    assertThat(failed.getLeaseToken()).isNull();
    assertThat(failed.getLeaseExpiresAt().getTime())
        .isBetween(
            add(beforeReport, RETRY_BACKOFF).getTime(), add(afterReport, RETRY_BACKOFF).getTime());
    assertThat(failed.getFailureCount()).isOne();
    assertThat(failed.getLastError()).hasSize(2048);
    assertThat(repository.claim(LEASE_DURATION, Duration.ZERO)).isEmpty();
  }

  @Test
  void staleWorkerCannotReportOrFinishReclaimedTask() {
    StorageCleanupTaskDAO task =
        create("s3://bucket/stale", add(databaseNow(), Duration.ofMinutes(-1)));
    Claim stale = repository.claim(LEASE_DURATION, Duration.ZERO).orElseThrow();
    setState(
        task.getResourceId(), stale.leaseToken(), add(databaseNow(), Duration.ofMillis(-1)), 0);
    Claim current = repository.claim(LEASE_DURATION, Duration.ZERO).orElseThrow();

    assertThat(
            repository.reportFailure(
                task.getResourceId(),
                stale.leaseToken(),
                new CleanupFailureReport("stale", RETRY_BACKOFF)))
        .isFalse();
    assertThat(repository.finish(task.getResourceId(), stale.leaseToken())).isFalse();
    assertThat(repository.finish(task.getResourceId(), current.leaseToken())).isTrue();
    assertThat(find(task.getResourceId())).isEmpty();
  }

  private StorageCleanupTaskDAO create(String location) {
    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> repository.create(session, ResourceType.TABLE, UUID.randomUUID(), location),
        "Failed to create test storage cleanup task",
        /* readOnly= */ false);
  }

  private StorageCleanupTaskDAO create(String location, Date deletedAt) {
    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          StorageCleanupTaskDAO task =
              repository.create(session, ResourceType.TABLE, UUID.randomUUID(), location);
          task.setDeletedAt(deletedAt);
          return task;
        },
        "Failed to create test storage cleanup task",
        /* readOnly= */ false);
  }

  private StorageCleanupTaskDAO get(UUID resourceId) {
    return find(resourceId).orElseThrow();
  }

  private Optional<StorageCleanupTaskDAO> find(UUID resourceId) {
    try (Session session = sessionFactory.openSession()) {
      return Optional.ofNullable(session.find(StorageCleanupTaskDAO.class, resourceId));
    }
  }

  private Date databaseNow() {
    try (Session session = sessionFactory.openSession()) {
      return session.createQuery("SELECT CURRENT_TIMESTAMP", Date.class).getSingleResult();
    }
  }

  private static void assertInvalidArgument(Runnable action, String message) {
    assertThatThrownBy(action::run)
        .isInstanceOfSatisfying(
            BaseException.class,
            exception -> {
              assertThat(exception.getErrorCode()).isEqualTo(ErrorCode.INVALID_ARGUMENT);
              assertThat(exception).hasMessage(message);
            });
  }

  private void setState(UUID resourceId, UUID leaseToken, Date leaseExpiresAt, int failures) {
    TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          StorageCleanupTaskDAO task = session.find(StorageCleanupTaskDAO.class, resourceId);
          task.setLeaseToken(leaseToken);
          task.setLeaseExpiresAt(leaseExpiresAt);
          task.setFailureCount(failures);
          return null;
        },
        "Failed to set test cleanup task state",
        /* readOnly= */ false);
  }

  private static Date add(Date time, Duration duration) {
    return new Date(Math.addExact(time.getTime(), duration.toMillis()));
  }
}
