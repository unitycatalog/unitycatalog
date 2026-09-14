package io.unitycatalog.server.persist;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.persist.StorageCleanupTaskRepository.Claim;
import io.unitycatalog.server.persist.StorageCleanupTaskRepository.CleanupReport.Failure;
import io.unitycatalog.server.persist.StorageCleanupTaskRepository.CleanupReport.Partial;
import io.unitycatalog.server.persist.dao.StorageCleanupTaskDAO;
import io.unitycatalog.server.persist.dao.StorageCleanupTaskDAO.ResourceType;
import io.unitycatalog.server.persist.utils.HibernateConfigurator;
import io.unitycatalog.server.persist.utils.TransactionManager;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class StorageCleanupTaskRepositoryTest {
  private static final Date NOW = Date.from(Instant.parse("2026-09-08T12:00:00Z"));
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
    StorageCleanupTaskDAO task = create("s3://bucket/a/unused/../b/c///", NOW);

    assertThat(task.getStorageLocation()).isEqualTo("s3://bucket/a/b/c");
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
                          session, ResourceType.TABLE, resourceId, "s3://bucket/path", NOW);
                      throw new IllegalStateException("rollback");
                    },
                    "Expected rollback",
                    /* readOnly= */ false))
        .isInstanceOf(RuntimeException.class);

    assertThat(find(resourceId)).isEmpty();
  }

  @Test
  void claimsOldestReadyTaskAndRespectsLeases() {
    StorageCleanupTaskDAO earliest =
        create("s3://bucket/earliest", add(NOW, INITIAL_DELAY.plusMinutes(2).negated()));
    StorageCleanupTaskDAO later =
        create("s3://bucket/later", add(NOW, INITIAL_DELAY.plusMinutes(1).negated()));
    create("s3://bucket/future", add(NOW, INITIAL_DELAY.minusSeconds(1).negated()));

    Claim first = repository.claim(NOW, LEASE_DURATION, INITIAL_DELAY).orElseThrow();
    assertThat(first.task().getResourceId()).isEqualTo(earliest.getResourceId());
    assertThat(get(earliest.getResourceId()).getLeaseToken()).isEqualTo(first.leaseToken());
    assertThat(get(earliest.getResourceId()).getLeaseExpiresAt())
        .hasSameTimeAs(add(NOW, LEASE_DURATION));

    Claim second = repository.claim(NOW, LEASE_DURATION, INITIAL_DELAY).orElseThrow();
    assertThat(second.task().getResourceId()).isEqualTo(later.getResourceId());
    assertThat(repository.claim(NOW, LEASE_DURATION, INITIAL_DELAY)).isEmpty();
  }

  @Test
  void claimsOldestAvailableTaskAcrossTaskStates() {
    create("s3://bucket/new", add(NOW, INITIAL_DELAY.negated()));
    StorageCleanupTaskDAO partial =
        create("s3://bucket/partial", add(NOW, INITIAL_DELAY.negated()));
    StorageCleanupTaskDAO failed = create("s3://bucket/failed", add(NOW, INITIAL_DELAY.negated()));
    StorageCleanupTaskDAO crashed =
        create("s3://bucket/crashed", add(NOW, INITIAL_DELAY.negated()));
    setState(partial.getResourceId(), null, add(NOW, Duration.ofMinutes(-70)), 0);
    setState(failed.getResourceId(), null, add(NOW, Duration.ofMinutes(-75)), 1);
    setState(crashed.getResourceId(), UUID.randomUUID(), add(NOW, Duration.ofMinutes(-80)), 0);

    assertThat(repository.claim(NOW, LEASE_DURATION, INITIAL_DELAY))
        .get()
        .extracting(claim -> claim.task().getResourceId())
        .isEqualTo(crashed.getResourceId());
  }

  @Test
  void validatesMinimumLeaseDuration() {
    create("s3://bucket/duration", NOW);

    for (Duration duration :
        java.util.List.of(Duration.ZERO, Duration.ofNanos(-1), Duration.ofNanos(1))) {
      assertThatThrownBy(() -> repository.claim(NOW, duration, Duration.ZERO))
          .isInstanceOfSatisfying(
              BaseException.class,
              exception -> {
                assertThat(exception.getErrorCode()).isEqualTo(ErrorCode.INVALID_ARGUMENT);
                assertThat(exception).hasMessage("Lease duration must be at least one millisecond");
              });
    }
    assertThat(repository.claim(NOW, Duration.ofMillis(1), Duration.ZERO)).isPresent();
  }

  @Test
  void onlyOneCompetingClaimSucceeds() throws Exception {
    create("s3://bucket/competing", NOW);
    CountDownLatch ready = new CountDownLatch(2);
    CountDownLatch start = new CountDownLatch(1);
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      java.util.concurrent.Callable<Optional<Claim>> claim =
          () -> {
            ready.countDown();
            start.await();
            return repository.claim(NOW, LEASE_DURATION, Duration.ZERO);
          };
      Future<Optional<Claim>> first = executor.submit(claim);
      Future<Optional<Claim>> second = executor.submit(claim);
      ready.await();
      start.countDown();

      assertThat(java.util.List.of(first.get(), second.get()))
          .filteredOn(Optional::isPresent)
          .hasSize(1);
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  void reportsFailureAndPartialProgress() {
    StorageCleanupTaskDAO task = create("s3://bucket/retry", NOW);
    Claim first = repository.claim(NOW, LEASE_DURATION, Duration.ZERO).orElseThrow();
    Date failedAt = add(NOW, Duration.ofSeconds(1));

    assertThat(
            repository.report(
                task.getResourceId(),
                first.leaseToken(),
                failedAt,
                new Failure("x".repeat(2100), RETRY_BACKOFF)))
        .isTrue();
    StorageCleanupTaskDAO failed = get(task.getResourceId());
    assertThat(failed.getLeaseToken()).isNull();
    assertThat(failed.getLeaseExpiresAt()).hasSameTimeAs(add(failedAt, RETRY_BACKOFF));
    assertThat(failed.getFailureCount()).isOne();
    assertThat(failed.getLastError()).hasSize(2048);
    assertThat(
            repository.claim(
                add(failed.getLeaseExpiresAt(), Duration.ofMillis(-1)),
                LEASE_DURATION,
                Duration.ZERO))
        .isEmpty();

    Date retryAt = add(failed.getLeaseExpiresAt(), Duration.ofMillis(1));
    Claim retry = repository.claim(retryAt, LEASE_DURATION, Duration.ZERO).orElseThrow();
    Date partialAt = add(retryAt, Duration.ofSeconds(1));
    assertThat(
            repository.report(task.getResourceId(), retry.leaseToken(), partialAt, new Partial()))
        .isTrue();
    StorageCleanupTaskDAO partial = get(task.getResourceId());
    assertThat(partial.getLeaseToken()).isNull();
    assertThat(partial.getLeaseExpiresAt()).hasSameTimeAs(partialAt);
    assertThat(partial.getFailureCount()).isZero();
    assertThat(partial.getLastError()).isNull();
  }

  @Test
  void staleWorkerCannotReportOrFinishReclaimedTask() {
    StorageCleanupTaskDAO task = create("s3://bucket/stale", NOW);
    Claim stale = repository.claim(NOW, LEASE_DURATION, Duration.ZERO).orElseThrow();
    Date reclaimedAt = add(add(NOW, LEASE_DURATION), Duration.ofMillis(1));
    Claim current = repository.claim(reclaimedAt, LEASE_DURATION, Duration.ZERO).orElseThrow();

    assertThat(
            repository.report(task.getResourceId(), stale.leaseToken(), reclaimedAt, new Partial()))
        .isFalse();
    assertThat(repository.finish(task.getResourceId(), stale.leaseToken(), reclaimedAt)).isFalse();
    assertThat(repository.finish(task.getResourceId(), current.leaseToken(), reclaimedAt)).isTrue();
    assertThat(find(task.getResourceId())).isEmpty();
  }

  private StorageCleanupTaskDAO create(String location, Date deletedAt) {
    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session ->
            repository.create(session, ResourceType.TABLE, UUID.randomUUID(), location, deletedAt),
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
