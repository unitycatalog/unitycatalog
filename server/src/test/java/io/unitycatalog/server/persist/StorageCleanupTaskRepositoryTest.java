package io.unitycatalog.server.persist;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.persist.dao.StorageCleanupTaskDAO;
import io.unitycatalog.server.persist.dao.StorageCleanupTaskDAO.ResourceType;
import io.unitycatalog.server.persist.utils.HibernateConfigurator;
import io.unitycatalog.server.persist.utils.TransactionManager;
import java.time.Duration;
import java.time.Instant;
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
  private static final Instant NOW = Instant.parse("2026-09-08T12:00:00Z");

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
  void createsNormalizedTaskAndFindsPathOverlaps() {
    StorageCleanupTaskDAO task = create("s3://bucket/a/unused/../b/c///", NOW);

    assertThat(task.getStorageLocation()).isEqualTo("s3://bucket/a/b/c");
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
                      repository.createTask(
                          session, ResourceType.TABLE, resourceId, "s3://bucket/path", NOW);
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
    StorageCleanupTaskDAO task = create(location, NOW);

    assertThat(task.getStorageLocation()).isEqualTo(location);
    assertThat(repository.hasPathOverlap(commonPrefix)).isTrue();
    assertThat(repository.hasPathOverlap(location)).isTrue();
    assertThat(repository.hasPathOverlap(location + "/child")).isTrue();
    assertThat(repository.hasPathOverlap(commonPrefix + "/sibling")).isFalse();
  }

  @Test
  void escapesLikeMetacharactersInPathChecks() {
    create("s3://bucket/literalX/child", NOW);
    create("s3://bucket/percentXYZ25/child", NOW);

    assertThat(repository.hasPathOverlap("s3://bucket/literal_")).isFalse();
    assertThat(repository.hasPathOverlap("s3://bucket/percent%25")).isFalse();
  }

  @Test
  void selectsEarliestReadyTaskAndRespectsLeases() {
    StorageCleanupTaskDAO earliest = create("s3://bucket/earliest", NOW.minusSeconds(120));
    StorageCleanupTaskDAO later = create("s3://bucket/later", NOW.minusSeconds(60));
    create("s3://bucket/future", NOW.plusSeconds(1));

    assertThat(repository.findReadyTask(NOW))
        .get()
        .extracting(StorageCleanupTaskDAO::getResourceId)
        .isEqualTo(earliest.getResourceId());
    UUID firstClaim =
        repository.claimTask(earliest.getResourceId(), NOW, Duration.ofMinutes(10)).orElseThrow();
    assertThat(repository.claimTask(earliest.getResourceId(), NOW, Duration.ofMinutes(10)))
        .isEmpty();
    assertThat(repository.findReadyTask(NOW))
        .get()
        .extracting(StorageCleanupTaskDAO::getResourceId)
        .isEqualTo(later.getResourceId());

    UUID secondClaim =
        repository
            .claimTask(
                earliest.getResourceId(), NOW.plus(Duration.ofMinutes(10)), Duration.ofMinutes(10))
            .orElseThrow();
    assertThat(secondClaim).isNotEqualTo(firstClaim);
  }

  @Test
  void validatesMinimumLeaseDuration() {
    StorageCleanupTaskDAO task = create("s3://bucket/duration", NOW);

    for (Duration duration :
        java.util.List.of(Duration.ZERO, Duration.ofNanos(-1), Duration.ofNanos(1))) {
      assertThatThrownBy(() -> repository.claimTask(task.getResourceId(), NOW, duration))
          .isInstanceOfSatisfying(
              BaseException.class,
              exception -> {
                assertThat(exception.getErrorCode()).isEqualTo(ErrorCode.INVALID_ARGUMENT);
                assertThat(exception).hasMessage("Lease duration must be at least one millisecond");
              });
    }
    assertThat(repository.claimTask(task.getResourceId(), NOW, Duration.ofMillis(1))).isPresent();
  }

  @Test
  void onlyOneCompetingClaimSucceeds() throws Exception {
    StorageCleanupTaskDAO task = create("s3://bucket/competing", NOW);
    CountDownLatch ready = new CountDownLatch(2);
    CountDownLatch start = new CountDownLatch(1);
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      java.util.concurrent.Callable<Optional<UUID>> claim =
          () -> {
            ready.countDown();
            start.await();
            return repository.claimTask(task.getResourceId(), NOW, Duration.ofMinutes(10));
          };
      Future<Optional<UUID>> first = executor.submit(claim);
      Future<Optional<UUID>> second = executor.submit(claim);
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
  void expiredLeaseCannotUpdateOrCompleteTask() {
    StorageCleanupTaskDAO task = create("s3://bucket/expired", NOW);
    UUID claim =
        repository.claimTask(task.getResourceId(), NOW, Duration.ofMinutes(10)).orElseThrow();
    Instant expiredAt = NOW.plus(Duration.ofMinutes(10));

    assertThat(repository.releaseIncomplete(task.getResourceId(), claim, expiredAt)).isFalse();
    assertThat(
            repository.recordFailure(
                task.getResourceId(), claim, expiredAt, Duration.ofMinutes(1), "error"))
        .isFalse();
    assertThat(repository.completeTask(task.getResourceId(), claim, expiredAt)).isFalse();
    assertThat(repository.claimTask(task.getResourceId(), expiredAt, Duration.ofMinutes(10)))
        .isPresent();
  }

  @Test
  void staleWorkerCannotUpdateOrCompleteReclaimedTask() {
    StorageCleanupTaskDAO task = create("s3://bucket/stale", NOW);
    UUID stale =
        repository.claimTask(task.getResourceId(), NOW, Duration.ofMinutes(10)).orElseThrow();
    Instant reclaimedAt = NOW.plus(Duration.ofMinutes(10));
    UUID current =
        repository
            .claimTask(task.getResourceId(), reclaimedAt, Duration.ofMinutes(10))
            .orElseThrow();

    assertThat(repository.releaseIncomplete(task.getResourceId(), stale, reclaimedAt)).isFalse();
    assertThat(
            repository.recordFailure(
                task.getResourceId(), stale, reclaimedAt, Duration.ofMinutes(30), "stale"))
        .isFalse();
    assertThat(repository.completeTask(task.getResourceId(), stale, reclaimedAt)).isFalse();

    Instant failedAt = reclaimedAt.plusSeconds(1);
    assertThat(
            repository.recordFailure(
                task.getResourceId(), current, failedAt, Duration.ofMinutes(30), "x".repeat(2100)))
        .isTrue();
    StorageCleanupTaskDAO failed = get(task.getResourceId());
    assertThat(failed.getFailureCount()).isOne();
    assertThat(failed.getLastError()).hasSize(2048);
    assertThat(failed.getLeaseToken()).isNull();
    assertThat(repository.findReadyTask(failedAt.plus(Duration.ofMinutes(30)).minusMillis(1)))
        .isEmpty();

    Instant retryAt = failedAt.plus(Duration.ofMinutes(30));
    UUID retry =
        repository.claimTask(task.getResourceId(), retryAt, Duration.ofMinutes(10)).orElseThrow();
    assertThat(repository.releaseIncomplete(task.getResourceId(), retry, retryAt)).isTrue();
    UUID finalClaim =
        repository.claimTask(task.getResourceId(), retryAt, Duration.ofMinutes(10)).orElseThrow();
    assertThat(repository.completeTask(task.getResourceId(), UUID.randomUUID(), retryAt)).isFalse();
    assertThat(repository.completeTask(task.getResourceId(), finalClaim, retryAt)).isTrue();
    assertThat(find(task.getResourceId())).isEmpty();
  }

  private StorageCleanupTaskDAO create(String location, Instant cleanableAt) {
    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session ->
            repository.createTask(
                session, ResourceType.TABLE, UUID.randomUUID(), location, cleanableAt),
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
}
