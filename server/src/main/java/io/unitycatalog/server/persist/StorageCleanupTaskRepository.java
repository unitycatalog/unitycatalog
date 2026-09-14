package io.unitycatalog.server.persist;

import io.unitycatalog.server.persist.dao.StorageCleanupTaskDAO;
import io.unitycatalog.server.persist.dao.StorageCleanupTaskDAO.ResourceType;
import io.unitycatalog.server.persist.utils.TransactionManager;
import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.ValidationUtils;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import org.hibernate.Session;
import org.hibernate.SessionFactory;

/** Persists cleanup tasks using the database clock for all task timestamps. */
public class StorageCleanupTaskRepository {
  private final SessionFactory sessionFactory;

  public record Claim(
      ResourceType resourceType, UUID resourceId, String storageLocation, UUID leaseToken) {}

  public record CleanupFailureReport(String sanitizedError, Duration retryBackoff) {
    public CleanupFailureReport {
      Objects.requireNonNull(sanitizedError, "sanitizedError");
      Objects.requireNonNull(retryBackoff, "retryBackoff");
      ValidationUtils.checkArgument(!retryBackoff.isNegative(), "Retry backoff cannot be negative");
    }
  }

  public StorageCleanupTaskRepository(SessionFactory sessionFactory) {
    this.sessionFactory = sessionFactory;
  }

  /**
   * Creates a cleanup task in the caller's current database transaction. If the resource deletion
   * or task creation fails, both changes are rolled back.
   */
  public StorageCleanupTaskDAO create(
      Session session, ResourceType resourceType, UUID resourceId, String storageLocation) {
    StorageCleanupTaskDAO task =
        StorageCleanupTaskDAO.builder()
            .resourceType(resourceType)
            .resourceId(resourceId)
            .storageLocation(NormalizedURL.normalize(storageLocation))
            .deletedAt(currentDatabaseTime(session))
            .build();
    session.persist(task);
    return task;
  }

  public boolean hasPathOverlap(String storageLocation) {
    String normalized = NormalizedURL.normalize(storageLocation);
    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session ->
            !session
                .createQuery(
                    "FROM StorageCleanupTaskDAO WHERE storageLocation IN :ancestors "
                        + "OR storageLocation LIKE :descendants ESCAPE '\\'",
                    StorageCleanupTaskDAO.class)
                .setParameter("ancestors", ancestors(normalized))
                .setParameter(
                    "descendants", escapeLike(normalized) + (normalized.endsWith("/") ? "%" : "/%"))
                .setMaxResults(1)
                .getResultList()
                .isEmpty(),
        "Failed to check storage cleanup path",
        /* readOnly= */ true);
  }

  /**
   * Claims one retention-eligible task with no live lease. An empty result means no task was ready
   * or another worker won the conditional claim.
   */
  public Optional<Claim> claim(Duration leaseDuration, Duration initialDelay) {
    ValidationUtils.checkArgument(
        leaseDuration.compareTo(Duration.ofMillis(1)) >= 0,
        "Lease duration must be at least one millisecond");
    ValidationUtils.checkArgument(
        !initialDelay.isNegative(), "Initial cleanup delay cannot be negative");
    UUID leaseToken = UUID.randomUUID();
    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          Date now = currentDatabaseTime(session);
          Date cutoff = Date.from(now.toInstant().minus(initialDelay));
          Optional<StorageCleanupTaskDAO> readyTask =
              session
                  .createQuery(
                      "FROM StorageCleanupTaskDAO WHERE deletedAt <= :cutoff "
                          + "AND (leaseExpiresAt IS NULL OR leaseExpiresAt <= :now) "
                          + "ORDER BY COALESCE(leaseExpiresAt, deletedAt)",
                      StorageCleanupTaskDAO.class)
                  .setParameter("cutoff", cutoff)
                  .setParameter("now", now)
                  .setMaxResults(1)
                  .uniqueResultOptional();
          if (readyTask.isEmpty()) {
            return Optional.empty();
          }
          StorageCleanupTaskDAO task = readyTask.get();
          Date expires = Date.from(now.toInstant().plus(leaseDuration));
          int updated =
              session
                  .createMutationQuery(
                      "UPDATE StorageCleanupTaskDAO SET leaseToken = :token, "
                          + "leaseExpiresAt = :expires WHERE resourceId = :id "
                          + "AND storageLocation = :location "
                          + "AND deletedAt <= :cutoff "
                          + "AND (leaseExpiresAt IS NULL OR leaseExpiresAt <= :now)")
                  .setParameter("token", leaseToken)
                  .setParameter("expires", expires)
                  .setParameter("id", task.getResourceId())
                  .setParameter("location", task.getStorageLocation())
                  .setParameter("cutoff", cutoff)
                  .setParameter("now", now)
                  .executeUpdate();
          return updated == 1
              ? Optional.of(
                  new Claim(
                      task.getResourceType(),
                      task.getResourceId(),
                      task.getStorageLocation(),
                      leaseToken))
              : Optional.empty();
        },
        "Failed to claim storage cleanup task",
        /* readOnly= */ false);
  }

  /**
   * Releases a matching live lease and waits for its retry delay. Returns false when the worker no
   * longer owns a live lease.
   */
  public boolean reportFailure(UUID resourceId, UUID leaseToken, CleanupFailureReport report) {
    Objects.requireNonNull(report, "report");
    String error =
        report.sanitizedError().substring(0, Math.min(report.sanitizedError().length(), 2048));
    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          Date now = currentDatabaseTime(session);
          Date nextAttempt = Date.from(now.toInstant().plus(report.retryBackoff()));
          var query =
              session
                  .createMutationQuery(
                      "UPDATE StorageCleanupTaskDAO SET leaseToken = NULL, "
                          + "leaseExpiresAt = :nextAttempt, failureCount = failureCount + 1, "
                          + "lastError = :error WHERE resourceId = :id AND leaseToken = :token "
                          + "AND leaseExpiresAt > :now")
                  .setParameter("nextAttempt", nextAttempt)
                  .setParameter("id", resourceId)
                  .setParameter("token", leaseToken)
                  .setParameter("now", now);
          query.setParameter("error", error);
          return query.executeUpdate() == 1;
        },
        "Failed to update storage cleanup task",
        /* readOnly= */ false);
  }

  /**
   * Deletes a task only while the matching lease is live. Returns false when ownership expired or
   * changed.
   */
  public boolean finish(UUID resourceId, UUID leaseToken) {
    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          Date now = currentDatabaseTime(session);
          return session
                  .createMutationQuery(
                      "DELETE FROM StorageCleanupTaskDAO WHERE resourceId = :id "
                          + "AND leaseToken = :token AND leaseExpiresAt > :now")
                  .setParameter("id", resourceId)
                  .setParameter("token", leaseToken)
                  .setParameter("now", now)
                  .executeUpdate()
              == 1;
        },
        "Failed to complete storage cleanup task",
        /* readOnly= */ false);
  }

  private static Date currentDatabaseTime(Session session) {
    return session.createQuery("SELECT CURRENT_TIMESTAMP", Date.class).getSingleResult();
  }

  private static List<String> ancestors(String location) {
    List<String> result = new ArrayList<>();
    result.add(location);
    int schemeEnd = location.indexOf("://") + 3;
    int pathStart = location.indexOf('/', schemeEnd);
    if (pathStart >= 0) {
      result.add(location.substring(0, pathStart + (pathStart == schemeEnd ? 1 : 0)));
      for (int slash = location.indexOf('/', pathStart + 1);
          slash >= 0;
          slash = location.indexOf('/', slash + 1)) {
        result.add(location.substring(0, slash));
      }
    }
    return result;
  }

  private static String escapeLike(String value) {
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_");
  }
}
