package io.unitycatalog.server.persist;

import io.unitycatalog.server.persist.dao.StorageCleanupTaskDAO;
import io.unitycatalog.server.persist.dao.StorageCleanupTaskDAO.ResourceType;
import io.unitycatalog.server.persist.utils.TransactionManager;
import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.ValidationUtils;
import java.time.Duration;
import java.util.Date;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import org.hibernate.Session;
import org.hibernate.SessionFactory;

public class StorageCleanupTaskRepository {
  private static final String LEASE_ERROR = "Lease duration must be at least one millisecond";
  private final SessionFactory sessionFactory;

  public record Claim(StorageCleanupTaskDAO task, UUID leaseToken) {}

  public sealed interface CleanupReport permits CleanupReport.Partial, CleanupReport.Failure {
    record Partial() implements CleanupReport {}

    record Failure(String sanitizedError, Duration retryBackoff) implements CleanupReport {
      public Failure {
        Objects.requireNonNull(sanitizedError, "sanitizedError");
        Objects.requireNonNull(retryBackoff, "retryBackoff");
      }
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
      Session session,
      ResourceType resourceType,
      UUID resourceId,
      String storageLocation,
      Date deletedAt) {
    StorageCleanupTaskDAO task =
        StorageCleanupTaskDAO.builder()
            .resourceType(resourceType)
            .resourceId(resourceId)
            .storageLocation(NormalizedURL.normalize(storageLocation))
            .deletedAt(deletedAt)
            .build();
    session.persist(task);
    return task;
  }

  /**
   * Claims one retention-eligible task with no live lease. An empty result means no task was ready
   * or another worker won the conditional claim.
   */
  public Optional<Claim> claim(Date now, Duration leaseDuration, Duration initialDelay) {
    ValidationUtils.checkArgument(leaseDuration.compareTo(Duration.ofMillis(1)) >= 0, LEASE_ERROR);
    Date cutoff = Date.from(now.toInstant().minus(initialDelay));
    UUID leaseToken = UUID.randomUUID();
    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          Optional<StorageCleanupTaskDAO> readyTask =
              session
                  .createQuery(
                      "FROM StorageCleanupTaskDAO WHERE deletedAt <= :cutoff "
                          + "AND (leaseExpiresAt IS NULL OR leaseExpiresAt < :now) "
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
          int updated =
              session
                  .createMutationQuery(
                      "UPDATE StorageCleanupTaskDAO SET leaseToken = :token, "
                          + "leaseExpiresAt = :expires WHERE resourceId = :id "
                          + "AND deletedAt <= :cutoff "
                          + "AND (leaseExpiresAt IS NULL OR leaseExpiresAt < :now)")
                  .setParameter("token", leaseToken)
                  .setParameter("expires", Date.from(now.toInstant().plus(leaseDuration)))
                  .setParameter("id", task.getResourceId())
                  .setParameter("cutoff", cutoff)
                  .setParameter("now", now)
                  .executeUpdate();
          return updated == 1 ? Optional.of(new Claim(task, leaseToken)) : Optional.empty();
        },
        "Failed to claim storage cleanup task",
        /* readOnly= */ false);
  }

  /**
   * Releases a matching live lease. Partial work is immediately eligible; failure waits for its
   * retry delay. Returns false when the worker no longer owns a live lease.
   */
  public boolean report(UUID resourceId, UUID leaseToken, Date now, CleanupReport report) {
    Objects.requireNonNull(report, "report");
    String update;
    Date nextAttempt;
    String error;
    if (report instanceof CleanupReport.Failure failure) {
      update =
          "SET leaseToken = NULL, leaseExpiresAt = :nextAttempt, "
              + "failureCount = failureCount + 1, lastError = :error";
      nextAttempt = Date.from(now.toInstant().plus(failure.retryBackoff()));
      error =
          failure.sanitizedError().substring(0, Math.min(failure.sanitizedError().length(), 2048));
    } else {
      update =
          "SET leaseToken = NULL, leaseExpiresAt = :nextAttempt, "
              + "failureCount = 0, lastError = NULL";
      nextAttempt = now;
      error = null;
    }
    String statement =
        "UPDATE StorageCleanupTaskDAO "
            + update
            + " WHERE resourceId = :id AND leaseToken = :token AND leaseExpiresAt > :now";
    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          var query =
              session
                  .createMutationQuery(statement)
                  .setParameter("nextAttempt", nextAttempt)
                  .setParameter("id", resourceId)
                  .setParameter("token", leaseToken)
                  .setParameter("now", now);
          if (error != null) {
            query.setParameter("error", error);
          }
          return query.executeUpdate() == 1;
        },
        "Failed to update storage cleanup task",
        /* readOnly= */ false);
  }

  /**
   * Deletes a task only while the matching lease is live. Returns false when ownership expired or
   * changed.
   */
  public boolean finish(UUID resourceId, UUID leaseToken, Date now) {
    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session ->
            session
                    .createMutationQuery(
                        "DELETE FROM StorageCleanupTaskDAO WHERE resourceId = :id "
                            + "AND leaseToken = :token AND leaseExpiresAt > :now")
                    .setParameter("id", resourceId)
                    .setParameter("token", leaseToken)
                    .setParameter("now", now)
                    .executeUpdate()
                == 1,
        "Failed to complete storage cleanup task",
        /* readOnly= */ false);
  }
}
