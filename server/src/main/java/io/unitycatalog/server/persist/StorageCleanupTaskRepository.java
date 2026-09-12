package io.unitycatalog.server.persist;

import io.unitycatalog.server.persist.dao.StorageCleanupTaskDAO;
import io.unitycatalog.server.persist.dao.StorageCleanupTaskDAO.ResourceType;
import io.unitycatalog.server.persist.utils.TransactionManager;
import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.ValidationUtils;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.hibernate.Session;
import org.hibernate.SessionFactory;

public class StorageCleanupTaskRepository {
  private static final String LEASE_ERROR = "Lease duration must be at least one millisecond";
  private final SessionFactory sessionFactory;

  public StorageCleanupTaskRepository(SessionFactory sessionFactory) {
    this.sessionFactory = sessionFactory;
  }

  public StorageCleanupTaskDAO createTask(
      Session session,
      ResourceType resourceType,
      UUID resourceId,
      String storageLocation,
      Instant cleanableAt) {
    String normalized = NormalizedURL.normalize(storageLocation);
    StorageCleanupTaskDAO task =
        StorageCleanupTaskDAO.builder()
            .resourceType(resourceType)
            .resourceId(resourceId)
            .storageLocation(normalized)
            .cleanableAt(cleanableAt)
            .build();
    session.persist(task);
    return task;
  }

  public boolean hasPathOverlap(String storageLocation) {
    String normalized = NormalizedURL.normalize(storageLocation);
    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session ->
            session
                .createQuery(
                    "FROM StorageCleanupTaskDAO WHERE storageLocation IN :ancestors "
                        + "OR storageLocation LIKE :descendants ESCAPE '\\'",
                    StorageCleanupTaskDAO.class)
                .setParameter("ancestors", ancestors(normalized))
                .setParameter(
                    "descendants", escapeLike(normalized) + (normalized.endsWith("/") ? "%" : "/%"))
                .getResultList()
                .stream()
                .anyMatch(task -> pathsOverlap(task.getStorageLocation(), normalized)),
        "Failed to check storage cleanup path",
        /* readOnly= */ true);
  }

  public Optional<StorageCleanupTaskDAO> findReadyTask(Instant now) {
    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session ->
            session
                .createQuery(
                    "FROM StorageCleanupTaskDAO WHERE cleanableAt <= :now "
                        + "AND (leaseToken IS NULL OR leaseExpiresAt <= :now) ORDER BY cleanableAt",
                    StorageCleanupTaskDAO.class)
                .setParameter("now", now)
                .setMaxResults(1)
                .uniqueResultOptional(),
        "Failed to find ready storage cleanup task",
        /* readOnly= */ true);
  }

  public Optional<UUID> claimTask(UUID resourceId, Instant now, Duration leaseDuration) {
    ValidationUtils.checkArgument(leaseDuration.compareTo(Duration.ofMillis(1)) >= 0, LEASE_ERROR);
    UUID leaseToken = UUID.randomUUID();
    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          int updated =
              session
                  .createMutationQuery(
                      "UPDATE StorageCleanupTaskDAO SET leaseToken = :token, "
                          + "leaseExpiresAt = :expires WHERE resourceId = :id "
                          + "AND cleanableAt <= :now "
                          + "AND (leaseToken IS NULL OR leaseExpiresAt <= :now)")
                  .setParameter("token", leaseToken)
                  .setParameter("expires", now.plus(leaseDuration))
                  .setParameter("id", resourceId)
                  .setParameter("now", now)
                  .executeUpdate();
          return updated == 1 ? Optional.of(leaseToken) : Optional.empty();
        },
        "Failed to claim storage cleanup task",
        /* readOnly= */ false);
  }

  public boolean releaseIncomplete(UUID resourceId, UUID leaseToken, Instant now) {
    return updateClaimedTask(
        "SET leaseToken = NULL, leaseExpiresAt = NULL, cleanableAt = :cleanableAt",
        resourceId,
        leaseToken,
        now,
        now,
        null);
  }

  public boolean recordFailure(
      UUID resourceId, UUID leaseToken, Instant now, Duration backoff, String sanitizedError) {
    return updateClaimedTask(
        "SET leaseToken = NULL, leaseExpiresAt = NULL, failureCount = failureCount + 1, "
            + "lastError = :error, cleanableAt = :cleanableAt",
        resourceId,
        leaseToken,
        now,
        now.plus(backoff),
        sanitizedError == null
            ? null
            : sanitizedError.substring(0, Math.min(sanitizedError.length(), 2048)));
  }

  private boolean updateClaimedTask(
      String update, UUID resourceId, UUID leaseToken, Instant now, Instant nextRun, String error) {
    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          var query =
              session.createMutationQuery(
                  "UPDATE StorageCleanupTaskDAO "
                      + update
                      + " WHERE resourceId = :id AND leaseToken = :token "
                      + "AND leaseExpiresAt > :now");
          query.setParameter("id", resourceId);
          query.setParameter("token", leaseToken);
          query.setParameter("now", now);
          query.setParameter("cleanableAt", nextRun);
          if (update.contains(":error")) query.setParameter("error", error);
          return query.executeUpdate() == 1;
        },
        "Failed to update storage cleanup task",
        /* readOnly= */ false);
  }

  public boolean completeTask(UUID resourceId, UUID leaseToken, Instant now) {
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

  private static List<String> ancestors(String location) {
    List<String> result = new ArrayList<>();
    result.add(location);
    int schemeEnd = location.indexOf("://") + 3;
    int pathStart = location.indexOf('/', schemeEnd);
    if (pathStart >= 0) {
      String root = location.substring(0, pathStart + (pathStart == schemeEnd ? 1 : 0));
      result.add(root);
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

  private static boolean pathsOverlap(String first, String second) {
    String firstPrefix = first + (first.endsWith("/") ? "" : "/");
    String secondPrefix = second + (second.endsWith("/") ? "" : "/");
    return first.equals(second) || first.startsWith(secondPrefix) || second.startsWith(firstPrefix);
  }
}
