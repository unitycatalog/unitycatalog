package io.unitycatalog.server.persist;

import io.unitycatalog.server.persist.dao.StorageCleanupTaskDAO;
import io.unitycatalog.server.utils.ValidationUtils;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import org.hibernate.Session;

/** Stores storage cleanup tasks; {@link StorageCleanupTaskRepository} is the database one. */
public interface CleanupTaskRepository {
  record Claim(
      ManagedResourceType resourceType, UUID resourceId, String storageLocation, UUID leaseToken) {}

  record CleanupFailureReport(String sanitizedError, Duration retryBackoff) {
    public CleanupFailureReport {
      Objects.requireNonNull(sanitizedError, "sanitizedError");
      Objects.requireNonNull(retryBackoff, "retryBackoff");
      ValidationUtils.checkArgument(!retryBackoff.isNegative(), "Retry backoff cannot be negative");
    }
  }

  /**
   * Creates a cleanup task in the caller's current database transaction. If the resource deletion
   * or task creation fails, both changes are rolled back. The task keeps the deleted resource's ID
   * and name.
   */
  StorageCleanupTaskDAO create(
      Session session,
      ManagedResourceType resourceType,
      UUID resourceId,
      String resourceName,
      String storageLocation);

  /**
   * Claims one retention-eligible task with no live lease. An empty result means no task was ready
   * or another worker won the conditional claim.
   */
  Optional<Claim> claim(Duration leaseDuration, Duration initialDelay);

  /**
   * Releases a matching live lease and waits for its retry delay. Returns false when the worker no
   * longer owns a live lease.
   */
  boolean reportFailure(UUID resourceId, UUID leaseToken, CleanupFailureReport report);

  /**
   * Deletes a task only while the matching lease is live. Returns false when ownership expired or
   * changed.
   */
  boolean finish(UUID resourceId, UUID leaseToken);
}
