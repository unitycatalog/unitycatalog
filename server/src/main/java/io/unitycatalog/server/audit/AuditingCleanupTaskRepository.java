package io.unitycatalog.server.audit;

import io.unitycatalog.server.persist.CleanupTaskRepository;
import io.unitycatalog.server.persist.CleanupTaskRepository.Claim;
import io.unitycatalog.server.persist.CleanupTaskRepository.CleanupFailureReport;
import io.unitycatalog.server.persist.ManagedResourceType;
import io.unitycatalog.server.persist.dao.StorageCleanupTaskDAO;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import org.hibernate.Session;

/** Records each queued storage cleanup once its transaction commits, naming who dropped it. */
final class AuditingCleanupTaskRepository implements CleanupTaskRepository {
  private final CleanupTaskRepository delegate;
  private final Auditor auditor;

  AuditingCleanupTaskRepository(CleanupTaskRepository delegate, Auditor auditor) {
    this.delegate = delegate;
    this.auditor = auditor;
  }

  @Override
  public StorageCleanupTaskDAO create(
      Session session,
      ManagedResourceType resourceType,
      UUID resourceId,
      String resourceName,
      String storageLocation) {
    StorageCleanupTaskDAO task =
        delegate.create(session, resourceType, resourceId, resourceName, storageLocation);
    String location = task.getStorageLocation();
    auditor.afterCommit(session, () -> auditor.recordQueueCleanup(resourceId, location));
    return task;
  }

  @Override
  public Optional<Claim> claim(Duration leaseDuration, Duration initialDelay) {
    return delegate.claim(leaseDuration, initialDelay);
  }

  @Override
  public boolean reportFailure(UUID resourceId, UUID leaseToken, CleanupFailureReport report) {
    return delegate.reportFailure(resourceId, leaseToken, report);
  }

  @Override
  public boolean finish(UUID resourceId, UUID leaseToken) {
    return delegate.finish(resourceId, leaseToken);
  }
}
