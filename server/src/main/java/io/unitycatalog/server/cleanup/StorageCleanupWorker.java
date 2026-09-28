package io.unitycatalog.server.cleanup;

import io.unitycatalog.server.persist.StorageCleanupTaskRepository;
import io.unitycatalog.server.persist.StorageCleanupTaskRepository.Claim;
import io.unitycatalog.server.persist.StorageCleanupTaskRepository.CleanupFailureReport;
import io.unitycatalog.server.persist.dao.StorageCleanupTaskDAO.ResourceType;
import io.unitycatalog.server.persist.utils.FileOperations;
import io.unitycatalog.server.utils.CooperativeDeadline;
import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.ServerProperties;
import io.unitycatalog.server.utils.ValidationUtils;
import java.io.IOException;
import java.net.URI;
import java.time.Clock;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.io.SupportsPrefixOperations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Polls and processes cleanup tasks on one scheduled thread. Storage operations share a cooperative
 * deadline that checks the attempt budget and shutdown interruption.
 */
public final class StorageCleanupWorker implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(StorageCleanupWorker.class);

  private final StorageCleanupTaskRepository taskRepository;
  private final FileOperations fileOperations;
  private final Clock clock;
  private final Duration pollInterval;
  private final Duration leaseDuration;
  private final Duration attemptTimeout;
  private final Duration initialDelay;
  private final Duration retryBackoff;
  private ScheduledExecutorService executor;

  /**
   * Creates a worker with a cooperative time budget for each cleanup attempt.
   *
   * @param taskRepository repository used to claim and update cleanup tasks
   * @param fileOperations factory for storage operations with fresh credentials
   * @param clock clock used to measure the cleanup attempt's budget
   * @param serverProperties cleanup timing settings, read when the worker is created
   */
  public StorageCleanupWorker(
      StorageCleanupTaskRepository taskRepository,
      FileOperations fileOperations,
      Clock clock,
      ServerProperties serverProperties) {
    this.taskRepository = Objects.requireNonNull(taskRepository, "taskRepository");
    this.fileOperations = Objects.requireNonNull(fileOperations, "fileOperations");
    this.clock = Objects.requireNonNull(clock, "clock");
    this.pollInterval = serverProperties.getStorageCleanupPollInterval();
    this.leaseDuration = serverProperties.getStorageCleanupLeaseDuration();
    this.attemptTimeout = serverProperties.getStorageCleanupAttemptTimeout();
    this.initialDelay = serverProperties.getStorageCleanupInitialDelay();
    this.retryBackoff = serverProperties.getStorageCleanupRetryBackoff();
    if (pollInterval.toMillis() <= 0) {
      throw new IllegalArgumentException("Cleanup poll interval must be at least one millisecond");
    }
    ValidationUtils.checkArgument(
        attemptTimeout.toMillis() > 0,
        "Storage cleanup attempt timeout must be at least one millisecond");
    ValidationUtils.checkArgument(
        leaseDuration.compareTo(attemptTimeout) > 0,
        "Storage cleanup lease duration must exceed the attempt timeout");
  }

  /**
   * Starts one daemon thread polling at the configured interval. Calling this while running or
   * still stopping has no effect.
   */
  public synchronized void start() {
    if (executor != null && !executor.isTerminated()) {
      return;
    }
    executor =
        Executors.newSingleThreadScheduledExecutor(
            runnable -> {
              Thread thread = new Thread(runnable, "storage-cleanup-worker");
              thread.setDaemon(true);
              return thread;
            });
    long millis = pollInterval.toMillis();
    executor.scheduleWithFixedDelay(this::pollQuietly, millis, millis, TimeUnit.MILLISECONDS);
  }

  private void pollQuietly() {
    try {
      runOnce();
    } catch (Throwable t) {
      LOGGER.warn("Storage cleanup poll failed; will retry on the next interval", t);
    }
  }

  /**
   * Stops polling and waits up to five seconds for an active attempt. A lingering daemon operation
   * does not block server shutdown; unfinished tasks remain available after their leases expire.
   * The worker may be started again once its previous executor has terminated.
   */
  public synchronized void stop() {
    if (executor == null || executor.isShutdown()) {
      return;
    }
    executor.shutdownNow();
    try {
      if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
        LOGGER.warn("Storage cleanup worker did not stop within 5s");
      }
    } catch (InterruptedException e) {
      // Preserve interruption of the caller waiting for shutdown.
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public void close() {
    stop();
  }

  /**
   * Claims and processes at most one task on the calling thread. Deadline checks apply to storage
   * work; database reporting and completion run after cleanup has returned. Do not call this
   * directly while background polling is running.
   *
   * @return false if no task was claimed; true if a task was claimed, even if it needs a retry
   */
  boolean runOnce() {
    Optional<Claim> maybeClaim = taskRepository.claim(leaseDuration, initialDelay);
    if (maybeClaim.isEmpty()) {
      return false;
    }

    Claim claim = maybeClaim.get();
    CooperativeDeadline deadline =
        new CooperativeDeadline(clock, clock.instant().plus(attemptTimeout));
    try {
      cleanup(claim, deadline);
      taskRepository.finish(claim.resourceId(), claim.leaseToken());
    } catch (Exception exception) {
      // Surface the failure to operators: the DB only keeps a sanitized class-name string, so
      // without this a stuck cleanup is invisible. Location/id are the resource's own managed
      // path, not secrets; the throwable gives the stack trace for debugging.
      LOGGER.warn(
          "Storage cleanup failed for {} {} at {}; will retry after backoff",
          claim.resourceType(),
          claim.resourceId(),
          claim.storageLocation(),
          exception);
      taskRepository.reportFailure(
          claim.resourceId(),
          claim.leaseToken(),
          new CleanupFailureReport(safeError(exception), retryBackoff));
    }
    return true;
  }

  /** Deletes the claimed location and verifies that no descendants remain. */
  private void cleanup(Claim claim, CooperativeDeadline deadline) throws IOException {
    NormalizedURL location = validateTask(claim);
    String prefix = location + "/";
    deadline.checkCancelled();
    try (SupportsPrefixOperations prefixOperations =
        fileOperations.getCleanupFileIO(location, deadline)) {
      deadline.checkCancelled();
      prefixOperations.deletePrefix(prefix);
      deadline.checkCancelled();
      try (CloseableIterable<FileInfo> remaining =
          CloseableIterable.of(prefixOperations.listPrefix(prefix))) {
        // A file may appear after deletePrefix's final listing. Keep the task if a fresh listing
        // finds one so the next attempt can remove it.
        boolean filesRemain = remaining.iterator().hasNext();
        deadline.checkCancelled();
        if (filesRemain) {
          throw new IllegalStateException("Storage cleanup did not empty the task prefix");
        }
      }
    }
  }

  /** Validates the stored resource and path before storage credentials are requested. */
  private static NormalizedURL validateTask(Claim claim) {
    ResourceType resourceType = Objects.requireNonNull(claim.resourceType(), "resourceType");
    String segment =
        switch (resourceType) {
          case TABLE, STAGING_TABLE -> "tables";
          case VOLUME -> "volumes";
          case REGISTERED_MODEL -> "models";
          case MODEL_VERSION -> "versions";
        };
    NormalizedURL location = NormalizedURL.from(claim.storageLocation());
    URI uri = location.toUri();
    if (claim.resourceId() == null
        || uri.getRawQuery() != null
        || uri.getRawFragment() != null
        || uri.getRawPath() == null
        || !uri.getRawPath().endsWith("/" + segment + "/" + claim.resourceId())) {
      throw new IllegalArgumentException(
          "Cleanup task location does not match its " + resourceType + " resource id");
    }
    return location;
  }

  private static String safeError(Exception exception) {
    return "Storage cleanup failed: " + exception.getClass().getSimpleName();
  }
}
