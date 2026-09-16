package io.unitycatalog.server.cleanup;

import io.unitycatalog.server.persist.StorageCleanupTaskRepository;
import io.unitycatalog.server.persist.StorageCleanupTaskRepository.Claim;
import io.unitycatalog.server.persist.StorageCleanupTaskRepository.CleanupFailureReport;
import io.unitycatalog.server.persist.dao.StorageCleanupTaskDAO.ResourceType;
import io.unitycatalog.server.persist.utils.FileOperations;
import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.ValidationUtils;
import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.io.SupportsPrefixOperations;

/**
 * Claims and processes at most one ready storage cleanup task.
 *
 * <p>A non-queuing single-thread executor prevents a new attempt from starting while a cancelled
 * attempt is still stopping.
 */
public final class StorageCleanupWorker implements AutoCloseable {
  private final StorageCleanupTaskRepository taskRepository;
  private final FileOperations fileOperations;
  private final ExecutorService executor;
  private final Duration leaseDuration;
  private final Duration socketTimeout;
  private final Duration attemptTimeout;
  private final Duration initialDelay;
  private final Duration retryBackoff;

  /**
   * Creates a worker that interrupts a cleanup attempt after its timeout.
   *
   * @param taskRepository repository used to claim and update cleanup tasks
   * @param fileOperations factory for storage operations with fresh credentials
   * @param leaseDuration duration of the task's database lease
   * @param socketTimeout socket inactivity timeout applied to each cloud storage request
   * @param attemptTimeout maximum time to wait before interrupting an attempt
   * @param initialDelay minimum task age before its first claim
   * @param retryBackoff delay before retrying an unsuccessful attempt
   */
  public StorageCleanupWorker(
      StorageCleanupTaskRepository taskRepository,
      FileOperations fileOperations,
      Duration leaseDuration,
      Duration socketTimeout,
      Duration attemptTimeout,
      Duration initialDelay,
      Duration retryBackoff) {
    this.taskRepository = Objects.requireNonNull(taskRepository, "taskRepository");
    this.fileOperations = Objects.requireNonNull(fileOperations, "fileOperations");
    this.leaseDuration = Objects.requireNonNull(leaseDuration, "leaseDuration");
    this.socketTimeout = Objects.requireNonNull(socketTimeout, "socketTimeout");
    this.attemptTimeout = Objects.requireNonNull(attemptTimeout, "attemptTimeout");
    this.initialDelay = Objects.requireNonNull(initialDelay, "initialDelay");
    this.retryBackoff = Objects.requireNonNull(retryBackoff, "retryBackoff");
    ValidationUtils.checkArgument(
        attemptTimeout.toMillis() > 0,
        "Storage cleanup attempt timeout must be at least one millisecond");
    ValidationUtils.checkArgument(
        leaseDuration.compareTo(attemptTimeout.plus(socketTimeout)) > 0,
        "Storage cleanup lease duration must exceed the attempt timeout plus socket timeout");
    this.executor = newExecutor();
  }

  /**
   * Starts at most one cleanup attempt and waits up to the configured timeout.
   *
   * @return false if no task was ready or an earlier attempt is still stopping; otherwise true
   */
  public boolean runOnce() {
    Future<Boolean> attempt;
    try {
      attempt = executor.submit(this::processOne);
    } catch (RejectedExecutionException exception) {
      return false;
    }

    try {
      return attempt.get(attemptTimeout.toMillis(), TimeUnit.MILLISECONDS);
    } catch (InterruptedException exception) {
      // Forward the caller's shutdown signal to the cleanup thread. Future.get clears the caller's
      // interrupt flag, so restore it before returning.
      attempt.cancel(true);
      Thread.currentThread().interrupt();
      return true;
    } catch (TimeoutException exception) {
      // Interrupt the cleanup thread. Prefix operations consume and clear this signal so the
      // executor thread can be reused after the attempt stops.
      attempt.cancel(true);
      return true;
    } catch (ExecutionException exception) {
      // processOne reports expected task failures, so anything escaping it is unexpected.
      throw new RuntimeException("Storage cleanup attempt failed", exception.getCause());
    }
  }

  private boolean processOne() {
    Optional<Claim> maybeClaim = taskRepository.claim(leaseDuration, initialDelay);
    if (maybeClaim.isEmpty()) {
      return false;
    }

    Claim claim = maybeClaim.get();
    try {
      cleanup(claim);
      taskRepository.finish(claim.resourceId(), claim.leaseToken());
    } catch (Exception exception) {
      taskRepository.reportFailure(
          claim.resourceId(),
          claim.leaseToken(),
          new CleanupFailureReport(safeError(exception), retryBackoff));
    }
    return true;
  }

  /** Deletes the claimed location and verifies that no descendants remain. */
  private void cleanup(Claim claim) throws IOException {
    NormalizedURL location = validateTask(claim);
    String prefix = location + "/";
    try (SupportsPrefixOperations prefixOperations =
        fileOperations.getCleanupFileIO(location, socketTimeout)) {
      prefixOperations.deletePrefix(prefix);
      try (CloseableIterable<FileInfo> remaining =
          CloseableIterable.of(prefixOperations.listPrefix(prefix))) {
        // A file may appear after deletePrefix's final listing. Keep the task if a fresh listing
        // finds one so the next attempt can remove it.
        boolean filesRemain = remaining.iterator().hasNext();
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

  private static ExecutorService newExecutor() {
    return new ThreadPoolExecutor(
        1,
        1,
        0,
        TimeUnit.MILLISECONDS,
        new SynchronousQueue<>(),
        runnable -> {
          Thread thread = new Thread(runnable, "storage-cleanup-attempt");
          thread.setDaemon(true);
          return thread;
        });
  }

  @Override
  public void close() {
    executor.shutdownNow();
  }
}
