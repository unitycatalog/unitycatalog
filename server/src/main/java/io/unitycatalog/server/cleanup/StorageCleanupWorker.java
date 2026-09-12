package io.unitycatalog.server.cleanup;

import io.unitycatalog.server.persist.StorageCleanupTaskRepository;
import io.unitycatalog.server.persist.dao.StorageCleanupTaskDAO;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/** Claims and processes at most one ready storage cleanup task. */
public final class StorageCleanupWorker {
  private static final int MAX_ERROR_LENGTH = 2048;

  private final StorageCleanupTaskRepository taskRepository;
  private final StorageCleanupAdapterFactory adapterFactory;
  private final StorageCleanupAttempt cleanupAttempt;
  private final Clock clock;
  private final Duration leaseDuration;
  private final Duration requestTimeout;
  private final Duration retryBackoff;

  public StorageCleanupWorker(
      StorageCleanupTaskRepository taskRepository,
      StorageCleanupAdapterFactory adapterFactory,
      StorageCleanupAttempt cleanupAttempt,
      Clock clock,
      Duration leaseDuration,
      Duration requestTimeout,
      Duration retryBackoff) {
    this.taskRepository = Objects.requireNonNull(taskRepository, "taskRepository");
    this.adapterFactory = Objects.requireNonNull(adapterFactory, "adapterFactory");
    this.cleanupAttempt = Objects.requireNonNull(cleanupAttempt, "cleanupAttempt");
    this.clock = Objects.requireNonNull(clock, "clock");
    this.leaseDuration = Objects.requireNonNull(leaseDuration, "leaseDuration");
    this.requestTimeout = Objects.requireNonNull(requestTimeout, "requestTimeout");
    this.retryBackoff = Objects.requireNonNull(retryBackoff, "retryBackoff");
  }

  /** Returns whether this worker claimed a task. */
  public boolean runOnce() {
    Instant claimedAt = clock.instant();
    Optional<StorageCleanupTaskDAO> readyTask = taskRepository.findReadyTask(claimedAt);
    if (readyTask.isEmpty()) {
      return false;
    }

    StorageCleanupTaskDAO task = readyTask.get();
    Optional<UUID> lease = taskRepository.claimTask(task.getResourceId(), claimedAt, leaseDuration);
    if (lease.isEmpty()) {
      return false;
    }

    StorageCleanupAttempt.Result result;
    try {
      result = cleanupAttempt.run(adapterFactory.create(task, requestTimeout));
    } catch (RuntimeException exception) {
      taskRepository.recordFailure(
          task.getResourceId(), lease.get(), clock.instant(), retryBackoff, safeError(exception));
      return true;
    }

    Instant finishedAt = clock.instant();
    if (result == StorageCleanupAttempt.Result.COMPLETE) {
      taskRepository.completeTask(task.getResourceId(), lease.get(), finishedAt);
    } else {
      taskRepository.releaseIncomplete(task.getResourceId(), lease.get(), finishedAt);
    }
    return true;
  }

  private static String safeError(RuntimeException exception) {
    String type = exception.getClass().getSimpleName();
    String error = "Storage cleanup failed" + (type.isEmpty() ? "" : ": " + type);
    return error.substring(0, Math.min(error.length(), MAX_ERROR_LENGTH));
  }
}
