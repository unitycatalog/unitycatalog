package io.unitycatalog.server.cleanup;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Runs one storage cleanup worker on a fixed-delay background schedule. */
public final class StorageCleanupPoller implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(StorageCleanupPoller.class);

  private final StorageCleanupWorker worker;
  private ScheduledExecutorService executor;

  public StorageCleanupPoller(StorageCleanupWorker worker) {
    this.worker = Objects.requireNonNull(worker, "worker");
  }

  /** Starts one daemon polling thread. Calling this while running has no effect. */
  public synchronized void start(Duration interval) {
    long millis = interval.toMillis();
    if (millis <= 0) {
      throw new IllegalArgumentException("Cleanup poll interval must be at least one millisecond");
    }
    if (executor != null) {
      return;
    }
    executor =
        Executors.newSingleThreadScheduledExecutor(
            runnable -> {
              Thread thread = new Thread(runnable, "storage-cleanup-worker");
              thread.setDaemon(true);
              return thread;
            });
    executor.scheduleWithFixedDelay(this::pollQuietly, millis, millis, TimeUnit.MILLISECONDS);
  }

  private void pollQuietly() {
    try {
      worker.runOnce();
    } catch (Throwable t) {
      LOGGER.warn("Storage cleanup poll failed; will retry on the next interval", t);
    }
  }

  /** Stops polling and waits for an active poll to finish. The poller may be started again. */
  public synchronized void stop() {
    if (executor == null) {
      return;
    }
    executor.shutdownNow();
    boolean interrupted = false;
    while (!executor.isTerminated()) {
      try {
        executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
      } catch (InterruptedException e) {
        interrupted = true;
      }
    }
    executor = null;
    if (interrupted) {
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public void close() {
    try {
      stop();
    } finally {
      worker.close();
    }
  }

  synchronized boolean isRunning() {
    return executor != null && !executor.isShutdown();
  }
}
