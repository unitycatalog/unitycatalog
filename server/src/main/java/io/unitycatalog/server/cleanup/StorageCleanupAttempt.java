package io.unitycatalog.server.cleanup;

import java.time.Duration;
import java.util.List;
import java.util.function.LongSupplier;

/** Runs bounded cleanup batches for one fixed storage-work time slice. */
public final class StorageCleanupAttempt {
  public enum Result {
    COMPLETE,
    PARTIAL
  }

  private final int batchSize;
  private final long timeSliceNanos;
  private final long requestTimeoutNanos;
  private final LongSupplier nanoTime;

  public StorageCleanupAttempt(int batchSize, Duration timeSlice, Duration requestTimeout) {
    this(batchSize, timeSlice, requestTimeout, System::nanoTime);
  }

  StorageCleanupAttempt(
      int batchSize, Duration timeSlice, Duration requestTimeout, LongSupplier nanoTime) {
    if (batchSize <= 0) {
      throw new IllegalArgumentException("Cleanup batch size must be positive");
    }
    this.timeSliceNanos = positiveNanos(timeSlice, "Cleanup time slice");
    this.requestTimeoutNanos = positiveNanos(requestTimeout, "Storage request timeout");
    if (requestTimeoutNanos > timeSliceNanos / 2) {
      throw new IllegalArgumentException(
          "Cleanup time slice must allow one list and delete request");
    }
    this.batchSize = batchSize;
    this.nanoTime = nanoTime;
  }

  /** Runs and closes one adapter. Adapter failures are propagated to the caller. */
  public Result run(StorageCleanupAdapter adapter) {
    long startedAt = nanoTime.getAsLong();
    try (adapter) {
      while (hasTime(startedAt, requestTimeoutNanos * 2)) {
        List<String> batch = adapter.listBatch(batchSize);
        if (batch.isEmpty()) {
          return Result.COMPLETE;
        }
        if (!hasTime(startedAt, requestTimeoutNanos)) {
          return Result.PARTIAL;
        }
        adapter.deleteBatch(batch);
      }
      return Result.PARTIAL;
    }
  }

  private boolean hasTime(long startedAt, long requiredNanos) {
    long elapsed = nanoTime.getAsLong() - startedAt;
    return elapsed >= 0 && elapsed <= timeSliceNanos - requiredNanos;
  }

  private static long positiveNanos(Duration duration, String name) {
    try {
      long nanos = duration.toNanos();
      if (nanos <= 0) {
        throw new IllegalArgumentException(name + " must be positive");
      }
      return nanos;
    } catch (ArithmeticException e) {
      throw new IllegalArgumentException(name + " is too large", e);
    }
  }
}
