package io.unitycatalog.spark.utils;

import java.time.Duration;
import java.time.Instant;

public interface Clock {
  /**
   * @return the current time of the clock.
   */
  Instant now();

  /**
   * Advances the current time of this clock by the specified duration. After this call,
   * {@link #now()} should return a time equal to the previous time plus the given {@code duration}.
   */
  void advance(Duration duration);

  /**
   * Sleeps for the specified duration.
   * In {@link SystemClock}, blocks via {@link Thread#sleep(long)}.
   * In {@link ManualClock}, no-op—use {@link #advance(Duration)} to simulate time progression.
   */
  void sleep(Duration duration) throws InterruptedException;

  static Clock systemClock() {
    return SystemClock.SINGLETON;
  }

  static Clock manualClock(Instant now) {
    return new ManualClock(now);
  }

  class SystemClock implements Clock {
    private static final SystemClock SINGLETON = new SystemClock();

    @Override
    public Instant now() {
      return Instant.now();
    }

    @Override
    public void advance(Duration duration) {
      throw new UnsupportedOperationException("Cannot advance system clock.");
    }

    @Override
    public void sleep(Duration duration) throws InterruptedException {
      Thread.sleep(duration.toMillis());
    }
  }

  class ManualClock implements Clock {
    private volatile Instant now;

    ManualClock(Instant now) {
      this.now = now;
    }

    @Override
    public synchronized Instant now() {
      return now;
    }

    @Override
    public synchronized void advance(Duration duration) {
      now = now.plus(duration);
    }

    @Override
    public void sleep(Duration duration) {
      // No-op for manual clock - time is advanced explicitly via advance()
    }
  }
}
