package io.unitycatalog.spark.utils;

import java.time.Duration;
import java.time.Instant;

public interface Clock {
  /**
   * @return the current time of the clock.
   */
  Instant now();

  /**
   * Sleeps for the specified duration.
   * For {@link SystemClock}, this blocks the current thread via {@link Thread#sleep(long)}.
   * For {@link ManualClock}, this advances the clock's time without blocking.
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
    public synchronized void sleep(Duration duration) {
      now = now.plus(duration);
    }
  }
}
