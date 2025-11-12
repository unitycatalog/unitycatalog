package io.unitycatalog.spark.utils;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public interface Clock {
  /**
   * @return the current time of the clock.
   */
  Instant now();

  /**
   * Sleeps for the given duration.
   * <p>
   * For system clock, this performs an actual sleep by calling {@link Thread#sleep}.
   * For manual clock, this advances the clock time by the given duration without sleeping.
   *
   * @param duration the duration to sleep
   * @throws InterruptedException if the sleep is interrupted (only for system clock)
   */
  void sleep(Duration duration) throws InterruptedException;

  static Clock systemClock() {
    return SystemClock.SINGLETON;
  }

  static Clock manualClock(Instant now) {
    return new ManualClock(now);
  }


  static Clock getManualClock(String name) {
    return ManualClock.globalManualClock.compute(name, (clockName, clock) ->
        clock == null ? manualClock(Instant.now()) : clock
    );
  }

  static void removeManualClock(String name) {
    ManualClock.globalManualClock.remove(name);
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
    private static final Map<String, Clock> globalManualClock = new ConcurrentHashMap<>();

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
      // For manual clock, just advance the time without actual sleeping
      now = now.plus(duration);
    }
  }
}
