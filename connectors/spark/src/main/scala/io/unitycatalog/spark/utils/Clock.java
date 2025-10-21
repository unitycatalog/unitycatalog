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
   * Advances the current time of this clock by the specified duration. After this call,
   * {@link #now()} should return a time equal to the previous time plus the given {@code duration}.
   */
  void advance(Duration duration);

  static Clock systemClock() {
    return SystemClock.SINGLETON;
  }

  static Clock manualClock(Instant now) {
    return new ManualClock(now);
  }

  Map<String, ManualClock> globalManualClock = new ConcurrentHashMap<>();

  static Clock getManualClock(String name) {
    return globalManualClock.compute(name, (clockName, clock) ->
        clock == null ? new ManualClock(Instant.now()) : clock
    );
  }

  static void removeManualClock(String name) {
    globalManualClock.remove(name);
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
  }

  class ManualClock implements Clock {
    private static final ManualClock SINGLETON = new ManualClock(Instant.now());
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
  }
}
