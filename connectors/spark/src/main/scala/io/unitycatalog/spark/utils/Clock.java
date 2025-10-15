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

  class SystemClock implements Clock {
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
    private final Instant now;

    ManualClock(Instant now) {
      this.now = now;
    }

    @Override
    public Instant now() {
      return now;
    }

    @Override
    public void advance(Duration duration) {
      now.plus(duration);
    }
  }
}
