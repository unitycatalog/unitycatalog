package io.unitycatalog.server.utils;

import java.time.Clock;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.CancellationException;

/** Checks deadline expiry and thread interruption at cooperative operation boundaries. */
public final class CooperativeDeadline {
  /** Disables deadline expiry but still checks and clears thread interruption. */
  public static final CooperativeDeadline NO_DEADLINE =
      new CooperativeDeadline(Clock.systemUTC(), Instant.MAX);

  private final Clock clock;
  private final Instant deadline;

  /**
   * @param clock clock used to check the deadline
   * @param deadline time at which the operation should stop starting more work
   */
  public CooperativeDeadline(Clock clock, Instant deadline) {
    this.clock = Objects.requireNonNull(clock, "clock");
    this.deadline = Objects.requireNonNull(deadline, "deadline");
  }

  /**
   * Stops at an operation boundary on interruption or deadline expiry. Detecting an interrupt
   * clears its flag so the caller can handle cancellation and the thread can be reused. This check
   * does not abort an in-flight request.
   *
   * @throws CancellationException if interrupted or the deadline has been reached
   */
  public void checkCancelled() {
    if (Thread.interrupted()) {
      throw new CancellationException("Operation interrupted");
    }
    if (!clock.instant().isBefore(deadline)) {
      throw new CancellationException("Operation deadline reached");
    }
  }
}
