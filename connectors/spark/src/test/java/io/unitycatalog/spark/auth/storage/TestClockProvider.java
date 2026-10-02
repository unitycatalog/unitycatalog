package io.unitycatalog.spark.auth.storage;

import io.unitycatalog.client.internal.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.function.Supplier;

/**
 * Test-only bridge that lets the server-side storage credential cache run on the same manual
 * timeline as the connector and the vend generator. Loaded reflectively by the server (see {@code
 * ServerProperties#getStorageCredentialCacheTestClockProvider}), so it lives in test scope and may
 * reference the client's manual clock; the server main module only sees {@code Supplier<Clock>}.
 *
 * <p>The returned {@link java.time.Clock} delegates {@code instant()} to the live manual clock, so
 * advancing the manual clock in the test immediately moves the cache's notion of "now".
 */
public final class TestClockProvider implements Supplier<java.time.Clock> {

  @Override
  public java.time.Clock get() {
    Clock manualClock = BaseCredRenewITTest.testClock();
    return new java.time.Clock() {
      @Override
      public ZoneId getZone() {
        return ZoneOffset.UTC;
      }

      @Override
      public java.time.Clock withZone(ZoneId zone) {
        return this; // fixed-UTC test clock; zone is irrelevant to the cache's millisecond reads
      }

      @Override
      public Instant instant() {
        return manualClock.now();
      }
    };
  }
}
